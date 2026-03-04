"""
Delayed gaps check: run a task after enter-gaps to monitor a destination column
and send mail for rows that were not modified (column still empty).
Uses APScheduler for the first version.
"""

import sys
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

_scheduler: Optional[BackgroundScheduler] = None


def _column_letter_to_index(letter: str) -> int:
    """Convert column letter to 0-based index (A=0, B=1, ..., Z=25, AA=26, ...)."""
    result = 0
    for char in letter.upper():
        result = result * 26 + (ord(char) - ord("A") + 1)
    return result - 1


def _get_sheet_name_from_id(drive_service, wb_id: str, sheet_id: int) -> str:
    """Resolve sheet name from spreadsheet id and sheet id."""
    spreadsheet = (
        drive_service.sheets_service.spreadsheets()
        .get(spreadsheetId=wb_id)
        .execute()
    )
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["sheetId"] == sheet_id:
            return sheet["properties"]["title"]
    raise ValueError(f"Sheet with ID {sheet_id} not found in spreadsheet {wb_id}")


def _read_sheet_range(
    drive_service,
    wb_id: str,
    sheet_id: int,
    range_notation: str,
) -> List[List[Any]]:
    """Read a range from a Google Sheet. range_notation is e.g. 'A2:Z500' (no sheet name)."""
    sheet_name = _get_sheet_name_from_id(drive_service, wb_id, sheet_id)
    full_range = f"'{sheet_name}'!{range_notation}"
    result = (
        drive_service.sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=wb_id, range=full_range)
        .execute()
    )
    return result.get("values", [])


def run_delayed_gaps_check(context: Dict[str, Any]) -> None:
    """
    Run the delayed task: read the gaps sheet, find rows matching caller_id and time_str,
    check destination column for empty cells, send mail per row or summary.
    """
    print(f"delayed_gaps_check: running", file=sys.stderr)
    config_path = context.get("config_path")
    if not config_path:
        print("delayed_gaps_check: config_path missing in context", file=sys.stderr)
        return

    from pathlib import Path
    from common_utils.config_manager import ConfigManager
    from .config import _get_default_config
    from .google_drive_utils import GDriveService
    from .mail_service import create_mail_service

    try:
        print(f"delayed_gaps_check: config_path: {config_path}", file=sys.stderr)
        config_manager = ConfigManager(config_path)
        config = _get_default_config(config_manager)

        print(f"loading finished", file=sys.stderr)
    except Exception as e:
        print(f"delayed_gaps_check: failed to load config: {e}", file=sys.stderr)
        return

    delayed_cfg = config.get_delayed_gaps_check_config()
    if not delayed_cfg.get("enabled", False):
        print("delayed_gaps_check: disabled in config, skipping", file=sys.stderr)
        return

    sheet_config = context.get("sheet_config")
    if not sheet_config:
        print("delayed_gaps_check: sheet_config missing in context", file=sys.stderr)
        return

    wb_id = sheet_config.get("wb_id")
    sheet_id = sheet_config.get("sheet_id")
    start_col = (sheet_config.get("start_column_gap_info") or "C").upper()
    dest_col_letter = (delayed_cfg.get("destination_column_letter") or "H").upper()
    mail_mode = delayed_cfg.get("mail_mode", "summary")
    mail_cfg = delayed_cfg.get("mail") or {}

    caller_id = context.get("caller_id", "")
    time_str = context.get("time_str", "")
    date_str = context.get("date_str", "")
    nick_name = context.get("nick_name", "")
    customers_file_name = context.get("customers_file_name", "")

    # Column indices (see GapSpreadsheetUpdater):
    # A      : gap value
    # start  : caller_display (nick_name or caller_id)
    # start+1: caller_id (text, with leading ')
    # start+2: date_str
    # start+3: time_str (text, with leading ')
    # start+4: customers_input_file_name
    start_idx = _column_letter_to_index(start_col)
    caller_id_col = start_idx + 1
    date_col = start_idx + 2
    time_col = start_idx + 3
    dest_col_idx = _column_letter_to_index(dest_col_letter)

    # Build range to read: from A through destination column, rows 2..500.
    # This guarantees we include gap value column (A), all gap-info columns, and the destination column.
    range_notation = f"A2:{dest_col_letter}500"

    try:
        service_config = config.get_service_config()
        drive_service = GDriveService(service_config)
    except Exception as e:
        print(f"delayed_gaps_check: failed to create drive service: {e}", file=sys.stderr)
        return

    try:
        rows = _read_sheet_range(drive_service, wb_id, sheet_id, range_notation)
    except Exception as e:
        print(f"delayed_gaps_check: failed to read sheet: {e}", file=sys.stderr)
        return

    # Normalize time for comparison (e.g. 09:05 vs 9:5)
    def norm(s: str) -> str:
        return (s or "").strip()

    def norm_time(t: str) -> str:
        s = norm(t)
        if not s:
            return s
        if s.startswith("'"):
            s = s[1:]
        return s

    def norm_caller(c: str) -> str:
        s = norm(c)
        # Caller ID was written as text with a leading quote, strip it if present
        if s.startswith("'"):
            s = s[1:]
        return s

    matching_rows: List[Dict[str, Any]] = []
    caller_norm = norm_caller(caller_id) if caller_id else ""
    date_norm = norm(date_str) if date_str else ""
    time_norm = norm_time(time_str) if time_str else ""

    # First, find rows that belong to this enter-gaps call by matching the
    # stamped metadata triplet (caller_id, date_str, time_str).
    # Then, within those rows, check the destination column for emptiness.
    for row_index, row in enumerate(rows):
        if not row:
            continue
        row_caller = norm_caller(row[caller_id_col]) if len(row) > caller_id_col else ""
        row_date = norm(row[date_col]) if len(row) > date_col else ""
        row_time = norm_time(row[time_col]) if len(row) > time_col else ""

        # Require exact match on all stamped components to identify relevant rows
        if not (caller_norm and date_norm and time_norm):
            # If any part of the stamp is missing, skip matching to avoid false positives
            continue

        if row_caller == caller_norm and row_date == date_norm and row_time == time_norm:
            dest_val = row[dest_col_idx] if len(row) > dest_col_idx else ""
            is_empty = not norm(dest_val)
            gap_val = row[0] if row else ""
            matching_rows.append({
                "row_index": row_index + 2,
                "gap_value": gap_val,
                "destination_empty": is_empty,
                "row": row,
            })

    empty_rows = [r for r in matching_rows if r["destination_empty"]]
    if not empty_rows:
        print("delayed_gaps_check: no empty destination rows, nothing to mail", file=sys.stderr)
        return

    smtp_config = config.get_smtp_config()

    print(f"SMTP config: {smtp_config}", file=sys.stderr)
    mail_service = create_mail_service(
        "delayed_gaps_check",
        mail_cfg,
        service_config={"smtp_config": smtp_config},
    )
    if not mail_service:
        print("delayed_gaps_check: no mail service (check mail config)", file=sys.stderr)
        return

    sheet_link = f"https://docs.google.com/spreadsheets/d/{wb_id}/edit#gid={sheet_id}"

    gap_values = ", ".join(str(r["gap_value"]) for r in empty_rows)
    
    mail_data = {
        "caller_id": caller_id,
        "time_str": time_str,
        "date_str": date_str,
        "nick_name": nick_name,
        "gap_values": gap_values,
        "empty_count": str(len(empty_rows)),
        "sheet_link": sheet_link,
    }

    if mail_mode == "per_row":
        for r in empty_rows:
            try:
                mail_data["gap_value"] = r["gap_value"]
                mail_data["row_number"] = str(r["row_index"])
                mail_service.send_mail(mail_data=mail_data)
            except Exception as e:
                print(f"delayed_gaps_check: per-row mail failed: {e}", file=sys.stderr)
    else:
        try:
            mail_service.send_mail(mail_data=mail_data)
        except Exception as e:
            print(f"delayed_gaps_check: summary mail failed: {e}", file=sys.stderr)


def schedule_delayed_gaps_check(
    delayed_config: Dict[str, Any],
    sheet_config: Dict[str, Any],
    context: Dict[str, Any],
    config_path: str,
) -> None:
    """
    Schedule the delayed task to run after delay_minutes.
    context should include: caller_id, time_str, date_str, nick_name, customers_file_name.
    """
    global _scheduler
    if _scheduler is None:
        print("delayed_gaps_check: scheduler not started, cannot schedule", file=sys.stderr)
        return

    if not delayed_config.get("enabled", False):
        return

    delay_minutes = int(delayed_config.get("delay_minutes", 60))
    # Use local machine time for scheduling (not UTC)
    run_at = datetime.now() + timedelta(minutes=delay_minutes)

    job_context = {
        "sheet_config": sheet_config,
        "caller_id": context.get("caller_id"),
        "time_str": context.get("time_str"),
        "date_str": context.get("date_str"),
        "nick_name": context.get("nick_name"),
        "customers_file_name": context.get("customers_file_name"),
        "config_path": config_path,
    }

    _scheduler.add_job(
        run_delayed_gaps_check,
        trigger="date",
        run_date=run_at,
        id=f"delayed_gaps_{context.get('caller_id', '')}_{context.get('time_str', '')}_{run_at.timestamp()}",
        args=[job_context],
        replace_existing=False,
    )
    print(f"delayed_gaps_check: scheduled for {run_at} (in {delay_minutes} min)", file=sys.stderr)


def get_scheduler() -> BackgroundScheduler:
    """Return the global scheduler, creating it if needed."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BackgroundScheduler()
    return _scheduler


def start_scheduler() -> None:
    """Start the global scheduler (call once on app startup)."""
    s = get_scheduler()
    if not s.running:
        s.start()
        print("delayed_gaps_check: scheduler started", file=sys.stderr)


def shutdown_scheduler() -> None:
    """Shutdown the global scheduler (e.g. on app shutdown)."""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        print("delayed_gaps_check: scheduler stopped", file=sys.stderr)
