"""
PayCall API utilities.

This module provides functions to interact with the PayCall WebService API.
"""

import sys
import time
from typing import Optional, Dict, Any, Tuple

from .config import _get_default_config
from common_utils.config_manager import ConfigManager
from datetime import datetime


def _load_paycall_config(config_manager: ConfigManager) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
    """
    Load and validate PayCall configuration.
    
    Returns:
        Tuple of (config_dict, retry_config, is_valid)
    """
    config = _get_default_config(config_manager)
    paycall_account = config.get_paycall_account()
    paycall_api_url = config.get_paycall_api_url()
    paycall_limit = config.get_paycall_limit()
    paycall_order_by = config.get_paycall_order_by()
    retry_config = config.get_paycall_retry_config()

    api_url = paycall_api_url
    username = paycall_account.get("email")
    password = paycall_account.get("password")
    user_id = paycall_account.get("paycall_id")
    
    print(f"limit: {paycall_limit} order by: {paycall_order_by}", file=sys.stderr)

    if not api_url or not username or not password:
        print("⚠️  PayCall config missing url/username/password; skipping call.", file=sys.stderr)
        return {}, {}, False
    
    config_dict = {
        'api_url': api_url,
        'username': username,
        'password': password,
        'user_id': user_id,
        'limit': paycall_limit,
        'order_by': paycall_order_by
    }
    
    return config_dict, retry_config, True


def _build_payload(
    start_date: datetime,
    end_date: datetime,
    caller_id: Optional[str],
    user_id: Optional[str],
    limit: int,
    order_by: str,
    from_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Build PayCall API request payload.
    
    Args:
        start_date: Start date for filtering
        end_date: End date for filtering
        caller_id: Optional caller ID
        user_id: PayCall user ID
        limit: Maximum records per request
        order_by: Sort order (asc/desc)
        from_id: Optional fromId for pagination
        
    Returns:
        Payload dictionary for API request
    """
    start_date_str = start_date.strftime("%d-%m-%Y")
    end_date_str = end_date.strftime("%d-%m-%Y")
    
    payload = {
        "action": "getCalls",
        "fromDate": start_date_str,
        "toDate": end_date_str,
        "uId": user_id or "",
        "callerId": caller_id or "",
        "limit": limit,
        "orderBy": order_by,
        "out": "json"
    }
    
    if from_id:
        payload["fromId"] = from_id
    
    return payload


def _make_paycall_request_with_retry(
    api_url: str,
    payload: Dict[str, Any],
    username: str,
    password: str,
    retry_config: Dict[str, Any],
    page: int
) -> Any:
    """
    Make PayCall API request with retry mechanism.
    
    Args:
        api_url: PayCall API URL
        payload: Request payload
        username: API username
        password: API password
        retry_config: Retry configuration dictionary
        page: Current page number for logging
        
    Returns:
        Response object if successful
        
    Raises:
        requests.exceptions.Timeout: If request times out after all retries
        requests.exceptions.RequestException: If request fails after all retries
        Exception: If unexpected error occurs after all retries
    """
    try:
        import requests
    except ImportError:
        error_msg = "requests library not installed; cannot call PayCall"
        print(f"⚠️  {error_msg}", file=sys.stderr)
        raise ImportError(error_msg)

    max_retries = retry_config['max_retries']
    backoff_factor = retry_config['backoff_factor']
    retryable_status_codes = retry_config['retryable_status_codes']
    retry_on_timeout = retry_config['retry_on_timeout']
    
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                wait_time = backoff_factor * (2 ** (attempt - 1))
                print(f"   Retrying request (attempt {attempt + 1}/{max_retries + 1}) after {wait_time:.1f}s...", file=sys.stderr)
                time.sleep(wait_time)
            else:
                print(f"📞 Fetching PayCall data (page {page})...", file=sys.stderr)
            
            response = requests.post(
                api_url,
                data=payload,
                auth=(username, password),
                timeout=30,
            )
            
            # Check if status code is retryable
            if response.status_code in retryable_status_codes:
                if attempt < max_retries:
                    print(f"   Received retryable status code {response.status_code}, will retry...", file=sys.stderr)
                    last_exception = requests.exceptions.HTTPError(
                        f"PayCall returned retryable status code {response.status_code}"
                    )
                    continue
                else:
                    # Exhausted retries, store exception and break
                    last_exception = requests.exceptions.HTTPError(
                        f"PayCall returned retryable status code {response.status_code} after {max_retries + 1} attempts"
                    )
                    break
            
            # If we get here, request was successful
            response.raise_for_status()
            return response
                
        except requests.exceptions.Timeout as e:
            last_exception = e
            if retry_on_timeout and attempt < max_retries:
                print(f"   Request timeout, will retry...", file=sys.stderr)
                continue
            else:
                # Can't retry timeout, break to raise exception
                break
                
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < max_retries:
                print(f"   Request failed: {e}, will retry...", file=sys.stderr)
                continue
            else:
                # Exhausted retries, break to raise exception
                break
                
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                print(f"   Unexpected error: {e}, will retry...", file=sys.stderr)
                continue
            else:
                # Exhausted retries, break to raise exception
                break
    
    # All retries exhausted, raise the last exception once
    if last_exception is None:
        raise Exception(f"PayCall request failed after all {max_retries + 1} retry attempts")
    
    # Raise appropriate exception based on type
    if isinstance(last_exception, requests.exceptions.Timeout):
        error_msg = f"PayCall request timeout after {max_retries + 1} attempts: {last_exception}"
        print(f"⚠️  {error_msg}", file=sys.stderr)
        raise requests.exceptions.Timeout(error_msg) from last_exception
    elif isinstance(last_exception, requests.exceptions.RequestException):
        error_msg = f"PayCall request failed after {max_retries + 1} attempts: {last_exception}"
        print(f"⚠️  {error_msg}", file=sys.stderr)
        raise requests.exceptions.RequestException(error_msg) from last_exception
    else:
        error_msg = f"PayCall request failed with unexpected error after {max_retries + 1} attempts: {last_exception}"
        print(f"⚠️  {error_msg}", file=sys.stderr)
        raise Exception(error_msg) from last_exception


def _parse_response(response: Any) -> Optional[list]:
    """
    Parse PayCall API response.
    
    Args:
        response: Response object from requests
        
    Returns:
        List of call records if successful, None otherwise
    """
    # Check if response has content
    if response is None:
        print(f"⚠️  Response is None", file=sys.stderr)
        raise Exception("Response object is None")
    
    if not response.text or response.text.strip() == '' or response.text.strip() == 'null' or response.text.strip() == 'undefined':
        print(f"⚠️  Empty response from PayCall API (status {response.status_code})", file=sys.stderr)
        return []
    
    try:
        rows = response.json()
    except ValueError as e:
        # JSON decode error - response is not valid JSON
        print(f"⚠️  Failed to parse PayCall response as JSON: {e}", file=sys.stderr)
        print(f"   Response status: {response.status_code}", file=sys.stderr)
        print(f"   Response text (first 500 chars): {response.text[:500]}", file=sys.stderr)
        raise Exception(f"Failed to parse PayCall response as JSON: {e}")
    except Exception as e:
        print(f"⚠️  Failed to parse PayCall response: {e}", file=sys.stderr)
        print(f"   Response status: {response.status_code}", file=sys.stderr)
        print(f"   Response text (first 500 chars): {response.text[:500]}", file=sys.stderr)
        raise Exception(f"Failed to parse PayCall response: {e}")

    # Check if parsed data is a list
    if not isinstance(rows, list):
        print(f"⚠️  Unexpected response format from PayCall API response text: {response.text}", file=sys.stderr)
        print(f"   Expected a list, got {type(rows).__name__}: {rows}", file=sys.stderr)
        print(f"   Response status: {response.status_code}", file=sys.stderr)
        # If it's a dict, it might be an error response
        if isinstance(rows, dict):
            print(f"   Response dict keys: {list(rows.keys())}", file=sys.stderr)
            # Return empty list instead of raising - might be end of data
            return []
        raise Exception(f"Unexpected response format from PayCall: expected list, got {type(rows).__name__}")

    return rows


def _filter_calls_by_time(
    rows: list,
    start_date: datetime,
    end_date: datetime
) -> Tuple[list, bool, Optional[str]]:
    """
    Filter calls by time range and extract fromId for pagination.
    
    Args:
        rows: List of call records
        start_date: Minimum start date for filtering
        end_date: Maximum start date for filtering
        
    Returns:
        Tuple of (filtered_rows, reached_end_time, from_id)
    """
    filtered_rows = []
    reached_end_time = False
    from_id = None

    if not rows:
        return filtered_rows, reached_end_time, from_id

    # Get fromId from last row for pagination
    from_id = rows[-1].get("ID")

    for row in rows:
        if "START" not in row:
            continue
        
        try:
            # Parse START time: format is "%Y-%m-%d %H:%M:%S"
            call_start = datetime.strptime(row["START"], "%Y-%m-%d %H:%M:%S")
            
            # If call starts after end_date, we've reached the end
            if call_start > end_date:
                reached_end_time = True
                print(f"   Reached end time (call START {call_start} > end_date {end_date})", file=sys.stderr)
                break
            
            # Only include calls that start on or after start_date
            if call_start >= start_date:
                filtered_rows.append(row)
                
        except ValueError as e:
            print(f"⚠️  Failed to parse time '{row.get('START')}': {e}", file=sys.stderr)
            continue

    return filtered_rows, reached_end_time, from_id


def get_paycall_data(
    config_manager: ConfigManager,
    caller_id: Optional[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
):
    """
    Fetch call data from PayCall according to the provided filters.

    Args:
        caller_id: Caller phone (without leading 0) as string.
        start_date: From date (datetime object).
        end_date: To date (datetime object).

    Returns:
        List of rows (dict) parsed from the PayCall JSON response.
    """
    # Load configuration
    config_dict, retry_config, is_valid = _load_paycall_config(config_manager)
    if not is_valid:
        return []

    all_rows = []
    page = 1
    reached_end_time = False
    from_id = None

    print(f"🔍 Fetching calls from {start_date} to {end_date}", file=sys.stderr)

    while not reached_end_time:
        # Build payload
        payload = _build_payload(
            start_date=start_date,
            end_date=end_date,
            caller_id=caller_id,
            user_id=config_dict['user_id'],
            limit=config_dict['limit'],
            order_by=config_dict['order_by'],
            from_id=from_id
        )

        # Make request with retry
        try:
            response = _make_paycall_request_with_retry(
                api_url=config_dict['api_url'],
                payload=payload,
                username=config_dict['username'],
                password=config_dict['password'],
                retry_config=retry_config,
                page=page
            )
                
        except Exception as e:
            print(f"⚠️  Failed to fetch PayCall data on page {page}: {e}", file=sys.stderr)
            raise

        # Parse response
        rows = _parse_response(response)
        if rows is None:
            break

        if not rows:
            print(f"   No more records to fetch.", file=sys.stderr)
            break

        # If the api_url contains "v2", reverse the response (bytes or string)
        if "v2" in config_dict['api_url']:
            rows = rows[::-1]

        # Filter calls by time
        filtered_rows, reached_end_time, from_id = _filter_calls_by_time(
            rows=rows,
            start_date=start_date,
            end_date=end_date
        )

        all_rows.extend(filtered_rows)
        print(f"   Retrieved {len(rows)} records, added {len(filtered_rows)} to results (total: {len(all_rows)})", file=sys.stderr)

        # Check if we should stop fetching
        if reached_end_time:
            break

        if len(rows) < config_dict['limit']:
            print(f"   Reached end of data (got {len(rows)} < limit {config_dict['limit']})", file=sys.stderr)
            break

        page += 1

    print(f"📥 Total retrieved: {len(all_rows)} records from PayCall (filtered by time range)", file=sys.stderr)
    print(f"PayCall config used: api_url={config_dict['api_url']}, username={config_dict['username']}, paycall_id={config_dict['user_id']}, limit={config_dict['limit']}, order_by={config_dict['order_by']}", file=sys.stderr)
    
    return all_rows
