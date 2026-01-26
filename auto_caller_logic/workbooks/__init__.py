"""
Workbook modules package.

Each workbook type has its own module with its Excel file definition.
"""

from .base_workbook import BaseExcelToGoogleWorkbook, ExcelToGoogleWorkbook
from .intermediate_workbook import IntermediateWorkbook
from .auto_dialer_workbook import AutoDialerWorkbook
from .filter_workbook import FilterWorkbook
from .callers_gaps_workbook import CallersGapWorkbook

__all__ = [
    'ExcelToGoogleWorkbook',
    'BaseExcelToGoogleWorkbook',
    'IntermediateWorkbook',
    'AutoDialerWorkbook',
    'FilterWorkbook',
    'CallersGapWorkbook',
]

