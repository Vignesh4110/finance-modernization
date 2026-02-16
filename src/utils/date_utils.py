"""
AS400 Date and Time Utilities

Handles conversion between IBM i date formats and Python dates.

IBM i Date Formats:
- CYYMMDD: Century-Year-Month-Day (e.g., 1240115 = 2024-01-15)
  - C=0 for 1900s (0991231 = 1999-12-31)
  - C=1 for 2000s (1240115 = 2024-01-15)
- HHMMSS: Hour-Minute-Second (e.g., 143052 = 14:30:52)

Author: Vignesh
"""

from datetime import date, datetime, time
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


def cyymmdd_to_date(cyymmdd: Union[str, int]) -> Optional[date]:
    """
    Convert IBM CYYMMDD format to Python date.
    
    Args:
        cyymmdd: Date in CYYMMDD format (string or int)
                 C = 0 for 1900s, 1 for 2000s
                 
    Returns:
        Python date object, or None if invalid/empty
        
    Examples:
        >>> cyymmdd_to_date(1240115)
        datetime.date(2024, 1, 15)
        >>> cyymmdd_to_date(0991231)
        datetime.date(1999, 12, 31)
        >>> cyymmdd_to_date(0)
        None
    """
    if cyymmdd is None:
        return None
    
    # Convert to string and strip whitespace
    date_str = str(cyymmdd).strip()
    
    # Handle empty or zero dates
    if not date_str or date_str == "0" or date_str == "0000000":
        return None
    
    # Pad to 7 digits if necessary
    date_str = date_str.zfill(7)
    
    if len(date_str) != 7:
        logger.warning(f"Invalid CYYMMDD format: {cyymmdd}")
        return None
    
    try:
        century = int(date_str[0])
        year_part = int(date_str[1:3])
        month = int(date_str[3:5])
        day = int(date_str[5:7])
        
        # Calculate full year
        if century == 0:
            year = 1900 + year_part
        else:
            year = 2000 + year_part
        
        return date(year, month, day)
    
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse CYYMMDD '{cyymmdd}': {e}")
        return None


def date_to_cyymmdd(d: Optional[date]) -> str:
    """
    Convert Python date to IBM CYYMMDD format.
    
    Args:
        d: Python date object
        
    Returns:
        String in CYYMMDD format (7 characters)
        
    Examples:
        >>> date_to_cyymmdd(date(2024, 1, 15))
        '1240115'
        >>> date_to_cyymmdd(date(1999, 12, 31))
        '0991231'
    """
    if d is None:
        return "0000000"
    
    century = 1 if d.year >= 2000 else 0
    year_part = d.year - (2000 if century == 1 else 1900)
    
    return f"{century}{year_part:02d}{d.month:02d}{d.day:02d}"


def hhmmss_to_time(hhmmss: Union[str, int]) -> Optional[time]:
    """
    Convert IBM HHMMSS format to Python time.
    
    Args:
        hhmmss: Time in HHMMSS format (string or int)
        
    Returns:
        Python time object, or None if invalid/empty
        
    Examples:
        >>> hhmmss_to_time(143052)
        datetime.time(14, 30, 52)
        >>> hhmmss_to_time(93000)
        datetime.time(9, 30, 0)
    """
    if hhmmss is None:
        return None
    
    # Convert to string and strip
    time_str = str(hhmmss).strip()
    
    # Handle empty or zero times
    if not time_str or time_str == "0" or time_str == "000000":
        return None
    
    # Pad to 6 digits
    time_str = time_str.zfill(6)
    
    if len(time_str) != 6:
        logger.warning(f"Invalid HHMMSS format: {hhmmss}")
        return None
    
    try:
        hour = int(time_str[0:2])
        minute = int(time_str[2:4])
        second = int(time_str[4:6])
        
        return time(hour, minute, second)
    
    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse HHMMSS '{hhmmss}': {e}")
        return None


def time_to_hhmmss(t: Optional[time]) -> str:
    """
    Convert Python time to IBM HHMMSS format.
    
    Args:
        t: Python time object
        
    Returns:
        String in HHMMSS format (6 characters)
    """
    if t is None:
        return "000000"
    
    return f"{t.hour:02d}{t.minute:02d}{t.second:02d}"


def parse_packed_decimal(value: str, decimal_places: int = 0) -> Optional[float]:
    """
    Parse a packed decimal value from AS400 text export.
    
    In CPYTOIMPF exports, packed decimals appear as right-justified
    numeric strings. The decimal point position is implied by the
    field definition.
    
    Args:
        value: String representation of the number
        decimal_places: Number of implied decimal places
        
    Returns:
        Float value, or None if invalid
        
    Examples:
        >>> parse_packed_decimal("00012345", 2)
        123.45
        >>> parse_packed_decimal("  5000", 0)
        5000.0
    """
    if value is None:
        return None
    
    value_str = str(value).strip()
    
    if not value_str:
        return None
    
    try:
        # Remove any existing decimal point (shouldn't be there, but just in case)
        value_str = value_str.replace(".", "")
        
        # Convert to integer first
        int_value = int(value_str)
        
        # Apply decimal places
        if decimal_places > 0:
            return int_value / (10 ** decimal_places)
        else:
            return float(int_value)
    
    except ValueError as e:
        logger.warning(f"Failed to parse packed decimal '{value}': {e}")
        return None


def calculate_days_between_cyymmdd(date1: Union[str, int], date2: Union[str, int]) -> Optional[int]:
    """
    Calculate days between two CYYMMDD dates.
    
    Useful for aging calculations directly on AS400 format dates.
    
    Args:
        date1: First date in CYYMMDD format
        date2: Second date in CYYMMDD format
        
    Returns:
        Number of days (date2 - date1), or None if invalid dates
    """
    d1 = cyymmdd_to_date(date1)
    d2 = cyymmdd_to_date(date2)
    
    if d1 is None or d2 is None:
        return None
    
    return (d2 - d1).days


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def is_valid_cyymmdd(cyymmdd: Union[str, int]) -> bool:
    """Check if a CYYMMDD value is valid."""
    return cyymmdd_to_date(cyymmdd) is not None


def is_valid_hhmmss(hhmmss: Union[str, int]) -> bool:
    """Check if a HHMMSS value is valid."""
    return hhmmss_to_time(hhmmss) is not None


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    # Test CYYMMDD conversion
    print("Testing CYYMMDD conversion:")
    test_dates = [1240115, "1240115", 991231, "0991231", 0, "0000000", None]
    for td in test_dates:
        result = cyymmdd_to_date(td)
        print(f"  {td!r:>12} -> {result}")
    
    print("\nTesting date to CYYMMDD:")
    test_dates2 = [date(2024, 1, 15), date(1999, 12, 31), None]
    for td in test_dates2:
        result = date_to_cyymmdd(td)
        print(f"  {td} -> {result}")
    
    print("\nTesting HHMMSS conversion:")
    test_times = [143052, "093000", 0, None]
    for tt in test_times:
        result = hhmmss_to_time(tt)
        print(f"  {tt!r:>10} -> {result}")
    
    print("\nTesting packed decimal parsing:")
    test_decimals = [("00012345", 2), ("  5000", 0), ("123456789", 2)]
    for val, dec in test_decimals:
        result = parse_packed_decimal(val, dec)
        print(f"  {val!r} (dec={dec}) -> {result}")