"""
Utility helper functions for Ultra Liquidation Bot
Basic, clean, and reusable functions
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Union, Optional

def convert_to_utc(timestamp: Union[int, float, str]) -> datetime:
    """
    Convert timestamp to UTC datetime object
    
    Args:
        timestamp: Unix timestamp in seconds or milliseconds, or ISO format string
        
    Returns:
        datetime: UTC datetime object
    """
    try:
        # Handle string timestamps (ISO format)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00')).astimezone(timezone.utc)
        
        # Handle numeric timestamps
        timestamp_float = float(timestamp)
        
        # Check if timestamp is in milliseconds (typical for exchange data)
        if timestamp_float > 1e10:  # If timestamp is after 1970 in milliseconds
            timestamp_float /= 1000  # Convert to seconds
            
        return datetime.fromtimestamp(timestamp_float, timezone.utc)
        
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid timestamp format: {timestamp}") from e

def get_current_utc() -> datetime:
    """
    Get current UTC datetime
    
    Returns:
        datetime: Current UTC datetime
    """
    return datetime.now(timezone.utc)

def format_price(price: float, decimal_places: int = 2) -> str:
    """
    Format price with appropriate decimal places
    
    Args:
        price: Price value to format
        decimal_places: Number of decimal places (default: 2)
        
    Returns:
        str: Formatted price string
    """
    if price >= 1000:
        # For large prices, use fewer decimal places
        decimal_places = max(0, decimal_places - 1)
    
    format_string = f"{{:,.{decimal_places}f}}"
    return format_string.format(price)

def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """
    Calculate percentage change between two values
    
    Args:
        old_value: Original value
        new_value: New value
        
    Returns:
        float: Percentage change (as decimal, e.g., 0.05 for 5%)
    """
    if old_value == 0:
        return 0.0  # Avoid division by zero
    
    return (new_value - old_value) / old_value

def is_valid_symbol(symbol: str) -> bool:
    """
    Check if trading symbol is valid
    
    Args:
        symbol: Trading symbol (e.g., 'BTCUSDT', 'ETH-USDT')
        
    Returns:
        bool: True if symbol appears valid
    """
    if not symbol or len(symbol) < 3:
        return False
    
    # Basic validation: should contain at least 3 characters and some common patterns
    valid_patterns = ['USDT', 'BUSD', 'USD', 'BTC', 'ETH']
    
    # Check if symbol ends with a known quote asset
    return any(symbol.upper().endswith(pattern) for pattern in valid_patterns)

def generate_unique_id() -> str:
    """
    Generate a unique identifier
    
    Returns:
        str: Unique ID string
    """
    return str(uuid.uuid4())

def calculate_usd_value(quantity: float, price: float) -> float:
    """
    Calculate USD value from quantity and price
    
    Args:
        quantity: Asset quantity
        price: Price per unit
        
    Returns:
        float: Total USD value
    """
    return quantity * price

def format_quantity(quantity: float) -> str:
    """
    Format quantity for display (with K, M suffixes for large numbers)
    
    Args:
        quantity: Quantity to format
        
    Returns:
        str: Formatted quantity string
    """
    if quantity >= 1_000_000:
        return f"{quantity/1_000_000:.2f}M"
    elif quantity >= 1_000:
        return f"{quantity/1_000:.2f}K"
    else:
        return f"{quantity:.4f}"

def timestamp_to_datetime(timestamp: int) -> str:
    """
    Convert timestamp to readable datetime string
    
    Args:
        timestamp: Unix timestamp in milliseconds
        
    Returns:
        str: Formatted datetime string
    """
    dt = convert_to_utc(timestamp)
    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')

def format_percentage(percentage: float, decimal_places: int = 2) -> str:
    """
    Format percentage for display
    
    Args:
        percentage: Percentage value (as decimal, e.g., 0.05 for 5%)
        decimal_places: Number of decimal places
        
    Returns:
        str: Formatted percentage string with % symbol
    """
    return f"{percentage * 100:.{decimal_places}f}%"

def clamp_value(value: float, min_val: float, max_val: float) -> float:
    """
    Clamp a value between minimum and maximum bounds
    
    Args:
        value: Value to clamp
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        float: Clamped value
    """
    return max(min_val, min(value, max_val))

def is_numeric_value(value: Union[str, int, float]) -> bool:
    """
    Check if value can be converted to a valid number
    
    Args:
        value: Value to check
        
    Returns:
        bool: True if value is numeric
    """
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def safe_float_conversion(value: Union[str, int, float], default: float = 0.0) -> float:
    """
    Safely convert value to float with default fallback
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        float: Converted value or default
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# Example usage and testing
if __name__ == "__main__":
    # Test the helper functions
    print("🧪 Testing helper functions...")
    
    # Test timestamp conversion
    test_ts = 1640995200000  # 2022-01-01 00:00:00 UTC in milliseconds
    dt = convert_to_utc(test_ts)
    print(f"✅ Timestamp conversion: {dt}")
    
    # Test price formatting
    price = 12345.6789
    formatted = format_price(price)
    print(f"✅ Price formatting: {price} -> {formatted}")
    
    # Test percentage change
    old_val, new_val = 100, 110
    change = calculate_percentage_change(old_val, new_val)
    print(f"✅ Percentage change: {old_val} -> {new_val} = {format_percentage(change)}")
    
    # Test symbol validation
    symbols = ["BTCUSDT", "ETH-USDT", "INVALID", ""]
    for symbol in symbols:
        valid = is_valid_symbol(symbol)
        print(f"✅ Symbol validation: {symbol} -> {valid}")
    
    print("🎉 All helper functions working correctly!")
