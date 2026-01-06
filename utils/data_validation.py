"""
Utility decorators and validators for robust data handling.
"""
import asyncio
import functools
from typing import Optional, Dict, Any
from config.logger import logger


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator for async functions with exponential backoff retry.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap in seconds
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            f"Retry {attempt + 1}/{max_retries} for {func.__name__}: {e}. "
                            f"Waiting {delay:.1f}s"
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"All {max_retries} retries failed for {func.__name__}: {e}")
            raise last_exception
        return wrapper
    return decorator


def validate_ohlc(
    open_val: Optional[float],
    high_val: Optional[float],
    low_val: Optional[float],
    close_val: Optional[float],
    volume: Optional[int] = None,
    fix: bool = True
) -> Dict[str, Any]:
    """
    Validate and optionally fix OHLC data.

    Rules:
    - High >= Open, Close, Low
    - Low <= Open, Close, High
    - All values should be positive
    - Volume should be non-negative

    Args:
        open_val, high_val, low_val, close_val: OHLC values
        volume: Optional volume value
        fix: If True, attempt to fix invalid values

    Returns:
        Dict with 'valid', 'fixed', 'data', and 'errors' keys
    """
    result = {
        'valid': True,
        'fixed': False,
        'data': {'open': open_val, 'high': high_val, 'low': low_val, 'close': close_val},
        'errors': []
    }

    if volume is not None:
        result['data']['volume'] = volume

    # Skip validation if all values are None
    if all(v is None for v in [open_val, high_val, low_val, close_val]):
        return result

    # Check for None values in partial data
    if any(v is None for v in [open_val, high_val, low_val, close_val]):
        result['valid'] = False
        result['errors'].append("Incomplete OHLC data (some values are None)")
        return result

    # Check for negative values
    if any(v < 0 for v in [open_val, high_val, low_val, close_val]):
        result['valid'] = False
        result['errors'].append("Negative price values detected")
        if fix:
            result['data'] = {k: abs(v) if v else v for k, v in result['data'].items()}
            result['fixed'] = True

    # Validate High >= all others
    if high_val < max(open_val, close_val, low_val):
        result['valid'] = False
        result['errors'].append(f"High ({high_val}) is not the highest value")
        if fix:
            result['data']['high'] = max(open_val, high_val, low_val, close_val)
            result['fixed'] = True

    # Validate Low <= all others
    if low_val > min(open_val, close_val, high_val):
        result['valid'] = False
        result['errors'].append(f"Low ({low_val}) is not the lowest value")
        if fix:
            result['data']['low'] = min(open_val, high_val, low_val, close_val)
            result['fixed'] = True

    # Validate volume
    if volume is not None and volume < 0:
        result['valid'] = False
        result['errors'].append(f"Negative volume: {volume}")
        if fix:
            result['data']['volume'] = abs(volume)
            result['fixed'] = True

    return result


def validate_volume(
    volume: Optional[int],
    previous_volume: Optional[int] = None,
    max_volume_spike_ratio: float = 100.0
) -> Dict[str, Any]:
    """
    Validate volume data for anomalies.

    Args:
        volume: Current volume value
        previous_volume: Previous period's volume for comparison
        max_volume_spike_ratio: Maximum allowed ratio vs previous volume

    Returns:
        Dict with 'valid', 'warnings', and 'volume' keys
    """
    result = {'valid': True, 'warnings': [], 'volume': volume}

    if volume is None:
        result['volume'] = 0
        result['warnings'].append("Volume is None, defaulting to 0")
        return result

    if volume < 0:
        result['valid'] = False
        result['warnings'].append(f"Negative volume: {volume}")
        result['volume'] = 0
        return result

    # Check for suspicious volume spikes
    if previous_volume and previous_volume > 0:
        ratio = volume / previous_volume
        if ratio > max_volume_spike_ratio:
            result['warnings'].append(
                f"Volume spike detected: {volume} vs prev {previous_volume} (ratio: {ratio:.1f}x)"
            )

    return result

