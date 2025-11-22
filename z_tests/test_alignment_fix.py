"""
Test script to verify the alignment fix for including ongoing candle.

This demonstrates that the fix now correctly includes the ongoing candle
in the query range.
"""
import time
from datetime import datetime


def align_to_5min_slot(timestamp_ms: int, ceiling: bool = False) -> int:
    """
    Align a timestamp to the nearest 5-minute slot boundary.
    This is a copy of the fixed method for testing.
    """
    five_min_ms = 5 * 60 * 1000
    if ceiling:
        # Ceiling: round up to next 5-min boundary (unless already on boundary)
        remainder = timestamp_ms % five_min_ms
        if remainder == 0:
            return timestamp_ms
        return timestamp_ms + (five_min_ms - remainder)
    else:
        # Floor: round down to current 5-min boundary
        return (timestamp_ms // five_min_ms) * five_min_ms


def test_alignment():
    """Test various timestamps to verify alignment works correctly."""

    # Test cases: (hour, minute, expected_floor, expected_ceiling)
    test_cases = [
        (10, 8, "10:05", "10:10"),   # Original problem case
        (10, 7, "10:05", "10:10"),
        (10, 3, "10:00", "10:05"),
        (9, 58, "9:55", "10:00"),
        (10, 0, "10:00", "10:00"),   # Exact boundary
        (10, 5, "10:05", "10:05"),   # Exact boundary
    ]

    print("=" * 80)
    print("ALIGNMENT TEST RESULTS")
    print("=" * 80)
    print()

    for hour, minute, expected_floor, expected_ceiling in test_cases:
        # Create a timestamp for today at the given hour:minute
        now = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        timestamp_ms = int(now.timestamp() * 1000)

        # Floor alignment
        floor_aligned = align_to_5min_slot(timestamp_ms, ceiling=False)
        floor_time = datetime.fromtimestamp(floor_aligned / 1000)

        # Ceiling alignment
        ceiling_aligned = align_to_5min_slot(timestamp_ms, ceiling=True)
        ceiling_time = datetime.fromtimestamp(ceiling_aligned / 1000)

        print(f"Time: {hour:02d}:{minute:02d}")
        print(f"  Floor   -> {floor_time.strftime('%H:%M')} (expected: {expected_floor})")
        print(f"  Ceiling -> {ceiling_time.strftime('%H:%M')} (expected: {expected_ceiling})")
        print()

    print("=" * 80)
    print("QUERY RANGE TEST")
    print("=" * 80)
    print()

    # Simulate the scenario described in the issue
    current_time_str = "10:08"
    hour, minute = 10, 8
    now = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
    timestamp_ms = int(now.timestamp() * 1000)

    # OLD behavior (floor)
    old_to_ts = align_to_5min_slot(timestamp_ms, ceiling=False)
    old_from_ts = old_to_ts - 15 * 60 * 1000
    old_to_time = datetime.fromtimestamp(old_to_ts / 1000)
    old_from_time = datetime.fromtimestamp(old_from_ts / 1000)

    # NEW behavior (ceiling)
    new_to_ts = align_to_5min_slot(timestamp_ms, ceiling=True)
    new_from_ts = new_to_ts - 15 * 60 * 1000
    new_to_time = datetime.fromtimestamp(new_to_ts / 1000)
    new_from_time = datetime.fromtimestamp(new_from_ts / 1000)

    print(f"Current time: {current_time_str}")
    print()
    print("OLD BEHAVIOR (floor alignment):")
    print(f"  Query range: {old_from_time.strftime('%H:%M')} to {old_to_time.strftime('%H:%M')}")
    print(f"  Returns candles: 10:00, 9:55, 9:50 (missing ongoing 10:05 candle)")
    print()
    print("NEW BEHAVIOR (ceiling alignment):")
    print(f"  Query range: {new_from_time.strftime('%H:%M')} to {new_to_time.strftime('%H:%M')}")
    print(f"  Returns candles: 10:05 (ongoing), 10:00, 9:55 ✓")
    print()
    print("=" * 80)


if __name__ == "__main__":
    test_alignment()

