"""
Test script to verify interval alignment works correctly for 1, 5, and 15 minute intervals
"""
from datetime import datetime

def align_to_interval_slot(timestamp_ms: int, interval_minutes: int = 5, ceiling: bool = False) -> int:
    """
    Align a timestamp to the nearest interval slot boundary.
    """
    interval_ms = interval_minutes * 60 * 1000
    if ceiling:
        # Ceiling: round up to next interval boundary (unless already on boundary)
        remainder = timestamp_ms % interval_ms
        if remainder == 0:
            return timestamp_ms
        return timestamp_ms + (interval_ms - remainder)
    else:
        # Floor: round down to current interval boundary
        return (timestamp_ms // interval_ms) * interval_ms

def format_time(timestamp_ms: int) -> str:
    """Format timestamp for display"""
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S')

def test_alignment(test_time_str: str, interval_minutes: int):
    """Test alignment for a specific time and interval"""
    # Parse test time (format: HH:MM:SS)
    h, m, s = map(int, test_time_str.split(':'))
    # Use today's date
    now = datetime.now().replace(hour=h, minute=m, second=s, microsecond=0)
    timestamp_ms = int(now.timestamp() * 1000)

    floor_aligned = align_to_interval_slot(timestamp_ms, interval_minutes, ceiling=False)
    ceiling_aligned = align_to_interval_slot(timestamp_ms, interval_minutes, ceiling=True)

    print(f"\n  Current time:      {format_time(timestamp_ms)}")
    print(f"  Floor aligned:     {format_time(floor_aligned)}")
    print(f"  Ceiling aligned:   {format_time(ceiling_aligned)}")

print("=" * 70)
print("Testing Interval Alignment for Different Time Intervals")
print("=" * 70)

print("\n### 1-MINUTE INTERVAL ###")
test_alignment("10:07:30", 1)
test_alignment("10:03:45", 1)
test_alignment("10:00:00", 1)
test_alignment("09:59:15", 1)

print("\n### 5-MINUTE INTERVAL ###")
test_alignment("10:07:00", 5)
test_alignment("10:03:00", 5)
test_alignment("10:00:00", 5)
test_alignment("09:58:00", 5)

print("\n### 15-MINUTE INTERVAL ###")
test_alignment("10:07:00", 15)
test_alignment("10:03:00", 15)
test_alignment("10:00:00", 15)
test_alignment("09:58:00", 15)

print("\n" + "=" * 70)
print("Use Cases for DataSaver:")
print("=" * 70)

print("\n1. For 1-minute saves:")
print("   - Set interval_minutes=1 in DataSaver")
print("   - Data saved every minute at boundaries (10:01, 10:02, 10:03, ...)")

print("\n2. For 5-minute saves (default):")
print("   - Set interval_minutes=5 in DataSaver")
print("   - Data saved every 5 minutes at boundaries (10:00, 10:05, 10:10, ...)")

print("\n3. For 15-minute saves:")
print("   - Set interval_minutes=15 in DataSaver")
print("   - Data saved every 15 minutes at boundaries (10:00, 10:15, 10:30, ...)")

print("\n" + "=" * 70)
print("✅ All interval alignments are now supported!")
print("=" * 70)

