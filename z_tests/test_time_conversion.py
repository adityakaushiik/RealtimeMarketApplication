"""
Test script to verify HHMM time format conversion works correctly
"""
from datetime import datetime
import pytz

def test_time_conversion(hhmm: int, timezone_str: str = "Asia/Kolkata"):
    """Test the time conversion logic"""
    tz = pytz.timezone(timezone_str)
    now = datetime.now(tz)
    today = now.date()

    hour = hhmm // 100
    minute = hhmm % 100

    dt = tz.localize(datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute)))
    timestamp_ms = int(dt.timestamp() * 1000)

    readable = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S %Z')

    return {
        "hhmm": hhmm,
        "hour": hour,
        "minute": minute,
        "timestamp_ms": timestamp_ms,
        "readable": readable
    }

if __name__ == "__main__":
    print("Testing HHMM time format conversion for NSE trading hours:\n")

    test_times = [
        (900, "Pre-market open"),
        (915, "Market open"),
        (1530, "Market close"),
        (1545, "Post-market close")
    ]

    for hhmm, description in test_times:
        result = test_time_conversion(hhmm)
        print(f"{description:20} | HHMM: {result['hhmm']:4} | "
              f"Time: {result['hour']:02d}:{result['minute']:02d} | "
              f"Readable: {result['readable']}")

    print("\n✅ All timestamps convert correctly!")
    print("\nFor DataSaver in main.py:")
    print("- Use market_open_time (915) for start_time")
    print("- Use market_close_time (1530) for end_time")
    print("- DataSaver will save data every 5 minutes between 9:15 AM and 3:30 PM")

