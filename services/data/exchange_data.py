from datetime import datetime
import pytz


class ExchangeData:
    """
    Represents exchange data configuration for periodic data collection.

    Attributes:
        exchange_name: Name of the exchange (e.g., 'NSE', 'BSE')
        exchange_id: Database ID of the exchange
        market_open_time_hhmm: Market open time in HHMM format (e.g., 915 for 9:15 AM)
        market_close_time_hhmm: Market close time in HHMM format (e.g., 1530 for 3:30 PM)
        timezone_str: Timezone string (e.g., 'Asia/Kolkata')
        interval_minutes: Interval in minutes for data collection (default: 5)
        start_time: Computed start time for data collection (timestamp in milliseconds)
        end_time: Computed end time for data collection (timestamp in milliseconds)
    """

    def __init__(
        self,
        exchange_name: str,
        exchange_id: int,
        market_open_time_hhmm: int,
        market_close_time_hhmm: int,
        timezone_str: str,
        interval_minutes: int = 5,
    ):
        self.exchange_name = exchange_name
        self.exchange_id = exchange_id
        self.market_open_time_hhmm = market_open_time_hhmm
        self.market_close_time_hhmm = market_close_time_hhmm
        self.timezone_str = timezone_str
        self.interval_minutes = interval_minutes

        # Compute start_time and end_time for today
        self.start_time = self._compute_timestamp_for_today(market_open_time_hhmm)
        self.end_time = self._compute_timestamp_for_today(market_close_time_hhmm)

    def _compute_timestamp_for_today(self, hhmm: int) -> int:
        """Compute timestamp in milliseconds for today's given HHMM in the exchange's timezone."""
        tz = pytz.timezone(self.timezone_str)
        now = datetime.now(tz)
        today = now.date()

        hour = hhmm // 100
        minute = hhmm % 100

        dt = tz.localize(datetime.combine(today, datetime.min.time().replace(hour=hour, minute=minute)))
        return int(dt.timestamp() * 1000)

    def get_exchange_info(self):
        return {
            "exchange_name": self.exchange_name,
            "exchange_id": self.exchange_id,
            "market_open_time_hhmm": self.market_open_time_hhmm,
            "market_close_time_hhmm": self.market_close_time_hhmm,
            "timezone": self.timezone_str,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "start_time_readable": datetime.fromtimestamp(self.start_time / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            "end_time_readable": datetime.fromtimestamp(self.end_time / 1000).strftime("%Y-%m-%d %H:%M:%S"),
            "interval_minutes": self.interval_minutes,
        }

    def is_market_open(self, current_time_ms: int) -> bool:
        """Check if the current time is within the market hours."""
        return self.start_time <= current_time_ms <= self.end_time

    def get_remaining_time_ms(self, current_time_ms: int) -> int:
        """Get remaining time until end_time in milliseconds."""
        return max(0, self.end_time - current_time_ms)
