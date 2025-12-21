from __future__ import annotations

from datetime import time, datetime, timezone
from typing import TYPE_CHECKING
import pytz

from sqlalchemy import Integer, String, Time
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .instruments import Instrument
    from .exchange_provider_mapping import ExchangeProviderMapping
    from .exchange_holiday import ExchangeHoliday


class Exchange(Base, BaseMixin):
    __tablename__ = "exchanges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    timezone: Mapped[str | None] = mapped_column(String(128), nullable=True)
    country: Mapped[str | None] = mapped_column(String(128), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Start Time and End Time in 24-hour HHMM (int) format
    pre_market_open_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    market_open_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    market_close_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    post_market_close_time: Mapped[time | None] = mapped_column(Time, nullable=True)

    is_open_24_hours: Mapped[bool] = mapped_column(
        Integer, nullable=False, server_default="0"
    )

    # Minimal relationships
    instruments: Mapped[list["Instrument"]] = relationship(back_populates="exchange")
    provider_mappings: Mapped[list["ExchangeProviderMapping"]] = relationship(
        back_populates="exchange", cascade="all, delete-orphan"
    )
    holidays: Mapped[list["ExchangeHoliday"]] = relationship(
        back_populates="exchange", cascade="all, delete-orphan"
    )

    # Transient attributes for runtime logic
    start_time: int = 0
    end_time: int = 0
    interval_minutes: int = 5

    def update_timestamps_for_date(self, date_obj) -> None:
        """Update start_time and end_time for a specific date."""
        # Use strictly market open and close times, ignoring pre/post market sessions
        if not self.market_open_time or not self.market_close_time:
             # Fallback or handle error if times are missing, though they should be present
             return

        effective_start_time = self.market_open_time
        effective_end_time = self.market_close_time

        # Convert to timestamp (ms)
        tz = pytz.timezone(self.timezone)
        start_dt = tz.localize(datetime.combine(date_obj, effective_start_time))
        end_dt = tz.localize(datetime.combine(date_obj, effective_end_time))

        self.start_time = int(start_dt.timestamp() * 1000)
        self.end_time = int(end_dt.timestamp() * 1000)

    def _compute_timestamp(self, date_obj, time_obj: time) -> int:
        """Compute timestamp in milliseconds for a given date and time in the exchange's timezone."""
        if time_obj is None:
            return 0
        tz = pytz.timezone(self.timezone)
        dt = tz.localize(datetime.combine(date_obj, time_obj))
        return int(dt.timestamp() * 1000)

    def get_exchange_info(self):
        return {
            "exchange_name": self.name,
            "exchange_id": self.id,
            "market_open_time": self.market_open_time.strftime("%H:%M:%S") if self.market_open_time else None,
            "market_close_time": self.market_close_time.strftime("%H:%M:%S") if self.market_close_time else None,
            "pre_market_open_time": self.pre_market_open_time.strftime("%H:%M:%S")
            if self.pre_market_open_time
            else None,
            "post_market_close_time": self.post_market_close_time.strftime("%H:%M:%S")
            if self.post_market_close_time
            else None,
            "timezone": self.timezone,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "start_time_readable": datetime.fromtimestamp(
                self.start_time / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
            "end_time_readable": datetime.fromtimestamp(self.end_time / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "interval_minutes": self.interval_minutes,
        }

    def is_market_open(self, current_time_ms: int) -> bool:
        """Check if the current time is within the market hours."""
        return self.start_time <= current_time_ms <= self.end_time

    def get_remaining_time_ms(self, current_time_ms: int) -> int:
        """Get remaining time until end_time in milliseconds."""
        return max(0, self.end_time - current_time_ms)
