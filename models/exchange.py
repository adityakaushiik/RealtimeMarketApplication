from __future__ import annotations

from datetime import time
from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, Time
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .instruments import Instrument
    from .exchange_provider_mapping import ExchangeProviderMapping


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
