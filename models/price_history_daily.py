from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Date, Float, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from .instruments import Instrument


class PriceHistoryDaily(Base):
    __tablename__ = "price_history_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), index=True, nullable=False)

    date: Mapped[date] = mapped_column(Date, nullable=False)
    open: Mapped[float | None] = mapped_column(Float, nullable=True)
    high: Mapped[float | None] = mapped_column(Float, nullable=True)
    low: Mapped[float | None] = mapped_column(Float, nullable=True)
    close: Mapped[float | None] = mapped_column(Float, nullable=True)
    previous_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    adj_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deliver_percentage: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_not_found: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    dividend: Mapped[float | None] = mapped_column(Float, nullable=True)
    split: Mapped[float | None] = mapped_column(Float, nullable=True)
    split_adjusted: Mapped[float | None] = mapped_column(Float, nullable=True)

    instrument: Mapped["Instrument"] = relationship(back_populates="daily_prices")
