from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Float, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from datetime import datetime

from .base import Base
from .mixins import BaseMixin


class PriceHistoryIntraday(Base, BaseMixin):
    __tablename__ = "price_history_intraday"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey("instruments.id"), index=True, nullable=False
    )

    datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[float | None] = mapped_column(Float, nullable=True)
    high: Mapped[float | None] = mapped_column(Float, nullable=True)
    low: Mapped[float | None] = mapped_column(Float, nullable=True)
    close: Mapped[float | None] = mapped_column(Float, nullable=True)
    previous_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    adj_close: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deliver_percentage: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_not_found: Mapped[bool] = mapped_column(
        Boolean, server_default="false", nullable=False
    )
    interval: Mapped[str | None] = mapped_column(String(32), nullable=True)

