from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Integer,
    String,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datetime import datetime

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .instruments import Instrument


class PriceHistoryIntraday(Base, BaseMixin):
    __tablename__ = "price_history_intraday"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "datetime",
            name="uq_price_history_intraday_instrument_datetime",
        ),
    )

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
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    deliver_percentage: Mapped[float | None] = mapped_column(Float, nullable=True)
    resolve_required: Mapped[bool] = mapped_column(
        Boolean, server_default="false", nullable=False
    )
    resolve_tries: Mapped[int] = mapped_column(
        Integer, server_default="0", nullable=False
    )
    interval: Mapped[str | None] = mapped_column(String(32), nullable=True)

    instrument: Mapped["Instrument"] = relationship()
