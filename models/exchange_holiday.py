from __future__ import annotations

from datetime import date, time
from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, Date, Boolean, Time, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .exchange import Exchange


class ExchangeHoliday(Base, BaseMixin):
    __tablename__ = "exchange_holidays"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    exchange_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("exchanges.id"), nullable=False
    )
    date: Mapped[date] = mapped_column(Date, nullable=False)
    description: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # If true, the market is fully closed (Holiday)
    is_closed: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # If is_closed is False, these optional times override the default exchange times
    # Useful for half-days or special sessions (Muhurat trading)
    open_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    close_time: Mapped[time | None] = mapped_column(Time, nullable=True)

    exchange: Mapped["Exchange"] = relationship(back_populates="holidays")
