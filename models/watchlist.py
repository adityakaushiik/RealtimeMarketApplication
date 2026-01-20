from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .user import User
    from .instruments import Instrument
    from .exchange import Exchange


class Watchlist(Base, BaseMixin):
    __tablename__ = "watchlists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False
    )
    exchange_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("exchanges.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    show_on_dashboard: Mapped[bool] = mapped_column(nullable=True, default=False)

    user: Mapped["User"] = relationship(back_populates="watchlists")
    exchange: Mapped["Exchange"] = relationship(back_populates="watchlist")
    items: Mapped[list["WatchlistItem"]] = relationship(
        back_populates="watchlist", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_user_watchlist_name"),
    )


class WatchlistItem(Base, BaseMixin):
    __tablename__ = "watchlist_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    watchlist_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("watchlists.id"), nullable=False
    )
    instrument_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("instruments.id"), nullable=False
    )

    watchlist: Mapped["Watchlist"] = relationship(back_populates="items")
    instrument: Mapped["Instrument"] = relationship()

    __table_args__ = (
        UniqueConstraint(
            "watchlist_id", "instrument_id", name="uq_watchlist_instrument"
        ),
    )
