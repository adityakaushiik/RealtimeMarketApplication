from __future__ import annotations

from sqlalchemy import Boolean, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from . import Sector, InstrumentType, Exchange, PriceHistoryIntraday, PriceHistoryDaily, ProviderInstrumentMapping
from .base import Base
from .mixins import BaseMixin


class Instrument(Base, BaseMixin):
    __tablename__ = "instruments"

    ## mapped_column is used now instead of Column
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id"), nullable=False, index=True)
    blacklisted: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False)
    delisted: Mapped[bool] = mapped_column(Boolean, server_default="false", nullable=False)

    instrument_type_id: Mapped[int] = mapped_column(
        ForeignKey("instrument_types.id"), nullable=False, index=True
    )
    sector_id: Mapped[int | None] = mapped_column(
        ForeignKey("sectors.id"), nullable=True, index=True
    )

    # Minimal relationships
    exchange: Mapped["Exchange"] = relationship(back_populates="instruments")
    instrument_type: Mapped["InstrumentType"] = relationship(back_populates="instruments")
    sector: Mapped["Sector"] | None = relationship(back_populates="instruments")

    intraday_prices: Mapped[list["PriceHistoryIntraday"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )
    daily_prices: Mapped[list["PriceHistoryDaily"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )

    provider_mappings: Mapped[list["ProviderInstrumentMapping"]] = relationship(
        back_populates="instrument", cascade="all, delete-orphan"
    )
