from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from .instruments import Instrument
    from .exchange_provider_mapping import ExchangeProviderMapping


class Exchange(Base):
    __tablename__ = "exchanges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    timezone: Mapped[str | None] = mapped_column(String(128), nullable=True)
    country: Mapped[str | None] = mapped_column(String(128), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Minimal relationships
    instruments: Mapped[list["Instrument"]] = relationship(back_populates="exchange")
    provider_mappings: Mapped[list["ExchangeProviderMapping"]] = relationship(
        back_populates="exchange", cascade="all, delete-orphan"
    )
