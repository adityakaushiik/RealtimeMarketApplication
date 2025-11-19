from __future__ import annotations

from typing import Any, TYPE_CHECKING

from sqlalchemy import Integer, String, JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .exchange_provider_mapping import ExchangeProviderMapping
    from .provider_instrument_mapping import ProviderInstrumentMapping


class Provider(Base, BaseMixin):

    __tablename__ = "providers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    credentials: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    rate_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)

    exchange_mappings: Mapped[list["ExchangeProviderMapping"]] = relationship(
        back_populates="provider", cascade="all, delete-orphan"
    )
    instrument_mappings: Mapped[list["ProviderInstrumentMapping"]] = relationship(
        back_populates="provider", cascade="all, delete-orphan"
    )
