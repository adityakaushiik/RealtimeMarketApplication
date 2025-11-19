from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .provider import Provider
    from .exchange import Exchange


class ExchangeProviderMapping(Base, BaseMixin):
    __tablename__ = "exchange_provider_mappings"

    provider_id: Mapped[int] = mapped_column(ForeignKey("providers.id"), nullable=False)
    exchange_id: Mapped[int] = mapped_column(ForeignKey("exchanges.id"), nullable=False)

    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="true"
    )
    is_primary: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )

    __table_args__ = (
        PrimaryKeyConstraint(
            "provider_id", "exchange_id", name="pk_exchange_provider_mapping"
        ),
    )

    provider: Mapped["Provider"] = relationship(back_populates="exchange_mappings")
    exchange: Mapped["Exchange"] = relationship(back_populates="provider_mappings")
