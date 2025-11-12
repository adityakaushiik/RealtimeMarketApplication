from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

if TYPE_CHECKING:
    from .provider import Provider
    from .instruments import Instrument


class ProviderInstrumentMapping(Base):
    __tablename__ = "provider_instrument_mappings"

    provider_id: Mapped[int] = mapped_column(ForeignKey("providers.id"), nullable=False)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), nullable=False)
    provider_instrument_search_code: Mapped[str] = mapped_column(String(255), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("provider_id", "instrument_id", name="pk_provider_instrument_mapping"),
    )

    provider: Mapped["Provider"] = relationship(back_populates="instrument_mappings")
    instrument: Mapped["Instrument"] = relationship(back_populates="provider_mappings")
