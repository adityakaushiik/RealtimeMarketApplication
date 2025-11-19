from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin

if TYPE_CHECKING:
    from .instruments import Instrument


class InstrumentType(Base, BaseMixin):
    __tablename__ = "instrument_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    category: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_order: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Required minimal relationship: instruments referencing this type
    instruments: Mapped[list["Instrument"]] = relationship(
        back_populates="instrument_type", cascade="all, delete-orphan"
    )
