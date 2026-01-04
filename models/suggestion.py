from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .mixins import BaseMixin


class SuggestionType(Base, BaseMixin):
    __tablename__ = "suggestion_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)

    suggestions: Mapped[list["Suggestion"]] = relationship(back_populates="suggestion_type")


class Suggestion(Base, BaseMixin):
    __tablename__ = "suggestions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    suggestion_type_id: Mapped[int] = mapped_column(ForeignKey("suggestion_types.id"), nullable=False)
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)

    user: Mapped["User"] = relationship("User", backref="suggestions")
    suggestion_type: Mapped["SuggestionType"] = relationship(back_populates="suggestions")

