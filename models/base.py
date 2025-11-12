from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase, declared_attr

# # Optional naming convention to help with migrations and constraint names.
# metadata = MetaData(
#     naming_convention={
#         "ix": "ix_%(column_0_label)s",
#         "uq": "uq_%(table_name)s_%(column_0_name)s",
#         "ck": "ck_%(table_name)s_%(constraint_name)s",
#         "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
#         "pk": "pk_%(table_name)s",
#     }
# )


class Base(DeclarativeBase):
    # Automatically generate __tablename__ if not provided (optional convenience)
    @declared_attr.directive
    def __tablename__(cls) -> str:  # type: ignore[override]
        return cls.__name__.lower()

__all__ = ["Base"]
