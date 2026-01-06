from sqlalchemy import (
    String,
    Enum,
    DateTime,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from models.base import Base
import uuid
import enum


class Gate1Direction(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class Gate2Status(str, enum.Enum):
    SAFE = "SAFE"
    NOT_SAFE = "NOT_SAFE"


class Gate3Decision(str, enum.Enum):
    APPROVE = "APPROVE"
    HOLD = "HOLD"
    BLOCK = "BLOCK"


class Gate3Cycle(Base):
    __tablename__ = "gate3_cycles"

    trade_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    instrument: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    exchange: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
    )

    gate1_direction: Mapped[Gate1Direction] = mapped_column(
        Enum(Gate1Direction, name="gate1_direction_enum", create_type=False),
        nullable=False,
    )

    gate2_status: Mapped[Gate2Status] = mapped_column(
        Enum(Gate2Status, name="gate2_status_enum", create_type=False),
        nullable=False,
    )

    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    cycle_timestamp: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    gate3_decision: Mapped[Gate3Decision] = mapped_column(
        Enum(Gate3Decision, name="gate3_decision_enum", create_type=False),
        nullable=False,
        server_default=Gate3Decision.HOLD.value,
    )

    gate3_decision_timestamp: Mapped[DateTime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    decision_reason: Mapped[str | None] = mapped_column(
        String(160),  # Task 2.1: max 160 chars
        nullable=True,
    )


class Gate3Logs(Base):
    __tablename__ = "gate3_logs"

    log_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    trade_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    log_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    message: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
    )

    timestamp: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
