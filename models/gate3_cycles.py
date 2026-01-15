import uuid
from sqlalchemy import Column, Text, TIMESTAMP, Enum, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, INET
from sqlalchemy.sql import func
from models.base import Base


# ---------- ENUMS ----------
gate3_decision_enum = Enum(
    "APPROVE",
    "HOLD",
    "BLOCK",
    name="gate3_decision_enum",
    create_type=False,
)

gate1_direction_enum = Enum(
    "BUY",
    "SELL",
    name="gate1_direction_enum",
    create_type=False,
)

gate2_status_enum = Enum(
    "SAFE",
    "NOT_SAFE",
    name="gate2_status_enum",
    create_type=False,
)


# ---------- A. GATE-3 CYCLE LOG ----------
class Gate3CycleLog(Base):
    __tablename__ = "gate3_cycle_log"

    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,  # Python-side default
        server_default=func.gen_random_uuid(),  # DB-side default
    )
    trade_id = Column(UUID(as_uuid=True), nullable=False)
    instrument = Column(Text, nullable=False)
    exchange = Column(Text, nullable=False)
    gate1_direction = Column(gate1_direction_enum, nullable=False)
    gate2_status = Column(gate2_status_enum, nullable=False)
    session_id = Column(UUID(as_uuid=True), nullable=False)
    cycle_created_at = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    source_service = Column(Text, nullable=False)
    request_id = Column(UUID(as_uuid=True), nullable=False)


# ---------- B. GATE-3 DECISION LOG ----------
class Gate3DecisionLog(Base):
    __tablename__ = "gate3_decision_log"

    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=func.gen_random_uuid(),
    )
    trade_id = Column(UUID(as_uuid=True), nullable=False, unique=True)
    decision = Column(gate3_decision_enum, nullable=False)
    decision_timestamp = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    decision_reason = Column(Text)
    architect_id = Column(Text, nullable=False)
    auth_method = Column(Text, nullable=False)
    request_id = Column(UUID(as_uuid=True), nullable=False)
    source_ip = Column(INET, nullable=False)


# ---------- C. SECURITY / MISUSE LOG ----------
class Gate3SecurityLog(Base):
    __tablename__ = "gate3_security_log"

    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=func.gen_random_uuid(),
    )
    timestamp = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    trade_id = Column(UUID(as_uuid=True))
    attempted_action = Column(Text, nullable=False)
    failure_reason = Column(Text, nullable=False)
    user_role = Column(Text)
    user_id = Column(Text)
    source_ip = Column(INET, nullable=False)
    endpoint = Column(Text, nullable=False)


# ---------- D. EXECUTION BLOCK LOG ----------
class Gate3ExecutionBlockLog(Base):
    __tablename__ = "gate3_execution_block_log"

    log_id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=func.gen_random_uuid(),
    )
    trade_id = Column(UUID(as_uuid=True), nullable=False)
    gate2_status = Column(gate2_status_enum, nullable=False)
    gate3_decision = Column(gate3_decision_enum, nullable=False)
    block_reason = Column(Text, nullable=False)
    timestamp = Column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    execution_service_id = Column(Text, nullable=False)
