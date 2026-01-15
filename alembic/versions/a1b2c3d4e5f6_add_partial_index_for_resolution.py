"""Add partial index for resolution queries

Revision ID: a1b2c3d4e5f6
Revises: 3ea37a345e55
Create Date: 2026-01-05 10:00:00.000000

"""

from typing import Sequence, Union
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "3ea37a345e55"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Partial index for intraday records needing resolution
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_intraday_needs_resolution 
        ON price_history_intraday (instrument_id, datetime) 
        WHERE resolve_required = TRUE AND resolve_tries < 3
    """)

    # Partial index for daily records needing resolution
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_daily_needs_resolution 
        ON price_history_daily (instrument_id, datetime) 
        WHERE resolve_required = TRUE AND resolve_tries < 3
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_intraday_needs_resolution")
    op.execute("DROP INDEX IF EXISTS ix_daily_needs_resolution")
