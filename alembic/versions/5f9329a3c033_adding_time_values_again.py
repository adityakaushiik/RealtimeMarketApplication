"""Adding Time Values again

Revision ID: 5f9329a3c033
Revises: 47dc884c150a
Create Date: 2025-12-01 00:23:48.095018

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5f9329a3c033"
down_revision: Union[str, Sequence[str], None] = "47dc884c150a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Fix the four columns in the correct order with explicit USING clause
def upgrade():
    op.alter_column(
        "exchanges",
        "pre_market_open_time",
        existing_type=sa.Text(),  # or sa.String(20), whatever it is now
        type_=sa.Time(),
        postgresql_using="pre_market_open_time::time",
        existing_nullable=True,
    )

    op.alter_column(
        "exchanges",
        "market_open_time",
        existing_type=sa.Text(),
        type_=sa.Time(),
        postgresql_using="market_open_time::time",
        existing_nullable=True,
    )

    op.alter_column(
        "exchanges",
        "market_close_time",
        existing_type=sa.Text(),
        type_=sa.Time(),
        postgresql_using="market_close_time::time",
        existing_nullable=True,
    )

    op.alter_column(
        "exchanges",
        "post_market_close_time",
        existing_type=sa.Text(),
        type_=sa.Time(),
        postgresql_using="post_market_close_time::time",
        existing_nullable=True,
    )

    # Now that they are real TIME columns, make the regular session non-nullable
    op.alter_column(
        "exchanges",
        "market_open_time",
        nullable=False,
        server_default=sa.text("'09:30:00'::time"),
    )
    op.alter_column(
        "exchanges",
        "market_close_time",
        nullable=False,
        server_default=sa.text("'16:00:00'::time"),
    )


def downgrade():
    # Reverse everything (turn back into text if you ever need to)
    op.alter_column("exchanges", "market_close_time", type_=sa.Text(), nullable=True)
    op.alter_column("exchanges", "market_open_time", type_=sa.Text(), nullable=True)
    op.alter_column(
        "exchanges", "post_market_close_time", type_=sa.Text(), nullable=True
    )
    op.alter_column("exchanges", "pre_market_open_time", type_=sa.Text(), nullable=True)
