"""merge_heads_fix

Revision ID: c4f7a909770e
Revises: a1b2c3d4e5f6, c3d8217717eb
Create Date: 2026-01-06 13:36:49.108310

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4f7a909770e"
down_revision: Union[str, Sequence[str], None] = ("a1b2c3d4e5f6", "c3d8217717eb")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
