"""add nip_searched_at to clinics

Revision ID: a8e5f6b7c901
Revises: f7d4e5a6b890
Create Date: 2026-03-06 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a8e5f6b7c901'
down_revision: Union[str, Sequence[str], None] = 'f7d4e5a6b890'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add nip_searched_at timestamp to clinics table."""
    op.add_column("clinics", sa.Column("nip_searched_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Remove nip_searched_at column from clinics."""
    op.drop_column("clinics", "nip_searched_at")
