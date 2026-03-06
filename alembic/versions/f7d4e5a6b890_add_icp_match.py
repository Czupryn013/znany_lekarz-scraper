"""add icp_match flag to clinics

Revision ID: f7d4e5a6b890
Revises: e6c3d4f5a789
Create Date: 2026-03-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f7d4e5a6b890'
down_revision: Union[str, Sequence[str], None] = 'e6c3d4f5a789'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add icp_match boolean flag to clinics table."""
    op.add_column("clinics", sa.Column("icp_match", sa.Boolean(), nullable=False, server_default=sa.text("false")))


def downgrade() -> None:
    """Remove icp_match column from clinics."""
    op.drop_column("clinics", "icp_match")
