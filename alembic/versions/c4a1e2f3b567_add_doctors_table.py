"""add doctors table

Revision ID: c4a1e2f3b567
Revises: b30f7a99ac35
Create Date: 2026-02-28 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4a1e2f3b567'
down_revision: Union[str, Sequence[str], None] = 'b30f7a99ac35'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add doctors table and clinic_doctors M2M association table."""
    op.create_table(
        "doctors",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("surname", sa.String(256), nullable=True),
        sa.Column("zl_url", sa.String(512), nullable=True),
    )

    op.create_table(
        "clinic_doctors",
        sa.Column(
            "clinic_id",
            sa.Integer,
            sa.ForeignKey("clinics.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "doctor_id",
            sa.Integer,
            sa.ForeignKey("doctors.id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )


def downgrade() -> None:
    """Remove clinic_doctors association table and doctors table."""
    op.drop_table("clinic_doctors")
    op.drop_table("doctors")
