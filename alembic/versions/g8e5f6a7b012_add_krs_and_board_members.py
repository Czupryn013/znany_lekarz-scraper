"""add krs columns to clinics and board_members table

Revision ID: g8e5f6a7b012
Revises: a8e5f6b7c901
Create Date: 2026-03-07 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "g8e5f6a7b012"
down_revision: Union[str, Sequence[str], None] = "a8e5f6b7c901"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add KRS/CEIDG columns to clinics and create board_members table."""
    # -- clinics: new registry columns --
    op.add_column("clinics", sa.Column("krs_number", sa.String(32), nullable=True))
    op.add_column("clinics", sa.Column("regon", sa.String(32), nullable=True))
    op.add_column("clinics", sa.Column("registration_date", sa.String(32), nullable=True))
    op.add_column("clinics", sa.Column("legal_type", sa.String(16), nullable=True))
    op.add_column("clinics", sa.Column("krs_searched_at", sa.DateTime(), nullable=True))

    # -- board_members table --
    op.create_table(
        "board_members",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "clinic_id",
            sa.Integer(),
            sa.ForeignKey("clinics.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("full_name", sa.String(256), nullable=False),
        sa.Column("pesel", sa.String(11), nullable=True),
        sa.Column("role", sa.String(128), nullable=True),
        sa.Column("source", sa.String(16), nullable=False),
    )


def downgrade() -> None:
    """Remove board_members table and KRS columns from clinics."""
    op.drop_table("board_members")
    op.drop_column("clinics", "krs_searched_at")
    op.drop_column("clinics", "legal_type")
    op.drop_column("clinics", "registration_date")
    op.drop_column("clinics", "regon")
    op.drop_column("clinics", "krs_number")
