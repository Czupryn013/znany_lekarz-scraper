"""add leads table and lead_clinic_roles junction table

Revision ID: h9f6g7b8c123
Revises: g8e5f6a7b012
Create Date: 2026-03-08 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "h9f6g7b8c123"
down_revision: Union[str, Sequence[str], None] = "g8e5f6a7b012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create leads table and lead_clinic_roles M2M junction table."""
    op.create_table(
        "leads",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("pesel", sa.String(11), nullable=True, unique=True),
        sa.Column("full_name", sa.String(256), nullable=False),
        sa.Column("phone", sa.String(64), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("linkedin_url", sa.String(512), nullable=True),
        sa.Column("lead_source", sa.String(32), nullable=False),
        sa.Column("phone_source", sa.String(32), nullable=True),
        sa.Column("enrichment_status", sa.String(32), nullable=False, server_default="PENDING"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=True, onupdate=sa.func.now()),
    )
    op.create_index("ix_leads_pesel", "leads", ["pesel"], unique=True)
    op.create_index("ix_leads_enrichment_status", "leads", ["enrichment_status"])

    op.create_table(
        "lead_clinic_roles",
        sa.Column(
            "lead_id",
            sa.Integer(),
            sa.ForeignKey("leads.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "clinic_id",
            sa.Integer(),
            sa.ForeignKey("clinics.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String(128), nullable=False),
        sa.PrimaryKeyConstraint("lead_id", "clinic_id", "role"),
    )


def downgrade() -> None:
    """Drop lead_clinic_roles and leads tables."""
    op.drop_table("lead_clinic_roles")
    op.drop_index("ix_leads_enrichment_status", table_name="leads")
    op.drop_index("ix_leads_pesel", table_name="leads")
    op.drop_table("leads")
