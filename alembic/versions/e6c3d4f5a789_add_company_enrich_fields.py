"""add company enrichment fields to clinics and linkedin_candidates table

Revision ID: e6c3d4f5a789
Revises: d5b2a3c4e678
Create Date: 2026-03-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e6c3d4f5a789'
down_revision: Union[str, Sequence[str], None] = 'd5b2a3c4e678'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add website_domain, linkedin_url, search timestamps to clinics; create linkedin_candidates."""
    # New columns on clinics
    op.add_column("clinics", sa.Column("website_domain", sa.String(255), nullable=True))
    op.add_column("clinics", sa.Column("linkedin_url", sa.String(512), nullable=True))
    op.add_column("clinics", sa.Column("domain_searched_at", sa.DateTime(), nullable=True))
    op.add_column("clinics", sa.Column("linkedin_searched_at", sa.DateTime(), nullable=True))

    # LinkedIn candidates table
    op.create_table(
        "linkedin_candidates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("clinic_id", sa.Integer(), sa.ForeignKey("clinics.id", ondelete="CASCADE"), nullable=False),
        sa.Column("url", sa.String(512), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="maybe"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Remove company enrichment fields and linkedin_candidates table."""
    op.drop_table("linkedin_candidates")
    op.drop_column("clinics", "linkedin_searched_at")
    op.drop_column("clinics", "domain_searched_at")
    op.drop_column("clinics", "linkedin_url")
    op.drop_column("clinics", "website_domain")
