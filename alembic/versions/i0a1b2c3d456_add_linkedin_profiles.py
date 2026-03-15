"""add linkedin_profiles table for storing full Apify profile data

Revision ID: i0a1b2c3d456
Revises: h9f6g7b8c123
Create Date: 2026-03-14 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision: str = "i0a1b2c3d456"
down_revision: Union[str, Sequence[str], None] = "h9f6g7b8c123"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create linkedin_profiles table."""
    op.create_table(
        "linkedin_profiles",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "lead_id",
            sa.Integer(),
            sa.ForeignKey("leads.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("linkedin_url", sa.String(512), nullable=False),
        sa.Column("public_identifier", sa.String(256), nullable=True),
        sa.Column("first_name", sa.String(128), nullable=True),
        sa.Column("last_name", sa.String(128), nullable=True),
        sa.Column("headline", sa.String(1024), nullable=True),
        sa.Column("location_text", sa.String(256), nullable=True),
        sa.Column("country_code", sa.String(8), nullable=True),
        sa.Column("profile_picture_url", sa.String(1024), nullable=True),
        sa.Column("current_company", sa.String(512), nullable=True),
        sa.Column("current_position", sa.String(512), nullable=True),
        sa.Column("connections_count", sa.Integer(), nullable=True),
        sa.Column("review_status", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("search_context", sa.String(16), nullable=True),
        sa.Column("llm_verdict", sa.String(8), nullable=True),
        sa.Column("raw_profile", JSON, nullable=True),
        sa.Column("fetched_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_linkedin_profiles_lead_id", "linkedin_profiles", ["lead_id"])
    op.create_index("ix_linkedin_profiles_review_status", "linkedin_profiles", ["review_status"])


def downgrade() -> None:
    """Drop linkedin_profiles table."""
    op.drop_index("ix_linkedin_profiles_review_status", table_name="linkedin_profiles")
    op.drop_index("ix_linkedin_profiles_lead_id", table_name="linkedin_profiles")
    op.drop_table("linkedin_profiles")
