"""initial schema

Revision ID: b30f7a99ac35
Revises: 
Create Date: 2026-02-20 23:09:59.638731

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b30f7a99ac35'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Specializations
    op.create_table(
        "specializations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("name", sa.String(255), nullable=False),
    )

    # Clinics
    op.create_table(
        "clinics",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("zl_url", sa.String(512), nullable=False, unique=True, index=True),
        sa.Column("name", sa.String(512), nullable=True),
        sa.Column("zl_profile_id", sa.String(64), nullable=True),
        sa.Column("nip", sa.String(32), nullable=True),
        sa.Column("legal_name", sa.String(512), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("zl_reviews_cnt", sa.Integer, nullable=True),
        sa.Column("doctors_count", sa.Integer, nullable=True),
        sa.Column("discovered_at", sa.DateTime, nullable=False),
        sa.Column("enriched_at", sa.DateTime, nullable=True),
    )

    # Clinic locations
    op.create_table(
        "clinic_locations",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "clinic_id",
            sa.Integer,
            sa.ForeignKey("clinics.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("address", sa.String(512), nullable=True),
        sa.Column("latitude", sa.Float, nullable=True),
        sa.Column("longitude", sa.Float, nullable=True),
    )

    # Search queries (clinic ↔ specialization)
    op.create_table(
        "search_queries",
        sa.Column(
            "clinic_id",
            sa.Integer,
            sa.ForeignKey("clinics.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "specialization_id",
            sa.Integer,
            sa.ForeignKey("specializations.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("discovered_at", sa.DateTime, nullable=False),
        sa.UniqueConstraint("clinic_id", "specialization_id", name="uq_clinic_specialization"),
    )

    # Scrape progress (checkpoint per specialization)
    op.create_table(
        "scrape_progress",
        sa.Column(
            "specialization_id",
            sa.Integer,
            sa.ForeignKey("specializations.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("last_page_scraped", sa.Integer, nullable=False, server_default="0"),
        sa.Column("total_pages", sa.Integer, nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("updated_at", sa.DateTime, nullable=False),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("scrape_progress")
    op.drop_table("search_queries")
    op.drop_table("clinic_locations")
    op.drop_table("clinics")
    op.drop_table("specializations")
