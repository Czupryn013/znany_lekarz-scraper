"""add social urls to clinic_locations

Revision ID: d5b2a3c4e678
Revises: c4a1e2f3b567
Create Date: 2026-02-28 22:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5b2a3c4e678'
down_revision: Union[str, Sequence[str], None] = 'c4a1e2f3b567'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add social/website URL columns to clinic_locations."""
    op.add_column("clinic_locations", sa.Column("facebook_url", sa.String(512), nullable=True))
    op.add_column("clinic_locations", sa.Column("instagram_url", sa.String(512), nullable=True))
    op.add_column("clinic_locations", sa.Column("youtube_url", sa.String(512), nullable=True))
    op.add_column("clinic_locations", sa.Column("linkedin_url", sa.String(512), nullable=True))
    op.add_column("clinic_locations", sa.Column("website_url", sa.String(512), nullable=True))


def downgrade() -> None:
    """Remove social/website URL columns from clinic_locations."""
    op.drop_column("clinic_locations", "website_url")
    op.drop_column("clinic_locations", "linkedin_url")
    op.drop_column("clinic_locations", "youtube_url")
    op.drop_column("clinic_locations", "instagram_url")
    op.drop_column("clinic_locations", "facebook_url")
