"""add doctor details columns, doctor_specializations M2M, clinic_doctors extra columns

Revision ID: j1b2c3d4e567
Revises: ec7df11e0b31
Create Date: 2026-03-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'j1b2c3d4e567'
down_revision: Union[str, None] = 'ec7df11e0b31'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # New columns on doctors table
    op.add_column('doctors', sa.Column('gender', sa.SmallInteger(), nullable=True))
    op.add_column('doctors', sa.Column('img_url', sa.String(length=1024), nullable=True))
    op.add_column('doctors', sa.Column('opinions_positive', sa.Integer(), nullable=True))
    op.add_column('doctors', sa.Column('opinions_neutral', sa.Integer(), nullable=True))
    op.add_column('doctors', sa.Column('opinions_negative', sa.Integer(), nullable=True))

    # New columns on clinic_doctors M2M
    op.add_column('clinic_doctors', sa.Column('booking_ratio', sa.Float(), nullable=True))
    op.add_column('clinic_doctors', sa.Column('is_bookable', sa.Boolean(), nullable=True))

    # Checkpoint column on clinics for refetch tracking
    op.add_column('clinics', sa.Column('doctors_refetched_at', sa.DateTime(), nullable=True))

    # New doctor_specializations M2M table
    op.create_table(
        'doctor_specializations',
        sa.Column('doctor_id', sa.Integer(), sa.ForeignKey('doctors.id', ondelete='CASCADE'), primary_key=True),
        sa.Column('specialization_id', sa.Integer(), sa.ForeignKey('specializations.id', ondelete='CASCADE'), primary_key=True),
        sa.Column('is_in_progress', sa.Boolean(), nullable=False, server_default=sa.text('false')),
    )


def downgrade() -> None:
    op.drop_table('doctor_specializations')

    op.drop_column('clinics', 'doctors_refetched_at')

    op.drop_column('clinic_doctors', 'is_bookable')
    op.drop_column('clinic_doctors', 'booking_ratio')

    op.drop_column('doctors', 'opinions_negative')
    op.drop_column('doctors', 'opinions_neutral')
    op.drop_column('doctors', 'opinions_positive')
    op.drop_column('doctors', 'img_url')
    op.drop_column('doctors', 'gender')
