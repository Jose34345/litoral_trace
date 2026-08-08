"""Añadir tabla satellite_ndvi_observations para histórico satelital.

Revision ID: 002_add_satellite_ndvi_table
Revises: 001_initial_postgis_schema
Create Date: 2026-08-01 13:00:00.000000
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '002_add_satellite_ndvi_table'
down_revision: Union[str, Sequence[str], None] = '001_initial_postgis_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_table(
        'satellite_ndvi_observations',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('lote_id', sa.Integer(), nullable=False),
        sa.Column('observation_date', sa.Date(), nullable=False),
        sa.Column('ndvi_mean', sa.Float(), nullable=False),
        sa.Column('ndvi_min', sa.Float(), nullable=True),
        sa.Column('ndvi_max', sa.Float(), nullable=True),
        sa.Column('ndvi_std', sa.Float(), nullable=True),
        sa.Column('cloud_percentage', sa.Float(), server_default='0.0', nullable=False),
        sa.Column('valid_pixel_count', sa.Integer(), nullable=True),
        sa.Column('valid_pixel_percentage', sa.Float(), nullable=True),
        sa.Column('satellite', sa.String(length=50), server_default='Sentinel-2', nullable=False),
        sa.Column('collection', sa.String(length=100), server_default='COPERNICUS/S2_SR_HARMONIZED', nullable=False),
        sa.Column('geometry_hash', sa.String(length=64), nullable=False),
        sa.Column('algorithm_version', sa.String(length=50), nullable=False),
        sa.Column('processing_date', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['lote_id'], ['lotes.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organization_id', 'lote_id', 'observation_date', 'geometry_hash', name='uq_satellite_obs_tenant_lote_date_hash')
    )
    op.create_index('ix_sat_obs_organization_id', 'satellite_ndvi_observations', ['organization_id'])
    op.create_index('ix_sat_obs_lote_id', 'satellite_ndvi_observations', ['lote_id'])
    op.create_index('ix_sat_obs_observation_date', 'satellite_ndvi_observations', ['observation_date'])
    op.create_index('ix_sat_obs_geometry_hash', 'satellite_ndvi_observations', ['geometry_hash'])
    op.create_index('ix_sat_obs_tenant_lote_date', 'satellite_ndvi_observations', ['organization_id', 'lote_id', 'observation_date'])

def downgrade() -> None:
    op.drop_table('satellite_ndvi_observations')
