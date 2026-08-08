"""Migración Inicial PostGIS y Esquema Multi-Tenant B2B.

Revision ID: 001_initial_postgis_schema
Revises: 
Create Date: 2026-08-01 12:00:00.000000
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '001_initial_postgis_schema'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Habilitar extensión PostGIS si es PostgreSQL
    conn = op.get_bind()
    if conn.dialect.name == 'postgresql':
        op.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

    # Tabla Organizations
    op.create_table(
        'organizations',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('slug', sa.String(length=100), nullable=False),
        sa.Column('tax_id', sa.String(length=50), nullable=True),
        sa.Column('tier', sa.String(length=50), server_default='pro', nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_organizations_name', 'organizations', ['name'])
    op.create_index('ix_organizations_slug', 'organizations', ['slug'], unique=True)
    op.create_index('ix_organizations_tax_id', 'organizations', ['tax_id'], unique=True)

    # Tabla Users
    op.create_table(
        'users',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('username', sa.String(length=100), nullable=False),
        sa.Column('password_hash', sa.String(length=255), nullable=False),
        sa.Column('role', sa.String(length=50), server_default='cliente', nullable=False),
        sa.Column('full_name', sa.String(length=255), nullable=True),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('last_login_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_users_organization_id', 'users', ['organization_id'])
    op.create_index('ix_users_email', 'users', ['email'], unique=True)
    op.create_index('ix_users_username', 'users', ['username'], unique=True)

    # Tabla Lotes
    op.create_table(
        'lotes',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('organization_id', sa.Integer(), nullable=False),
        sa.Column('identificador', sa.String(length=100), nullable=False),
        sa.Column('productor_id', sa.String(length=100), nullable=False),
        sa.Column('producto_forestal', sa.String(length=100), nullable=False),
        sa.Column('hectareas', sa.Float(), server_default='0.0', nullable=False),
        sa.Column('latitud', sa.Float(), nullable=False),
        sa.Column('longitud', sa.Float(), nullable=False),
        sa.Column('polygon_wkt', sa.Text(), nullable=True),
        sa.Column('estatus', sa.String(length=50), server_default='Pendiente', nullable=False),
        sa.Column('volumen_ingresado_ton', sa.Float(), server_default='0.0', nullable=True),
        sa.Column('volumen_exportar_ton', sa.Float(), server_default='0.0', nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_lotes_organization_id', 'lotes', ['organization_id'])
    op.create_index('ix_lotes_identificador', 'lotes', ['identificador'])
    op.create_index('ix_lotes_productor_id', 'lotes', ['productor_id'])
    op.create_index('ix_lotes_estatus', 'lotes', ['estatus'])

def downgrade() -> None:
    op.drop_table('lotes')
    op.drop_table('users')
    op.drop_table('organizations')
