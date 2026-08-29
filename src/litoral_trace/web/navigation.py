"""Navegación hipermedia derivada de RBAC para la interfaz server-rendered."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.auth.rbac import Permission, has_permission


@dataclass(frozen=True)
class NavigationItem:
    key: str
    label: str
    href: str
    section: str
    permission: Permission
    active_prefixes: tuple[str, ...]


@dataclass(frozen=True)
class NavigationView:
    key: str
    label: str
    href: str
    section: str
    active: bool


_NAVIGATION = (
    NavigationItem(
        key="dashboard",
        label="Inicio",
        href="/dashboard",
        section="operacion",
        permission=Permission.LOTE_READ,
        active_prefixes=("/dashboard",),
    ),
    NavigationItem(
        key="pilot_readiness",
        label="Preparar piloto",
        href="/pilot-readiness",
        section="operacion",
        permission=Permission.LOTE_READ,
        active_prefixes=("/pilot-readiness",),
    ),
    NavigationItem(
        key="operations",
        label="Operaciones",
        href="/operations",
        section="operacion",
        permission=Permission.TRACEABILITY_OPERATE,
        active_prefixes=("/operations",),
    ),
    NavigationItem(
        key="assurance_workspace",
        label="Agregar documentos",
        href="/api/v1/assurance/workspace",
        section="operacion",
        permission=Permission.VAULT_UPLOAD,
        active_prefixes=("/api/v1/assurance/workspace",),
    ),
    NavigationItem(
        key="attention",
        label="Requiere atención",
        href="/api/v1/assurance/attention",
        section="operacion",
        permission=Permission.TRACEABILITY_OPERATE,
        active_prefixes=("/api/v1/assurance/attention",),
    ),
    NavigationItem(
        key="imports",
        label="Carga masiva",
        href="/imports",
        section="operacion",
        permission=Permission.LOTE_CREATE,
        active_prefixes=("/imports",),
    ),
    NavigationItem(
        key="traceability",
        label="Trazabilidad",
        href="/traceability",
        section="compliance",
        permission=Permission.LOTE_READ,
        active_prefixes=("/traceability",),
    ),
    NavigationItem(
        key="release_control",
        label="Control de salida",
        href="/release-control",
        section="compliance",
        permission=Permission.LOTE_READ,
        active_prefixes=("/release-control",),
    ),
    NavigationItem(
        key="evidence",
        label="Evidencias",
        href="/evidence",
        section="compliance",
        permission=Permission.VAULT_READ,
        active_prefixes=("/evidence", "/vault"),
    ),
    NavigationItem(
        key="integrations",
        label="Integraciones",
        href="/integrations",
        section="administracion",
        permission=Permission.INTEGRATION_READ,
        active_prefixes=("/integrations",),
    ),
    NavigationItem(
        key="settings",
        label="Configuración",
        href="/settings",
        section="administracion",
        permission=Permission.SETTINGS_WRITE,
        active_prefixes=("/settings",),
    ),
    NavigationItem(
        key="platform",
        label="Plataforma",
        href="/admin",
        section="administracion",
        permission=Permission.PLATFORM_ADMIN,
        active_prefixes=("/admin",),
    ),
)


def _is_active(path: str, prefixes: tuple[str, ...]) -> bool:
    normalized = path or "/"
    return any(
        normalized == prefix or normalized.startswith(f"{prefix}/")
        for prefix in prefixes
    )


def build_navigation(
    user: Any,
    *,
    current_path: str,
) -> tuple[NavigationView, ...]:
    """Devuelve sólo las opciones que el rol autenticado puede utilizar."""

    visible: list[NavigationView] = []
    role = str(getattr(user, "role", "") or "").strip().lower()
    assurance_flags = get_assurance_feature_flags()

    for item in _NAVIGATION:
        if not has_permission(user, item.permission):
            continue

        if item.key == "pilot_readiness" and role not in {"admin", "manager"}:
            continue

        if item.key == "assurance_workspace" and not (
            assurance_flags.assurance_v1 and assurance_flags.document_intelligence
        ):
            continue

        if item.key == "attention" and not (
            assurance_flags.assurance_v1 and assurance_flags.operational_exceptions
        ):
            continue

        visible.append(
            NavigationView(
                key=item.key,
                label=item.label,
                href=item.href,
                section=item.section,
                active=_is_active(current_path, item.active_prefixes),
            )
        )

    return tuple(visible)
