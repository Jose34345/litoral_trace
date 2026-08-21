"""Navegación hipermedia derivada de RBAC para la interfaz server-rendered."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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
        key="vault",
        label="Documentos y evidencias",
        href="/vault",
        section="compliance",
        permission=Permission.VAULT_READ,
        active_prefixes=("/vault",),
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
        label="Administración de plataforma",
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

    for item in _NAVIGATION:
        if not has_permission(user, item.permission):
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
