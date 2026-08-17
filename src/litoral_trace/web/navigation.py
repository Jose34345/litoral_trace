"""RBAC-derived hypermedia navigation for the server-rendered UI."""
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
        label="Dashboard",
        href="/dashboard",
        section="operacion",
        permission=Permission.LOTE_READ,
        active_prefixes=("/dashboard",),
    ),
    NavigationItem(
        key="imports",
        label="Importaciones",
        href="/imports",
        section="operacion",
        permission=Permission.LOTE_CREATE,
        active_prefixes=("/imports",),
    ),
    NavigationItem(
        key="vault",
        label="Vault / Evidencias",
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
    """Return only transitions the authenticated role may actually use."""

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
