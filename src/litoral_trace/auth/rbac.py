"""Control de Accesos Basado en Roles (RBAC) y jerarquía de permisos B2B."""
from __future__ import annotations
import functools
from typing import Callable, Any

# Jerarquía numérico-jerárquica de permisos
ROLE_LEVELS: dict[str, int] = {
    "admin": 100,       # Acceso total, gestión de tenants, licencias y llaves API
    "manager": 75,      # Gestión de lotes, auditorías, cargas masivas
    "auditor": 50,      # Lectura de auditorías, emisión de certificados DDS/PDF
    "cliente": 25,      # Visualización básica de dashboard y lotes asignados
    "guest": 0          # Sin permisos de modificación
}

def get_role_level(role: str) -> int:
    """Devuelve el nivel numérico de un rol en la jerarquía."""
    return ROLE_LEVELS.get(role.lower().strip(), 0)

def has_permission(user_role: str, required_role: str) -> bool:
    """Verifica si el rol del usuario posee un nivel igual o superior al rol requerido."""
    return get_role_level(user_role) >= get_role_level(required_role)

def require_role(required_role: str = "cliente") -> Callable:
    """Decorador de seguridad para restringir la ejecución de vistas/funciones en Streamlit.
    
    Args:
        required_role: Rol mínimo necesario para ejecutar la función.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                import streamlit as st
                current_role = st.session_state.get("rol", "guest")
                if not has_permission(current_role, required_role):
                    st.error(f"Acceso Denegado: Su rol ('{current_role}') no posee privilegios para ejecutar esta acción (requiere '{required_role}').")
                    st.stop()
            except ImportError:
                pass
            return func(*args, **kwargs)
        return wrapper
    return decorator
