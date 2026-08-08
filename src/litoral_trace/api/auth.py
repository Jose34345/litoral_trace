"""Router de autenticacion REST B2B, emision JWT y contexto Tenant."""
from __future__ import annotations

from typing import Any

from fastapi import (
    APIRouter,
    Cookie,
    Depends,
    Header,
    HTTPException,
    Response,
    status,
)
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from sqlalchemy import select

from litoral_trace.auth.passwords import verify_password
from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import Organization, User


router = APIRouter(
    prefix="/api/v1/auth",
    tags=["Autenticacion B2B"],
)

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/api/v1/auth/login",
    auto_error=False,
)


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int = 86400
    user_info: dict[str, Any]


class UserTenantContext(BaseModel):
    username: str
    organization_id: int
    organization_name: str
    role: str
    email: str


def get_current_tenant_user(
    authorization: str | None = Header(None),
    bearer_token: str | None = Depends(oauth2_scheme),
    session_jwt: str | None = Cookie(None),
) -> UserTenantContext:
    """Extrae y valida el contexto Tenant desde JWT."""

    token = None

    if isinstance(bearer_token, str) and bearer_token:
        token = bearer_token
    elif isinstance(authorization, str) and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif isinstance(session_jwt, str) and session_jwt:
        token = session_jwt

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Se requiere un token JWT.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_jwt_token(token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token JWT invalido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return UserTenantContext(
        username=str(payload.get("sub", "")),
        organization_id=int(payload.get("org_id", 0)),
        organization_name=str(payload.get("org_name", "")),
        role=str(payload.get("role", "cliente")),
        email=str(payload.get("email", "")),
    )


@router.post("/login", response_model=TokenResponse)
async def login_b2b(
    payload: LoginRequest,
    response: Response,
) -> TokenResponse:
    """Autentica un usuario real contra PostgreSQL y emite JWT."""

    username = payload.username.strip()
    password = payload.password

    if not username or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe ingresar usuario y contrasena.",
        )

    session = get_db_session()

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        user = session.execute(
            select(User).where(User.username == username)
        ).scalar_one_or_none()

        # No revelar si el usuario existe.
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contrasena incorrectos.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="El usuario esta inactivo.",
            )

        if not verify_password(password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contrasena incorrectos.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        organization = session.execute(
            select(Organization).where(
                Organization.id == user.organization_id
            )
        ).scalar_one_or_none()

        if organization is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="La organizacion del usuario no existe.",
            )

        if not organization.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="La organizacion esta inactiva.",
            )

        user_data = {
            "sub": user.username,
            "org_id": user.organization_id,
            "org_name": organization.name,
            "role": user.role,
            "email": user.email,
        }

        jwt_token = create_jwt_token(
            user_data,
            expires_in_seconds=86400,
        )

        response.set_cookie(
            key="session_jwt",
            value=jwt_token,
            httponly=True,
            samesite="lax",
            max_age=86400,
        )

        return TokenResponse(
            access_token=jwt_token,
            expires_in_seconds=86400,
            user_info={
                "username": user.username,
                "organization_id": user.organization_id,
                "organization_name": organization.name,
                "role": user.role,
                "email": user.email,
            },
        )

    finally:
        session.close()


@router.get("/me", response_model=UserTenantContext)
async def get_current_user_profile(
    user: UserTenantContext = Depends(get_current_tenant_user),
) -> UserTenantContext:
    """Obtiene el contexto Tenant del usuario autenticado."""

    return user
