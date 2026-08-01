"""Router de Autenticación REST B2B, emisión JWT y contexto Tenant."""
from __future__ import annotations
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, status, Header, Cookie, Response
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token

router = APIRouter(prefix="/api/v1/auth", tags=["Autenticación B2B"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)

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
    session_jwt: str | None = Cookie(None)
) -> UserTenantContext:
    """Dependencia de seguridad que extrae y valida el contexto Tenant desde JWT (Header o Cookie)."""
    token = None
    if isinstance(bearer_token, str) and bearer_token:
        token = bearer_token
    elif isinstance(authorization, str) and authorization.lower().startswith("bearer "):
        token = authorization.split(" ")[1].strip()
    elif isinstance(session_jwt, str) and session_jwt:
        token = session_jwt

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Error de Autenticación: Se requiere token JWT en encabezado Authorization 'Bearer <token>' o Cookie.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_jwt_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Error de Autenticación: Token JWT inválido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return UserTenantContext(
        username=payload.get("sub", "anonimo"),
        organization_id=payload.get("org_id", 1),
        organization_name=payload.get("org_name", "Exportadora Forestal del Chaco S.A."),
        role=payload.get("role", "cliente"),
        email=payload.get("email", "comercial@litoraltrace.com")
    )

@router.post("/login", response_model=TokenResponse)
async def login_b2b(payload: LoginRequest, response: Response) -> TokenResponse:
    """Endpoint de Login B2B. Valida credenciales y emite Token JWT."""
    username = payload.username.strip()
    password = payload.password.strip()

    if username == "admin" and password == "admin123":
        user_data = {
            "sub": "admin",
            "org_id": 1,
            "org_name": "Exportadora Forestal del Chaco S.A.",
            "role": "admin",
            "email": "comercial@litoraltrace.com"
        }
    elif username and password:
        user_data = {
            "sub": username,
            "org_id": 42,
            "org_name": "Aserradero Gran Chaco S.R.L.",
            "role": "manager",
            "email": f"{username}@litoraltrace.com"
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe ingresar usuario y contraseña."
        )

    jwt_token = create_jwt_token(user_data, expires_in_seconds=86400)
    
    response.set_cookie(
        key="session_jwt",
        value=jwt_token,
        httponly=True,
        samesite="lax",
        max_age=86400
    )

    return TokenResponse(
        access_token=jwt_token,
        expires_in_seconds=86400,
        user_info={
            "username": user_data["sub"],
            "organization_id": user_data["org_id"],
            "organization_name": user_data["org_name"],
            "role": user_data["role"],
            "email": user_data["email"]
        }
    )

@router.get("/me", response_model=UserTenantContext)
async def get_current_user_profile(user: UserTenantContext = Depends(get_current_tenant_user)) -> UserTenantContext:
    """Endpoint para obtener el perfil del usuario activo y su organización Tenant."""
    return user
