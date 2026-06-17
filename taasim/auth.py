from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError

from .config import AppConfig

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


def create_access_token(data: Dict[str, Any], config: AppConfig, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=60))
    to_encode.update({"exp": expire, "sub": data.get("username")})
    token = jwt.encode(to_encode, config.jwt_secret, algorithm="HS256")
    return token


async def verify_token(request: Request, authorization: Optional[str] = None) -> Dict[str, Any] | None:
    """Verify Authorization header Bearer token when auth is enabled.

    Returns decoded token payload dict or None when auth disabled.
    """
    config = request.app.state.config
    if not config.auth_enabled:
        return None

    auth_header = authorization or request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid Authorization header", headers={"WWW-Authenticate": "Bearer"})
    token = auth_header.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, config.jwt_secret, algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"})


async def token_endpoint(form_data: OAuth2PasswordRequestForm = Depends(), request: Request | None = None) -> Dict[str, str]:
    """Simple token endpoint for demo purposes.

    Accepts username in `username` and role in `password` (convenience).
    In production replace with proper user store and password checking.
    """
    config = (request.app.state.config if request is not None and hasattr(request.app.state, "config") else AppConfig())
    username = form_data.username
    # For demo: allow client to pass role in the `password` field (not secure)
    role = form_data.password or "rider"
    now = datetime.utcnow()
    payload = {"username": username, "role": role, "iat": now}
    token = create_access_token({"username": username, "role": role}, config)
    return {"access_token": token, "token_type": "bearer"}
