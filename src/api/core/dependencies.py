from __future__ import annotations

from typing import Any

from fastapi import Depends, HTTPException, Request, status

from .config import AppConfig


async def verify_token(request: Request, authorization: str | None = None) -> dict[str, Any] | None:
    config: AppConfig = request.app.state.config
    if not config.auth_enabled:
        return None

    auth_header = authorization or request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = auth_header.split(" ", 1)[1]
    try:
        from jose import jwt, JWTError
        payload = jwt.decode(token, config.jwt_secret, algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
