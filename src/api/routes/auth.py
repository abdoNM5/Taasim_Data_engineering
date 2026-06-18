from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt

from ..core.config import AppConfig

router = APIRouter()


def _create_access_token(data: dict[str, Any], config: AppConfig, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=60))
    to_encode.update({"exp": expire, "sub": data.get("username")})
    return jwt.encode(to_encode, config.jwt_secret, algorithm="HS256")


@router.post("/auth/token", response_model=None)
async def auth_token(
    request_: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> dict[str, str]:
    config: AppConfig = request_.app.state.config
    username = form_data.username
    role = form_data.password or "rider"
    token = _create_access_token({"username": username, "role": role}, config)
    return {"access_token": token, "token_type": "bearer"}
