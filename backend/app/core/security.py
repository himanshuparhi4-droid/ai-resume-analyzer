from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from passlib.context import CryptContext
from passlib.exc import MissingBackendError, PasswordValueError, UnknownHashError

from app.core.config import settings

# Prefer a stable pbkdf2 hash for new passwords, but keep bcrypt available so
# older deployment-local accounts can still log in and get upgraded in place.
pwd_context = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")
ALGORITHM = "HS256"


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except (MissingBackendError, PasswordValueError, TypeError, UnknownHashError, ValueError):
        return False


def verify_and_update_password(plain_password: str, hashed_password: str) -> tuple[bool, str | None]:
    try:
        return pwd_context.verify_and_update(plain_password, hashed_password)
    except (MissingBackendError, PasswordValueError, TypeError, UnknownHashError, ValueError):
        return False, None


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def create_access_token(subject: str, expires_delta: timedelta | None = None) -> str:
    expire = datetime.now(UTC) + (expires_delta or timedelta(minutes=settings.access_token_expire_minutes))
    payload: dict[str, Any] = {"sub": subject, "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=ALGORITHM)


def decode_access_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, settings.secret_key, algorithms=[ALGORITHM])
