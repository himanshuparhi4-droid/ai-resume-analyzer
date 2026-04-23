from __future__ import annotations

import re

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.security import create_access_token, get_password_hash, verify_and_update_password
from app.models.user import User
from app.schemas.auth import PasswordResetRequest, TokenResponse, UserCreate, UserOut


class AuthService:
    def __init__(self, db: Session) -> None:
        self.db = db

    @staticmethod
    def _normalize_email(email: str) -> str:
        return str(email or "").strip().lower()

    @staticmethod
    def _normalize_full_name(full_name: str) -> str:
        collapsed = re.sub(r"\s+", " ", str(full_name or "")).strip()
        return collapsed.casefold()

    def _issue_token(self, user: User) -> TokenResponse:
        return TokenResponse(
            access_token=create_access_token(user.id),
            user=UserOut.model_validate(user, from_attributes=True),
        )

    def register(self, payload: UserCreate) -> TokenResponse:
        normalized_email = self._normalize_email(payload.email)
        existing = self.db.scalar(select(User).where(User.email == normalized_email))
        if existing:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")

        user = User(
            email=normalized_email,
            full_name=payload.full_name.strip(),
            hashed_password=get_password_hash(payload.password),
        )
        self.db.add(user)
        try:
            self.db.commit()
        except IntegrityError as exc:
            self.db.rollback()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered") from exc
        self.db.refresh(user)
        return self._issue_token(user)

    def login(self, email: str, password: str) -> TokenResponse:
        normalized_email = self._normalize_email(email)
        user = self.db.scalar(select(User).where(User.email == normalized_email))
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

        verified, upgraded_hash = verify_and_update_password(password, user.hashed_password)
        if not verified:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

        if upgraded_hash and upgraded_hash != user.hashed_password:
            user.hashed_password = upgraded_hash
            self.db.add(user)
            self.db.commit()
            self.db.refresh(user)

        return self._issue_token(user)

    def reset_password(self, payload: PasswordResetRequest) -> TokenResponse:
        normalized_email = self._normalize_email(payload.email)
        user = self.db.scalar(select(User).where(User.email == normalized_email))
        if not user or self._normalize_full_name(user.full_name) != self._normalize_full_name(payload.full_name):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No account matched that email and full name on this deployment.",
            )

        verified, _ = verify_and_update_password(payload.new_password, user.hashed_password)
        if verified:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Choose a new password that is different from the current one.",
            )

        user.hashed_password = get_password_hash(payload.new_password)
        user.full_name = user.full_name.strip()
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        return self._issue_token(user)
