from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, field_validator


def _strip_string(value: str) -> str:
    return str(value or "").strip()


class UserCreate(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=2, max_length=255)
    password: str = Field(min_length=8, max_length=128)

    @field_validator("email", mode="before")
    @classmethod
    def strip_email(cls, value: str) -> str:
        return _strip_string(value)

    @field_validator("full_name", mode="before")
    @classmethod
    def strip_full_name(cls, value: str) -> str:
        return _strip_string(value)


class UserLogin(BaseModel):
    email: EmailStr
    password: str

    @field_validator("email", mode="before")
    @classmethod
    def strip_email(cls, value: str) -> str:
        return _strip_string(value)


class PasswordResetRequest(BaseModel):
    email: EmailStr
    full_name: str = Field(min_length=2, max_length=255)
    new_password: str = Field(min_length=8, max_length=128)

    @field_validator("email", mode="before")
    @classmethod
    def strip_email(cls, value: str) -> str:
        return _strip_string(value)

    @field_validator("full_name", mode="before")
    @classmethod
    def strip_full_name(cls, value: str) -> str:
        return _strip_string(value)


class UserOut(BaseModel):
    id: str
    email: EmailStr
    full_name: str
    created_at: datetime


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut
