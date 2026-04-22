from __future__ import annotations

import unittest

from passlib.hash import bcrypt
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.models.user import User
from app.schemas.auth import PasswordResetRequest, UserCreate
from app.services.auth_service import AuthService


class AuthServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            future=True,
        )
        Base.metadata.create_all(bind=engine)
        self.session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)()
        self.service = AuthService(self.session)

    def tearDown(self) -> None:
        self.session.close()

    def test_login_accepts_legacy_bcrypt_hash_and_upgrades_it(self) -> None:
        legacy_hash = bcrypt.hash("legacy-password")
        user = User(
            email="legacy@example.com",
            full_name="Legacy User",
            hashed_password=legacy_hash,
        )
        self.session.add(user)
        self.session.commit()

        response = self.service.login("legacy@example.com", "legacy-password")

        self.session.refresh(user)
        self.assertEqual(response.user.email, "legacy@example.com")
        self.assertNotEqual(user.hashed_password, legacy_hash)
        self.assertIn("pbkdf2-sha256", user.hashed_password)

    def test_reset_password_matches_trimmed_case_insensitive_identity_and_logs_user_in(self) -> None:
        self.service.register(
            UserCreate(
                email="reset@example.com",
                full_name="Himanshu Parhi",
                password="OriginalPass123",
            )
        )

        response = self.service.reset_password(
            PasswordResetRequest(
                email="  reset@example.com  ",
                full_name="  himanshu   parhi ",
                new_password="UpdatedPass456",
            )
        )

        self.assertEqual(response.user.email, "reset@example.com")
        login_response = self.service.login("reset@example.com", "UpdatedPass456")
        self.assertEqual(login_response.user.full_name, "Himanshu Parhi")


if __name__ == "__main__":
    unittest.main()
