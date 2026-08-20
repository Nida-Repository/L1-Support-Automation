"""Unit Tests for Authentication Service and JWT Security.

Validates password hashing, constant-time verification, JWT signing, expiration,
tamper detection, and dependency injection.
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from api.dependencies import get_current_user
from config.settings import settings
from services.auth_service import AuthenticationError, AuthService


def test_password_hashing_and_verification():
    auth = AuthService()
    plain = "SuperSecurePassword123!"
    hashed = auth.hash_password(plain)

    assert hashed != plain
    assert auth.verify_password(plain, hashed) is True
    assert auth.verify_password("WrongPassword", hashed) is False
    assert auth.verify_password("", hashed) is False
    assert auth.verify_password(plain, "") is False


def test_jwt_token_lifecycle():
    auth = AuthService()
    token = auth.create_access_token(
        subject="test_engineer",
        expires_delta=datetime.timedelta(minutes=15),
        custom_claims={"role": "L1_Support"},
    )
    assert token is not None
    assert len(token.split(".")) == 3

    payload = auth.decode_access_token(token)
    assert payload["sub"] == "test_engineer"
    assert payload["role"] == "L1_Support"
    assert "exp" in payload
    assert "iat" in payload


def test_jwt_token_tampering_rejected():
    auth = AuthService()
    token = auth.create_access_token(subject="admin")
    parts = token.split(".")
    
    # Tamper with payload
    tampered_payload = parts[1] + "tamper"
    tampered_token = f"{parts[0]}.{tampered_payload}.{parts[2]}"

    with pytest.raises(AuthenticationError):
        auth.decode_access_token(tampered_token)


def test_jwt_token_expiration():
    auth = AuthService()
    # Create token expired 5 minutes ago
    token = auth.create_access_token(
        subject="admin",
        expires_delta=datetime.timedelta(minutes=-5),
    )
    with pytest.raises(AuthenticationError, match="Token has expired"):
        auth.decode_access_token(token)


def test_authenticate_user():
    auth = AuthService()
    # Test seeded admin
    user = auth.authenticate_user(settings.admin_username, settings.admin_password)
    assert user is not None
    assert user["username"] == settings.admin_username

    # Bad password
    assert auth.authenticate_user(settings.admin_username, "IncorrectPass!") is None
    # Unknown user
    assert auth.authenticate_user("nonexistent_user", "AnyPassword") is None


def test_get_current_user_dependency_valid():
    auth = AuthService()
    token = auth.create_access_token(subject="support_hero")
    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

    user = get_current_user(credentials)
    assert user.username == "support_hero"


def test_get_current_user_dependency_missing():
    with pytest.raises(HTTPException) as exc_info:
        get_current_user(None)
    assert exc_info.value.status_code == 401
    assert "Authentication required" in exc_info.value.detail


def test_get_current_user_dependency_invalid():
    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="invalid.token.here")
    with pytest.raises(HTTPException) as exc_info:
        get_current_user(credentials)
    assert exc_info.value.status_code == 401