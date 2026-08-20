"""Authentication and JWT Token Service.

Provides secure password hashing (Argon2id), constant-time verification,
HS256 JWT access token signing, validation, and user authentication.
"""
from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import json
import logging
import secrets
from typing import Any, Dict, Optional

from config.settings import settings

logger = logging.getLogger(__name__)

# Try importing argon2-cffi PasswordHasher for state-of-the-art password hashing
try:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError
    _ph: Optional[PasswordHasher] = PasswordHasher()
except ImportError:
    _ph = None
    logger.warning("argon2-cffi not found, falling back to PBKDF2-HMAC-SHA256 password hashing.")


def _b64url_encode(data: bytes) -> str:
    """Encode bytes to URL-safe Base64 without trailing '=' padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _b64url_decode(data_str: str) -> bytes:
    """Decode URL-safe Base64 string with optional padding."""
    padding = 4 - (len(data_str) % 4)
    if padding != 4:
        data_str += "=" * padding
    return base64.urlsafe_b64decode(data_str.encode("utf-8"))


class AuthenticationError(Exception):
    """Raised when authentication or token validation fails."""
    pass


class AuthService:
    """Production-grade authentication service."""

    def __init__(self) -> None:
        self.secret_key = settings.jwt_secret_key
        self.algorithm = settings.jwt_algorithm or "HS256"
        self.expire_minutes = settings.jwt_access_token_expire_minutes

        # Internal user store seeded with admin user from environment
        self._users: Dict[str, Dict[str, Any]] = {}
        self._seed_default_admin()

    def _seed_default_admin(self) -> None:
        """Seed initial administrator user with hashed password from configuration."""
        admin_user = settings.admin_username
        admin_pass = settings.admin_password
        if admin_user and admin_pass:
            hashed = self.hash_password(admin_pass)
            self._users[admin_user.lower()] = {
                "username": admin_user,
                "password_hash": hashed,
                "created_at": datetime.datetime.now(datetime.timezone.utc),
            }
            logger.info("Initialized and seeded administrative user: '%s' (password hashed)", admin_user)

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash plain password using Argon2id or salted PBKDF2-HMAC-SHA256."""
        if not password:
            raise ValueError("Password must not be empty.")
        if _ph is not None:
            return _ph.hash(password)

        # Fallback to PBKDF2-HMAC-SHA256 with 600,000 iterations
        salt = secrets.token_bytes(16)
        key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 600_000)
        return f"pbkdf2:sha256:600000"

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify plain password against hash with constant-time comparison."""
        if not plain_password or not hashed_password:
            return False

        if hashed_password.startswith(""):
            if _ph is None:
                logger.error("Cannot verify argon2 hash without argon2-cffi library installed.")
                return False
            try:
                return _ph.verify(hashed_password, plain_password)
            except Exception:
                return False

        if hashed_password.startswith("pbkdf2:"):
            try:
                parts = hashed_password.split("$")
                if len(parts) != 3:
                    return False
                meta, salt_hex, key_hex = parts
                _, _, iterations_str = meta.split(":")
                iterations = int(iterations_str)
                salt = bytes.fromhex(salt_hex)
                expected_key = bytes.fromhex(key_hex)
                computed_key = hashlib.pbkdf2_hmac("sha256", plain_password.encode("utf-8"), salt, iterations)
                return secrets.compare_digest(computed_key, expected_key)
            except Exception as exc:
                logger.warning("Error verifying PBKDF2 password: %s", exc)
                return False

        return False

    def create_access_token(
        self,
        subject: str,
        expires_delta: Optional[datetime.timedelta] = None,
        custom_claims: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate signed HS256 JWT access token."""
        if not self.secret_key:
            raise AuthenticationError("JWT Secret Key is not configured. Token creation aborted.")

        now = datetime.datetime.now(datetime.timezone.utc)
        delta = expires_delta or datetime.timedelta(minutes=self.expire_minutes)
        exp = now + delta

        payload: Dict[str, Any] = {
            "sub": subject,
            "iat": int(now.timestamp()),
            "exp": int(exp.timestamp()),
            "jti": secrets.token_hex(8),
        }
        if custom_claims:
            payload.update(custom_claims)

        header = {"alg": "HS256", "typ": "JWT"}
        header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))

        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        signature = hmac.new(self.secret_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
        sig_b64 = _b64url_encode(signature)

        return f"{header_b64}.{payload_b64}.{sig_b64}"

    def decode_access_token(self, token: str) -> Dict[str, Any]:
        """Validate signature and expiration of a JWT access token."""
        if not self.secret_key:
            raise AuthenticationError("JWT Secret Key is not configured.")

        parts = token.split(".")
        if len(parts) != 3:
            raise AuthenticationError("Malformed JWT token format.")

        header_b64, payload_b64, sig_b64 = parts
        signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
        expected_sig = hmac.new(self.secret_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
        actual_sig = _b64url_decode(sig_b64)

        if not secrets.compare_digest(actual_sig, expected_sig):
            raise AuthenticationError("Invalid token signature.")

        try:
            payload_json = _b64url_decode(payload_b64).decode("utf-8")
            payload = json.loads(payload_json)
        except Exception as exc:
            raise AuthenticationError(f"Corrupt token payload: {exc}") from exc

        # Expiration check
        exp = payload.get("exp")
        if exp is not None:
            now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
            if now_ts > exp:
                raise AuthenticationError("Token has expired.")

        return payload

    def authenticate_user(self, username: str, plain_password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user against credential store."""
        if not username or not plain_password:
            return None

        user_record = self._users.get(username.strip().lower())
        if not user_record:
            # Perform dummy verify to mitigate timing attacks
            self.verify_password("dummy_password", "=19=65536,t=3,p=4")
            return None

        if self.verify_password(plain_password, user_record["password_hash"]):
            return {"username": user_record["username"]}

        return None


# Global singleton instance
auth_service = AuthService()