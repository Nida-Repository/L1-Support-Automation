"""FastAPI Authentication and Authorization Dependencies.

Provides JWT Bearer token validation and dependency injection for protected endpoints.
"""
from __future__ import annotations

import logging
from typing import Optional

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from models.auth_model import UserRead
from services.auth_service import AuthenticationError, auth_service

logger = logging.getLogger(__name__)

# Security scheme for OpenAPI Swagger UI Bearer authentication
http_bearer = HTTPBearer(
    scheme_name="BearerAuth",
    description="Enter your JWT Bearer token generated from /auth/login",
    auto_error=False,
)


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(http_bearer),
) -> UserRead:
    """Validate Bearer token from request Authorization header and return authenticated user."""
    if credentials is None or not credentials.credentials:
        logger.warning("Authentication failed: Missing Bearer authorization header.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required. Provide a valid Bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    try:
        payload = auth_service.decode_access_token(token)
        username: Optional[str] = payload.get("sub")
        if not username:
            logger.warning("Authentication failed: Token missing 'sub' claim.")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token claims: subject missing.",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return UserRead(username=username)

    except AuthenticationError as exc:
        logger.warning("Token verification failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as exc:
        logger.error("Unexpected error validating token: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate authentication credentials.",
            headers={"WWW-Authenticate": "Bearer"},
        )