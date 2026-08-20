"""FastAPI Router for User Authentication.

Provides the login endpoint to issue JWT access tokens for support engineers.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from config.settings import settings
from models.auth_model import LoginRequest, TokenResponse
from services.auth_service import auth_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post(
    "/login",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
    summary="Authenticate support engineer and obtain JWT access token",
)
def login(payload: LoginRequest) -> TokenResponse:
    """Validate engineer credentials and return a signed HS256 JWT access token.

    - **username**: Registered username (default: admin)
    - **password**: Configured password
    """
    logger.info("Login attempt for user '%s'", payload.username)
    user = auth_service.authenticate_user(payload.username, payload.password)
    if not user:
        logger.warning("Failed login attempt for user '%s'", payload.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expires_seconds = settings.jwt_access_token_expire_minutes * 60
    access_token = auth_service.create_access_token(subject=user["username"])
    logger.info("Successfully issued JWT token for user '%s' (expires in %ds)", user["username"], expires_seconds)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=expires_seconds,
    )