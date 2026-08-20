"""Pydantic v2 Schemas for User Authentication and Token Management.

Provides request and response validation for user login and JWT token exchange.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class LoginRequest(BaseModel):
    """Payload for user login."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    username: str = Field(..., min_length=1, max_length=100, description="User's username")
    password: str = Field(..., min_length=1, description="User's plain password")


class TokenResponse(BaseModel):
    """Response containing JWT access token and expiration information."""

    access_token: str = Field(..., description="Signed JWT Bearer token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token validity in seconds")


class UserRead(BaseModel):
    """Authenticated user context representation."""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)

    username: str = Field(..., max_length=100, description="Authenticated username")