"""
Authentication for the API. This is deliberately small and standard:
- Passwords are hashed with bcrypt, never stored or logged in plain text.
- Login issues a JWT containing the user's id and org_id. The frontend
  stores it and sends it as a Bearer token on every request.
- get_current_user is a FastAPI dependency any endpoint can require to
  become "logged-in users only" - and it's what lets an endpoint know
  WHICH organization's data to return, which is the actual tenant
  isolation mechanism (see /retail in main.py).
"""
import os
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24

bearer_scheme = HTTPBearer()


def hash_password(password: str) -> str:
    # bcrypt has a 72-byte input limit; truncate defensively rather than
    # letting an unusually long password raise an error at signup.
    return bcrypt.hashpw(password.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8")[:72], password_hash.encode("utf-8"))


def create_access_token(user_id: int, org_id: int, email: str) -> str:
    payload = {
        "sub": str(user_id),
        "org_id": org_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


class CurrentUser(BaseModel):
    user_id: int
    org_id: int
    email: str


def decode_token(token: str) -> CurrentUser:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    return CurrentUser(
        user_id=int(payload["sub"]),
        org_id=payload["org_id"],
        email=payload["email"],
    )


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> CurrentUser:
    """
    Require a valid Bearer token. Any endpoint that depends on this becomes
    logged-in-users-only, and gets back exactly which org the caller
    belongs to - that org_id is what /retail filters on, which is the
    actual data isolation, not just "logged in or not".
    """
    return decode_token(credentials.credentials)
