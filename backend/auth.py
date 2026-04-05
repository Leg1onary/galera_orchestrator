"""
Authentication module for Galera Orchestrator.

Reads auth config from nodes.yaml (auth section).
Uses bcrypt for password hashing, HS256 JWT for session tokens.

Config example (nodes.yaml):
  auth:
    enabled: true
    username: admin
    password_hash: "$2b$12$..."   # bcrypt hash, generate with: python3 -c "from passlib.hash import bcrypt; print(bcrypt.hash('yourpassword'))"
    token_expire_hours: 24        # optional, default 24
    secret_key: "change-me-32+"   # optional, auto-generated if omitted
"""

import os
import secrets
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt as _bcrypt
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from jose import JWTError, jwt

log = logging.getLogger("galera_orchestrator")

# ── Module-level state ────────────────────────────────────────────
# Populated by init_auth(cfg) on startup and config reload.
_auth_enabled:  bool = False
_username:      str  = "admin"
_password_hash: str  = ""
_secret_key:    str  = secrets.token_hex(32)
_expire_hours:  int  = 24

ALGORITHM = "HS256"

# Paths that are always public (no auth required)
PUBLIC_PATHS = {"/", "/api/auth/login", "/api/health", "/favicon.ico"}
PUBLIC_PREFIXES = ("/static/",)


def init_auth(cfg: dict) -> None:
    """Load auth config from parsed nodes.yaml dict. Call on startup and reload."""
    global _auth_enabled, _username, _password_hash, _secret_key, _expire_hours

    auth = cfg.get("auth", {}) or {}
    _auth_enabled = bool(auth.get("enabled", False))

    if not _auth_enabled:
        return

    _username     = str(auth.get("username", "admin")).strip()
    _password_hash = str(auth.get("password_hash", "")).strip()
    _expire_hours  = int(auth.get("token_expire_hours", 24))

    # Secret key: use from config if provided, otherwise keep the auto-generated one
    cfg_secret = str(auth.get("secret_key", "")).strip()
    if cfg_secret and cfg_secret != "change-me-32+":
        _secret_key = cfg_secret

    if not _password_hash:
        log.warning("auth.enabled=true but auth.password_hash is empty — auth will reject all logins")


def is_auth_enabled() -> bool:
    return _auth_enabled


def verify_password(plain: str) -> bool:
    if not _password_hash:
        return False
    try:
        return _bcrypt.checkpw(plain.encode(), _password_hash.encode())
    except Exception:
        return False


def create_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=_expire_hours)
    payload = {"sub": username, "exp": expire}
    return jwt.encode(payload, _secret_key, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[str]:
    """Return username if token valid, None otherwise."""
    try:
        data = jwt.decode(token, _secret_key, algorithms=[ALGORITHM])
        return data.get("sub")
    except JWTError:
        return None


def get_token_from_request(request: Request) -> Optional[str]:
    """Extract JWT from Authorization header or galera_token cookie."""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:]
    return request.cookies.get("galera_token")


def require_auth(request: Request) -> None:
    """
    Raise HTTP 401 if auth is enabled and request is not authenticated.
    Call this at the start of protected endpoints, or use as a dependency.
    """
    if not _auth_enabled:
        return

    path = request.url.path
    if path in PUBLIC_PATHS:
        return
    if any(path.startswith(p) for p in PUBLIC_PREFIXES):
        return

    token = get_token_from_request(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    username = decode_token(token)
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired or invalid",
            headers={"WWW-Authenticate": "Bearer"},
        )


def hash_password(plain: str) -> str:
    """Utility: generate a bcrypt hash for a plaintext password."""
    return _bcrypt.hashpw(plain.encode(), _bcrypt.gensalt()).decode()
