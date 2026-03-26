"""OAuth2 Client Credentials + Private Key JWT authentication.

Implements RFC 7523 (JWT Profile for OAuth 2.0 Client Authentication) and
the emerging IETF draft for OAuth 2.0 AI Agent On-Behalf-Of Authorization.

The token validator is designed to be framework-agnostic — it receives a
raw bearer token string and returns an AgentIdentity or raises.

Dependencies: python-jose (optional — graceful fallback if not installed).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from ameoba.domain.security import AgentIdentity

logger = structlog.get_logger(__name__)

_JOSE_AVAILABLE = False
try:
    from jose import JWTError, jwt as _jwt  # type: ignore[import]
    _JOSE_AVAILABLE = True
except ImportError:
    pass


class JWTValidator:
    """Validates JWT bearer tokens and extracts AgentIdentity.

    Supports:
    - HS256 (symmetric secret — development)
    - RS256 (asymmetric — production, use a real JWKS endpoint)

    For production, replace ``secret_or_pubkey`` with a JWKS URI fetcher.
    """

    def __init__(
        self,
        secret_or_pubkey: str,
        algorithm: str = "HS256",
        *,
        audience: str | None = None,
        issuer: str | None = None,
    ) -> None:
        self._secret = secret_or_pubkey
        self._algorithm = algorithm
        self._audience = audience
        self._issuer = issuer

    def validate(self, token: str) -> AgentIdentity:
        """Validate a JWT and return the caller's identity.

        Raises:
            ValueError: If the token is invalid or expired.
        """
        if not _JOSE_AVAILABLE:
            raise RuntimeError(
                "python-jose is not installed. Run: pip install python-jose[cryptography]"
            )

        try:
            options: dict[str, Any] = {"verify_exp": True}
            claims = _jwt.decode(  # type: ignore[union-attr]
                token,
                self._secret,
                algorithms=[self._algorithm],
                audience=self._audience,
                issuer=self._issuer,
                options=options,
            )
        except JWTError as exc:  # type: ignore[name-defined]
            raise ValueError(f"Invalid JWT: {exc}") from exc

        return _claims_to_identity(claims)

    def create_token(
        self,
        agent_id: str,
        *,
        tenant_id: str = "default",
        scopes: list[str] | None = None,
        expires_in_seconds: int = 3600,
    ) -> str:
        """Create a signed JWT for testing / internal use."""
        if not _JOSE_AVAILABLE:
            raise RuntimeError("python-jose is required to create tokens")

        import time
        now = int(time.time())
        claims = {
            "sub": agent_id,
            "tenant_id": tenant_id,
            "scopes": scopes or ["read", "write"],
            "iat": now,
            "exp": now + expires_in_seconds,
        }
        if self._issuer:
            claims["iss"] = self._issuer
        if self._audience:
            claims["aud"] = self._audience

        return _jwt.encode(claims, self._secret, algorithm=self._algorithm)  # type: ignore[union-attr]


def _claims_to_identity(claims: dict[str, Any]) -> AgentIdentity:
    """Map JWT claims to an AgentIdentity domain object."""
    now = datetime.now(timezone.utc)

    agent_id = claims.get("sub") or claims.get("client_id") or "unknown"
    tenant_id = claims.get("tenant_id") or claims.get("tid") or "default"
    scopes_raw = claims.get("scopes") or claims.get("scope") or []
    if isinstance(scopes_raw, str):
        scopes_raw = scopes_raw.split()
    groups = claims.get("groups") or []

    # RFC 8693 delegation: `act` claim contains the acting agent
    act = claims.get("act", {})
    delegated_by: str | None = None
    delegation_depth = 0
    if act:
        delegated_by = act.get("sub")
        delegation_depth = int(claims.get("delegation_depth", 1))

    exp_ts = claims.get("exp")
    iat_ts = claims.get("iat")

    return AgentIdentity(
        agent_id=agent_id,
        tenant_id=tenant_id,
        groups=groups,
        session_id=claims.get("session_id"),
        scopes=scopes_raw,
        issued_at=datetime.fromtimestamp(iat_ts, tz=timezone.utc) if iat_ts else None,
        expires_at=datetime.fromtimestamp(exp_ts, tz=timezone.utc) if exp_ts else None,
        delegated_by=delegated_by,
        delegation_depth=delegation_depth,
    )
