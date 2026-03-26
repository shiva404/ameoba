"""FastAPI authentication middleware.

Extracts and validates the caller identity from HTTP requests.
Supports API keys (X-API-Key header) and Bearer JWT tokens.

The principal is injected into ``request.state.principal`` for use
in route handlers and the authorization gateway.
"""

from __future__ import annotations

from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

import structlog

from ameoba.domain.security import AgentIdentity

logger = structlog.get_logger(__name__)

# Public paths that skip authentication
_PUBLIC_PATHS = {"/v1/health", "/v1/ready", "/docs", "/redoc", "/openapi.json"}


class AuthMiddleware(BaseHTTPMiddleware):
    """Extract caller identity on every request.

    Sets ``request.state.principal`` to an ``AgentIdentity`` if authenticated,
    or ``None`` for unauthenticated requests to public paths.

    Route handlers that require authentication should use ``get_agent_id``
    dependency from ``dependencies.py``.
    """

    def __init__(self, app: ASGIApp, *, api_key_store: object, jwt_validator: object | None = None) -> None:
        super().__init__(app)
        self._api_keys = api_key_store
        self._jwt = jwt_validator

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        principal: AgentIdentity | None = None

        if request.url.path not in _PUBLIC_PATHS:
            principal = await self._extract_principal(request)

        request.state.principal = principal

        # Inject tenant context for structured logging
        if principal:
            structlog.contextvars.bind_contextvars(
                agent_id=principal.agent_id,
                tenant_id=principal.tenant_id,
            )

        response = await call_next(request)

        structlog.contextvars.clear_contextvars()
        return response

    async def _extract_principal(self, request: Request) -> AgentIdentity | None:
        # 1. Bearer JWT
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer ") and self._jwt is not None:
            token = auth_header[7:]
            try:
                return self._jwt.validate(token)  # type: ignore[attr-defined]
            except Exception as exc:
                logger.debug("jwt_validation_failed", error=str(exc))

        # 2. API key
        api_key = request.headers.get("X-API-Key")
        if api_key:
            meta = self._api_keys.validate(api_key)  # type: ignore[attr-defined]
            if meta:
                return AgentIdentity(
                    agent_id=meta["agent_id"],
                    tenant_id=meta["tenant_id"],
                    scopes=meta.get("scopes", []),
                )

        return None
