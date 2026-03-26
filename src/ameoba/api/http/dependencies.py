"""FastAPI dependency injection.

All handlers receive the kernel via ``Depends(get_kernel)`` — never
import it directly, as this keeps handlers testable.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from ameoba.kernel.kernel import AmeobaKernel

# ---------------------------------------------------------------------------
# Kernel dependency
# ---------------------------------------------------------------------------

def get_kernel(request: Request) -> AmeobaKernel:
    """Retrieve the kernel from app state (set in lifespan)."""
    kernel: AmeobaKernel | None = getattr(request.app.state, "kernel", None)
    if kernel is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kernel not initialised",
        )
    return kernel


KernelDep = Annotated[AmeobaKernel, Depends(get_kernel)]

# ---------------------------------------------------------------------------
# Authentication (MVP: API key)
# ---------------------------------------------------------------------------

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_agent_id(
    request: Request,
    api_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> str | None:
    """Extract the caller's agent identity from the request.

    MVP: validates API key against the configured list.
    Production: replace with JWT / OAuth2 middleware.
    """
    from ameoba.config import settings

    if not settings.auth.api_key_enabled:
        return None  # Auth disabled

    if not settings.auth.api_keys:
        return "anonymous"  # No keys configured — allow all (dev mode)

    if api_key and api_key in settings.auth.api_keys:
        return api_key[:8]  # Use first 8 chars as agent_id for logs

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "ApiKey"},
    )


AgentIdDep = Annotated[str | None, Depends(get_agent_id)]
