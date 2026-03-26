"""GET /v1/health — liveness and readiness probes."""

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    backends: dict[str, str]
    audit_sequence: int


@router.get("/v1/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    """Liveness probe — returns 200 if the kernel is running."""
    from ameoba.api.http.dependencies import get_kernel
    try:
        kernel = get_kernel(request)
        h = await kernel.health()
        return HealthResponse(
            status=h.get("kernel", "ok"),
            backends=h.get("backends", {}),
            audit_sequence=h.get("audit_sequence", 0),
        )
    except Exception as exc:
        return HealthResponse(status=f"error: {exc}", backends={}, audit_sequence=0)


@router.get("/v1/ready")
async def ready(request: Request) -> dict:
    """Readiness probe — returns 200 when all backends are healthy."""
    from ameoba.api.http.dependencies import get_kernel
    kernel = get_kernel(request)
    h = await kernel.health()
    degraded = [bid for bid, s in h.get("backends", {}).items() if s != "available"]
    if degraded:
        return {"ready": False, "degraded_backends": degraded}
    return {"ready": True}
