"""FastAPI application factory.

Usage::

    from ameoba.api.http.app import create_app
    app = create_app()
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ameoba.api.http.routers import audit, health, ingest, query, schema
from ameoba.config import Settings, settings as default_settings
from ameoba.kernel.kernel import AmeobaKernel
from ameoba.observability.logging import configure_logging

logger = structlog.get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Override settings (useful for testing).

    Returns:
        A fully configured FastAPI application.
    """
    cfg = settings or default_settings

    configure_logging(level=cfg.obs.log_level, fmt=cfg.obs.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Start the kernel on startup; stop it on shutdown."""
        kernel = AmeobaKernel(settings=cfg)
        await kernel.start()
        app.state.kernel = kernel
        logger.info("app_started")
        try:
            yield
        finally:
            await kernel.stop()
            logger.info("app_stopped")

    app = FastAPI(
        title="Ameoba",
        description="Intelligent adaptive data fabric for agentic workflows",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS (must be added before auth middleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.api.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Auth middleware (JWT Bearer + X-API-Key)
    if cfg.auth.api_key_enabled:
        from ameoba.security.authn.api_key import APIKeyStore
        from ameoba.security.authn.middleware import AuthMiddleware
        api_key_store = APIKeyStore()
        if cfg.auth.api_keys:
            api_key_store.load_from_list(cfg.auth.api_keys)
        jwt_validator = None
        try:
            from ameoba.security.authn.oauth2 import JWTValidator
            jwt_validator = JWTValidator(
                secret_or_key=cfg.auth.jwt_secret,
                algorithm=cfg.auth.jwt_algorithm,
            )
        except ImportError:
            pass
        app.add_middleware(AuthMiddleware, api_key_store=api_key_store, jwt_validator=jwt_validator)

    # Routers
    app.include_router(health.router)
    app.include_router(ingest.router)
    app.include_router(query.router)
    app.include_router(audit.router)
    app.include_router(schema.router)

    return app
