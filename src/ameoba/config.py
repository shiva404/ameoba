"""Application configuration via environment variables and .env files.

All configuration is centralised here.  No magic strings in the rest of
the codebase — always import from this module.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class EmbeddedConfig(BaseSettings):
    """Paths for embedded (zero-dependency) storage backends."""

    model_config = SettingsConfigDict(env_prefix="AMEOBA_EMBEDDED_")

    data_dir: Path = Field(
        default=Path("~/.ameoba/data").expanduser(),
        description="Root directory for all embedded storage",
    )

    @field_validator("data_dir", mode="before")
    @classmethod
    def expand_home(cls, v: str | Path) -> Path:
        return Path(v).expanduser().resolve()

    @property
    def duckdb_path(self) -> Path:
        return self.data_dir / "ameoba.duckdb"

    @property
    def sqlite_audit_path(self) -> Path:
        return self.data_dir / "audit.sqlite"

    @property
    def blob_dir(self) -> Path:
        return self.data_dir / "blobs"

    @property
    def staging_db_path(self) -> Path:
        return self.data_dir / "staging.duckdb"

    def ensure_dirs(self) -> None:
        """Create all required directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.blob_dir.mkdir(parents=True, exist_ok=True)


class ClassifierConfig(BaseSettings):
    """Tunable thresholds for the classification pipeline."""

    model_config = SettingsConfigDict(env_prefix="AMEOBA_CLASSIFIER_")

    # Structural layer thresholds
    relational_jaccard_threshold: float = Field(
        default=0.85, ge=0.0, le=1.0,
        description="Min Jaccard key similarity to consider data relational",
    )
    document_jaccard_threshold: float = Field(
        default=0.5, ge=0.0, le=1.0,
        description="Max Jaccard similarity before classifying as document",
    )
    max_relational_nesting_depth: int = Field(
        default=2,
        description="Max nesting depth still considered relational",
    )
    # Binary layer
    blob_entropy_threshold: float = Field(
        default=7.0, ge=0.0, le=8.0,
        description="Shannon entropy (bits/byte) above which data is treated as blob",
    )
    blob_null_byte_pct_threshold: float = Field(
        default=0.01, ge=0.0, le=1.0,
        description="Null-byte fraction above which data is treated as binary blob",
    )
    # Streaming / large payload
    streaming_byte_budget: int = Field(
        default=10 * 1024 * 1024,  # 10 MB
        description="Max bytes to read for streaming classification",
    )
    direct_blob_size_bytes: int = Field(
        default=1024 * 1024 * 1024,  # 1 GB
        description="Payloads above this size go directly to blob store",
    )
    # Schema drift
    schema_drift_window: int = Field(
        default=100,
        description="Records between schema drift checks",
    )


class APIConfig(BaseSettings):
    """HTTP and gRPC server settings."""

    model_config = SettingsConfigDict(env_prefix="AMEOBA_API_")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)
    grpc_port: int = Field(default=50051, ge=1, le=65535)
    workers: int = Field(default=1, ge=1)
    cors_origins: list[str] = Field(default=["*"])
    request_timeout_seconds: int = Field(default=30, ge=1)


class AuthConfig(BaseSettings):
    """Authentication and authorisation settings."""

    model_config = SettingsConfigDict(env_prefix="AMEOBA_AUTH_")

    # API key (dev/test only — disable in production)
    api_key_enabled: bool = Field(default=True)
    api_keys: list[str] = Field(
        default_factory=list,
        description="Comma-separated list of valid API keys",
    )

    # JWT
    jwt_secret: str = Field(
        default="CHANGE_ME_IN_PRODUCTION",
        description="HS256 secret or path to RSA public key",
    )
    jwt_algorithm: Literal["HS256", "RS256"] = Field(default="HS256")

    # Cedar policy
    cedar_policy_dir: Path = Field(
        default=Path(__file__).parent / "security" / "authz" / "policies",
    )


class ObservabilityConfig(BaseSettings):
    """Logging, tracing, and metrics settings."""

    model_config = SettingsConfigDict(env_prefix="AMEOBA_OBS_")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(default="INFO")
    log_format: Literal["json", "pretty"] = Field(
        default="pretty",
        description="json for production, pretty for local dev",
    )
    otlp_endpoint: str | None = Field(
        default=None,
        description="OpenTelemetry collector endpoint (e.g. http://localhost:4317)",
    )
    service_name: str = Field(default="ameoba")


class Settings(BaseSettings):
    """Root settings object — compose all sub-configs here."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    embedded: EmbeddedConfig = Field(default_factory=EmbeddedConfig)
    classifier: ClassifierConfig = Field(default_factory=ClassifierConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    obs: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    # Environment tag (affects log format default, safety checks)
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
    )

    def is_production(self) -> bool:
        return self.environment == "production"


# Module-level singleton — import this everywhere.
# Tests can override with `settings = Settings(_env_file=None, ...)`.
settings = Settings()
