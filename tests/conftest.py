"""Shared pytest fixtures.

Tests use isolated temporary directories for all embedded backends
so they never touch the user's real data directory.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
import pytest_asyncio

from ameoba.config import EmbeddedConfig, Settings
from ameoba.kernel.kernel import AmeobaKernel


@pytest.fixture(scope="session")
def event_loop_policy():
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    return tmp_path / "ameoba_test_data"


@pytest.fixture
def test_settings(tmp_data_dir: Path) -> Settings:
    """Settings wired to a temporary directory — safe for parallel tests."""
    embedded = EmbeddedConfig(data_dir=tmp_data_dir)
    return Settings(
        embedded=embedded,
        environment="development",
    )


@pytest_asyncio.fixture
async def kernel(test_settings: Settings) -> AsyncGenerator[AmeobaKernel, None]:
    """A fully started kernel backed by a temporary directory."""
    k = AmeobaKernel(test_settings)
    await k.start()
    yield k
    await k.stop()
