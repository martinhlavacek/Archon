"""
Database client interface and factory.

Provides a unified interface for database operations that can be
backed by either asyncpg or Supabase.
"""

import os
from abc import ABC, abstractmethod
from typing import Any

from ...config.logfire_config import get_logger

logger = get_logger(__name__)


class DatabaseClient(ABC):
    """Abstract base class for database clients."""

    @abstractmethod
    async def fetch(self, query: str, *args) -> list[dict[str, Any]]:
        """Execute query and return all rows as dicts."""
        pass

    @abstractmethod
    async def fetchrow(self, query: str, *args) -> dict[str, Any] | None:
        """Execute query and return single row as dict."""
        pass

    @abstractmethod
    async def fetchval(self, query: str, *args) -> Any:
        """Execute query and return single value."""
        pass

    @abstractmethod
    async def execute(self, query: str, *args) -> str:
        """Execute query without returning rows."""
        pass

    @abstractmethod
    async def executemany(self, query: str, args: list) -> None:
        """Execute query multiple times with different arguments."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the database connection/pool."""
        pass


def get_database_client() -> type:
    """
    Get the appropriate database client class based on environment.

    Returns:
        AsyncPGClient if DATABASE_URL is set, otherwise raises error.

    Raises:
        ValueError: If DATABASE_URL is not configured.
    """
    database_url = os.getenv("DATABASE_URL")

    if database_url:
        from .asyncpg_client import AsyncPGClient

        logger.info("Using AsyncPG database client")
        return AsyncPGClient
    else:
        raise ValueError(
            "DATABASE_URL environment variable is required. "
            "Set it to your PostgreSQL connection string, e.g.: "
            "postgresql://user:password@host:5432/database"
        )
