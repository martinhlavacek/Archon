"""
AsyncPG Database Client for Archon.

Replaces Supabase SDK with direct PostgreSQL connection via asyncpg.
Provides async connection pooling and query execution.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from asyncpg import Pool

from ...config.logfire_config import get_logger
from .client import DatabaseClient

logger = get_logger(__name__)


class AsyncPGClient(DatabaseClient):
    """
    Database client using asyncpg for PostgreSQL connections.

    Uses a singleton connection pool shared across all instances.
    Thread-safe initialization via asyncio.Lock.
    """

    _pool: Pool | None = None
    _lock = asyncio.Lock()

    @classmethod
    async def get_pool(cls) -> Pool:
        """
        Get or create the connection pool.

        Supports two configuration methods:
        1. DATABASE_URL environment variable (full connection string)
        2. Individual POSTGRES_* environment variables:
           - POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB

        Returns:
            asyncpg connection pool

        Raises:
            ValueError: If no connection parameters are set
            ConnectionError: If unable to connect to database
        """
        if cls._pool is None:
            async with cls._lock:
                # Double-check pattern
                if cls._pool is None:
                    database_url = os.getenv("DATABASE_URL")
                    postgres_host = os.getenv("POSTGRES_HOST")

                    if not database_url and not postgres_host:
                        raise ValueError(
                            "Database connection not configured. "
                            "Set DATABASE_URL or POSTGRES_* environment variables."
                        )

                    try:
                        if postgres_host:
                            # Use individual connection parameters
                            # This handles special characters in username/password better
                            cls._pool = await asyncpg.create_pool(
                                host=postgres_host,
                                port=int(os.getenv("POSTGRES_PORT", "5432")),
                                user=os.getenv("POSTGRES_USER", "postgres"),
                                password=os.getenv("POSTGRES_PASSWORD", ""),
                                database=os.getenv("POSTGRES_DB", "postgres"),
                                ssl=os.getenv("POSTGRES_SSL", "require"),
                                min_size=2,
                                max_size=10,
                                command_timeout=60,
                                statement_cache_size=0,
                            )
                            logger.info(f"AsyncPG pool created (host: {postgres_host})")
                        else:
                            # Use DATABASE_URL
                            cls._pool = await asyncpg.create_pool(
                                database_url,
                                min_size=2,
                                max_size=10,
                                command_timeout=60,
                                statement_cache_size=0,
                            )
                            logger.info("AsyncPG pool created (DATABASE_URL)")
                    except Exception as e:
                        logger.error(f"Failed to create database pool: {e}")
                        raise ConnectionError(f"Unable to connect to database: {e}") from e

        return cls._pool

    @classmethod
    @asynccontextmanager
    async def connection(cls):
        """
        Get a connection from the pool as a context manager.

        Usage:
            async with AsyncPGClient.connection() as conn:
                result = await conn.fetch("SELECT * FROM table")
        """
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            yield conn

    @classmethod
    async def fetch(cls, query: str, *args) -> list[dict[str, Any]]:
        """
        Execute query and return all rows as dicts.

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            List of rows as dictionaries
        """
        async with cls.connection() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]

    @classmethod
    async def fetchrow(cls, query: str, *args) -> dict[str, Any] | None:
        """
        Execute query and return single row as dict.

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Single row as dictionary, or None if no results
        """
        async with cls.connection() as conn:
            row = await conn.fetchrow(query, *args)
            return dict(row) if row else None

    @classmethod
    async def fetchval(cls, query: str, *args) -> Any:
        """
        Execute query and return single value.

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Single value from first column of first row
        """
        async with cls.connection() as conn:
            return await conn.fetchval(query, *args)

    @classmethod
    async def execute(cls, query: str, *args) -> str:
        """
        Execute query without returning rows.

        Args:
            query: SQL query with $1, $2, etc. placeholders
            *args: Query parameters

        Returns:
            Command status string (e.g., "INSERT 0 1")
        """
        async with cls.connection() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def executemany(cls, query: str, args: list) -> None:
        """
        Execute query multiple times with different arguments.

        Args:
            query: SQL query with $1, $2, etc. placeholders
            args: List of argument tuples
        """
        async with cls.connection() as conn:
            await conn.executemany(query, args)

    @classmethod
    async def close(cls) -> None:
        """Close the connection pool."""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            logger.info("AsyncPG connection pool closed")


# Convenience functions for direct usage
async def fetch(query: str, *args) -> list[dict[str, Any]]:
    """Execute query and return all rows as dicts."""
    return await AsyncPGClient.fetch(query, *args)


async def fetchrow(query: str, *args) -> dict[str, Any] | None:
    """Execute query and return single row as dict."""
    return await AsyncPGClient.fetchrow(query, *args)


async def fetchval(query: str, *args) -> Any:
    """Execute query and return single value."""
    return await AsyncPGClient.fetchval(query, *args)


async def execute(query: str, *args) -> str:
    """Execute query without returning rows."""
    return await AsyncPGClient.execute(query, *args)


async def close() -> None:
    """Close the connection pool."""
    await AsyncPGClient.close()
