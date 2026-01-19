"""
Client Manager Service

Manages database and API client connections.
Supports both asyncpg (preferred for K8s) and Supabase (legacy) backends.
"""

import os
import re

from ..config.logfire_config import search_logger

# Track which database mode we're using
_database_mode: str | None = None


def get_database_mode() -> str:
    """
    Determine and cache which database mode to use.

    Returns:
        'asyncpg' if DATABASE_URL is set, 'supabase' otherwise
    """
    global _database_mode
    if _database_mode is None:
        if os.getenv("DATABASE_URL"):
            _database_mode = "asyncpg"
            search_logger.info("Database mode: asyncpg (DATABASE_URL)")
        elif os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_KEY"):
            _database_mode = "supabase"
            search_logger.info("Database mode: supabase (SUPABASE_URL)")
        else:
            raise ValueError(
                "Database configuration required. Set one of:\n"
                "  - DATABASE_URL: PostgreSQL connection string (preferred for K8s)\n"
                "  - SUPABASE_URL + SUPABASE_SERVICE_KEY: Supabase credentials (legacy)"
            )
    return _database_mode


def is_asyncpg_mode() -> bool:
    """Check if using asyncpg mode."""
    return get_database_mode() == "asyncpg"


def get_supabase_client():
    """
    Get a Supabase client instance (legacy mode).

    Returns:
        Supabase client instance

    Raises:
        ValueError: If in asyncpg mode or Supabase not configured
    """
    from supabase import Client, create_client

    # Check if we should use Supabase
    mode = get_database_mode()
    if mode == "asyncpg":
        raise ValueError(
            "Supabase client not available in asyncpg mode. "
            "Use AsyncPGClient from services.database instead."
        )

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")

    if not url or not key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in environment variables"
        )

    try:
        # Let Supabase handle connection pooling internally
        client = create_client(url, key)

        # Extract project ID from URL for logging purposes only
        match = re.match(r"https://([^.]+)\.supabase\.co", url)
        if match:
            project_id = match.group(1)
            search_logger.debug(f"Supabase client initialized - project_id={project_id}")

        return client
    except Exception as e:
        search_logger.error(f"Failed to create Supabase client: {e}")
        raise


def get_asyncpg_client():
    """
    Get the AsyncPGClient class for asyncpg mode.

    Returns:
        AsyncPGClient class (not instance - use class methods)

    Raises:
        ValueError: If not in asyncpg mode
    """
    mode = get_database_mode()
    if mode != "asyncpg":
        raise ValueError(
            "AsyncPG client only available in asyncpg mode. "
            "Set DATABASE_URL environment variable."
        )

    from .database import AsyncPGClient
    return AsyncPGClient
