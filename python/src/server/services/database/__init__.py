"""
Database abstraction layer for Archon.

This module provides a unified interface for database operations,
supporting both asyncpg (PostgreSQL) and Supabase backends.
"""

from .asyncpg_client import AsyncPGClient
from .client import DatabaseClient, get_database_client

__all__ = [
    "AsyncPGClient",
    "DatabaseClient",
    "get_database_client",
]
