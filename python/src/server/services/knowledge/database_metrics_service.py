"""
Database Metrics Service

Handles retrieval of database statistics and metrics.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from datetime import datetime
from typing import Any

from ...config.logfire_config import safe_logfire_error, safe_logfire_info
from ..client_manager import get_database_mode, is_asyncpg_mode


class DatabaseMetricsService:
    """
    Service for retrieving database metrics and statistics.
    """

    def __init__(self, supabase_client=None):
        """
        Initialize the database metrics service.

        Args:
            supabase_client: The Supabase client for database operations (legacy mode only)
        """
        self._supabase_client = supabase_client
        self._mode = get_database_mode()

    @property
    def supabase(self):
        """Lazy load Supabase client for legacy mode."""
        if self._mode != "supabase":
            raise ValueError("Supabase client not available in asyncpg mode")
        if self._supabase_client is None:
            from src.server.utils import get_supabase_client
            self._supabase_client = get_supabase_client()
        return self._supabase_client

    async def get_metrics(self) -> dict[str, Any]:
        """
        Get database metrics and statistics.

        Returns:
            Dictionary containing database metrics
        """
        try:
            safe_logfire_info("Getting database metrics")

            if is_asyncpg_mode():
                return await self._get_metrics_asyncpg()
            else:
                return await self._get_metrics_supabase()

        except Exception as e:
            safe_logfire_error(f"Failed to get database metrics | error={str(e)}")
            raise

    async def _get_metrics_asyncpg(self) -> dict[str, Any]:
        """Get metrics using asyncpg."""
        from ..database import AsyncPGClient

        metrics = {}

        # Sources count
        sources_count = await AsyncPGClient.fetchval(
            "SELECT COUNT(*) FROM archon_sources"
        )
        metrics["sources_count"] = sources_count or 0

        # Crawled pages count
        pages_count = await AsyncPGClient.fetchval(
            "SELECT COUNT(*) FROM archon_crawled_pages"
        )
        metrics["pages_count"] = pages_count or 0

        # Code examples count
        try:
            code_examples_count = await AsyncPGClient.fetchval(
                "SELECT COUNT(*) FROM archon_code_examples"
            )
            metrics["code_examples_count"] = code_examples_count or 0
        except Exception:
            metrics["code_examples_count"] = 0

        # Add timestamp
        metrics["timestamp"] = datetime.now().isoformat()

        # Calculate additional metrics
        metrics["average_pages_per_source"] = (
            round(metrics["pages_count"] / metrics["sources_count"], 2)
            if metrics["sources_count"] > 0
            else 0
        )

        safe_logfire_info(
            f"Database metrics retrieved | sources={metrics['sources_count']} | pages={metrics['pages_count']} | code_examples={metrics['code_examples_count']}"
        )

        return metrics

    async def _get_metrics_supabase(self) -> dict[str, Any]:
        """Get metrics using Supabase (legacy)."""
        metrics = {}

        # Sources count
        sources_result = (
            self.supabase.table("archon_sources").select("*", count="exact").execute()
        )
        metrics["sources_count"] = sources_result.count if sources_result.count else 0

        # Crawled pages count
        pages_result = (
            self.supabase.table("archon_crawled_pages").select("*", count="exact").execute()
        )
        metrics["pages_count"] = pages_result.count if pages_result.count else 0

        # Code examples count
        try:
            code_examples_result = (
                self.supabase.table("archon_code_examples").select("*", count="exact").execute()
            )
            metrics["code_examples_count"] = (
                code_examples_result.count if code_examples_result.count else 0
            )
        except Exception:
            metrics["code_examples_count"] = 0

        # Add timestamp
        metrics["timestamp"] = datetime.now().isoformat()

        # Calculate additional metrics
        metrics["average_pages_per_source"] = (
            round(metrics["pages_count"] / metrics["sources_count"], 2)
            if metrics["sources_count"] > 0
            else 0
        )

        safe_logfire_info(
            f"Database metrics retrieved | sources={metrics['sources_count']} | pages={metrics['pages_count']} | code_examples={metrics['code_examples_count']}"
        )

        return metrics

    async def get_storage_statistics(self) -> dict[str, Any]:
        """
        Get storage statistics including sizes and counts by type.

        Returns:
            Dictionary containing storage statistics
        """
        try:
            if is_asyncpg_mode():
                return await self._get_storage_statistics_asyncpg()
            else:
                return await self._get_storage_statistics_supabase()

        except Exception as e:
            safe_logfire_error(f"Failed to get storage statistics | error={str(e)}")
            return {}

    async def _get_storage_statistics_asyncpg(self) -> dict[str, Any]:
        """Get storage statistics using asyncpg."""
        from ..database import AsyncPGClient

        stats = {}

        # Get knowledge type distribution
        rows = await AsyncPGClient.fetch(
            "SELECT metadata->>'knowledge_type' as knowledge_type FROM archon_sources"
        )

        type_counts = {}
        for row in rows:
            ktype = row.get("knowledge_type", "unknown")
            type_counts[ktype] = type_counts.get(ktype, 0) + 1
        stats["knowledge_type_distribution"] = type_counts

        # Get recent activity
        recent_rows = await AsyncPGClient.fetch(
            """
            SELECT source_id, created_at
            FROM archon_sources
            ORDER BY created_at DESC
            LIMIT 5
            """
        )

        stats["recent_sources"] = [
            {
                "source_id": row["source_id"],
                "created_at": str(row["created_at"]) if row["created_at"] else None
            }
            for row in recent_rows
        ]

        return stats

    async def _get_storage_statistics_supabase(self) -> dict[str, Any]:
        """Get storage statistics using Supabase (legacy)."""
        stats = {}

        # Get knowledge type distribution
        knowledge_types_result = (
            self.supabase.table("archon_sources").select("metadata->knowledge_type").execute()
        )

        if knowledge_types_result.data:
            type_counts = {}
            for row in knowledge_types_result.data:
                ktype = row.get("knowledge_type", "unknown")
                type_counts[ktype] = type_counts.get(ktype, 0) + 1
            stats["knowledge_type_distribution"] = type_counts

        # Get recent activity
        recent_sources = (
            self.supabase.table("archon_sources")
            .select("source_id, created_at")
            .order("created_at", desc=True)
            .limit(5)
            .execute()
        )

        stats["recent_sources"] = [
            {"source_id": s["source_id"], "created_at": s["created_at"]}
            for s in (recent_sources.data or [])
        ]

        return stats
