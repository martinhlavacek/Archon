"""
Base Search Strategy

Implements the foundational vector similarity search that all other strategies build upon.
This is the core semantic search functionality.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from typing import Any

from ...config.logfire_config import get_logger, safe_span
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)

# Fixed similarity threshold for vector results
SIMILARITY_THRESHOLD = 0.05


class BaseSearchStrategy:
    """Base strategy implementing fundamental vector similarity search"""

    def __init__(self, supabase_client=None):
        """Initialize with optional database client (legacy mode only)"""
        self._supabase_client = supabase_client
        self._mode = get_database_mode()

    @property
    def supabase_client(self):
        """Lazy load Supabase client for legacy mode."""
        if self._mode != "supabase":
            raise ValueError("Supabase client not available in asyncpg mode")
        if self._supabase_client is None:
            from src.server.utils import get_supabase_client
            self._supabase_client = get_supabase_client()
        return self._supabase_client

    async def vector_search(
        self,
        query_embedding: list[float],
        match_count: int,
        filter_metadata: dict | None = None,
        table_rpc: str = "match_archon_crawled_pages",
    ) -> list[dict[str, Any]]:
        """
        Perform basic vector similarity search.

        This is the foundational semantic search that all strategies use.

        Args:
            query_embedding: The embedding vector for the query
            match_count: Number of results to return
            filter_metadata: Optional metadata filters
            table_rpc: The RPC function to call (match_archon_crawled_pages or match_archon_code_examples)

        Returns:
            List of matching documents with similarity scores
        """
        with safe_span("base_vector_search", table=table_rpc, match_count=match_count) as span:
            try:
                if is_asyncpg_mode():
                    results = await self._vector_search_asyncpg(
                        query_embedding, match_count, filter_metadata, table_rpc
                    )
                else:
                    results = self._vector_search_supabase(
                        query_embedding, match_count, filter_metadata, table_rpc
                    )

                # Filter by similarity threshold
                filtered_results = []
                for result in results:
                    similarity = float(result.get("similarity", 0.0))
                    if similarity >= SIMILARITY_THRESHOLD:
                        filtered_results.append(result)

                span.set_attribute("results_found", len(filtered_results))
                span.set_attribute(
                    "results_filtered",
                    len(results) - len(filtered_results) if results else 0,
                )

                return filtered_results

            except Exception as e:
                logger.error(f"Vector search failed: {e}")
                span.set_attribute("error", str(e))
                return []

    async def _vector_search_asyncpg(
        self,
        query_embedding: list[float],
        match_count: int,
        filter_metadata: dict | None,
        table_rpc: str,
    ) -> list[dict[str, Any]]:
        """Perform vector search using asyncpg by calling PostgreSQL function."""
        from ..database import AsyncPGClient

        # Parse filter metadata
        source_filter = None
        filter_json = {}
        if filter_metadata:
            if "source" in filter_metadata:
                source_filter = filter_metadata["source"]
            else:
                filter_json = filter_metadata

        # Call the PostgreSQL function directly
        # The embedding needs to be formatted as a PostgreSQL vector literal
        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"

        rows = await AsyncPGClient.fetch(
            f"""
            SELECT * FROM {table_rpc}(
                query_embedding := $1::vector,
                match_count := $2,
                filter := $3::jsonb,
                source_filter := $4
            )
            """,
            embedding_str, match_count, json.dumps(filter_json), source_filter
        )

        results = []
        for row in rows:
            result = dict(row)
            # Convert UUID to string
            if "id" in result:
                result["id"] = str(result["id"])
            if "source_id" in result:
                result["source_id"] = str(result["source_id"]) if result["source_id"] else None
            results.append(result)

        return results

    def _vector_search_supabase(
        self,
        query_embedding: list[float],
        match_count: int,
        filter_metadata: dict | None,
        table_rpc: str,
    ) -> list[dict[str, Any]]:
        """Perform vector search using Supabase RPC (legacy)."""
        # Build RPC parameters
        rpc_params = {"query_embedding": query_embedding, "match_count": match_count}

        # Add filter parameters
        if filter_metadata:
            if "source" in filter_metadata:
                rpc_params["source_filter"] = filter_metadata["source"]
                rpc_params["filter"] = {}
            else:
                rpc_params["filter"] = filter_metadata
        else:
            rpc_params["filter"] = {}

        # Execute search
        response = self.supabase_client.rpc(table_rpc, rpc_params).execute()

        return response.data if response.data else []
