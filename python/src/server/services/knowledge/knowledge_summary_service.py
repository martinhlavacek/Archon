"""
Knowledge Summary Service

Provides lightweight summary data for knowledge items to minimize data transfer.
Optimized for frequent polling and card displays.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

from typing import Any, Optional

from ...config.logfire_config import safe_logfire_info, safe_logfire_error
from ..client_manager import get_database_mode, is_asyncpg_mode


class KnowledgeSummaryService:
    """
    Service for providing lightweight knowledge item summaries.
    Designed for efficient polling with minimal data transfer.
    """

    def __init__(self, supabase_client=None):
        """
        Initialize the knowledge summary service.

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

    async def get_summaries(
        self,
        page: int = 1,
        per_page: int = 20,
        knowledge_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get lightweight summaries of knowledge items.

        Returns only essential data needed for card displays:
        - Basic metadata (title, url, type, tags)
        - Counts only (no actual content)
        - Minimal processing overhead

        Args:
            page: Page number (1-based)
            per_page: Items per page
            knowledge_type: Optional filter by knowledge type
            search: Optional search term

        Returns:
            Dict with minimal item summaries and pagination info
        """
        try:
            safe_logfire_info(f"Fetching knowledge summaries | page={page} | per_page={per_page}")

            if is_asyncpg_mode():
                return await self._get_summaries_asyncpg(page, per_page, knowledge_type, search)
            else:
                return await self._get_summaries_supabase(page, per_page, knowledge_type, search)

        except Exception as e:
            safe_logfire_error(f"Failed to get knowledge summaries | error={str(e)}")
            raise

    async def _get_summaries_asyncpg(
        self,
        page: int,
        per_page: int,
        knowledge_type: Optional[str],
        search: Optional[str],
    ) -> dict[str, Any]:
        """Get summaries using asyncpg."""
        import json
        from ..database import AsyncPGClient

        # Build WHERE clauses
        where_clauses = []
        params = []
        param_idx = 1

        if knowledge_type:
            where_clauses.append(f"metadata->>'knowledge_type' = ${param_idx}")
            params.append(knowledge_type)
            param_idx += 1

        if search:
            search_pattern = f"%{search}%"
            where_clauses.append(f"(title ILIKE ${param_idx} OR summary ILIKE ${param_idx})")
            params.append(search_pattern)
            param_idx += 1

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        # Get total count
        count_result = await AsyncPGClient.fetchval(
            f"SELECT COUNT(*) FROM archon_sources WHERE {where_sql}",
            *params
        )
        total = count_result or 0

        # Apply pagination
        offset = (page - 1) * per_page
        params_with_pagination = params + [per_page, offset]

        sources = await AsyncPGClient.fetch(
            f"""
            SELECT source_id, title, summary, metadata, source_url, created_at, updated_at
            FROM archon_sources
            WHERE {where_sql}
            ORDER BY updated_at DESC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """,
            *params_with_pagination
        )

        sources_list = [dict(row) for row in sources]
        source_ids = [s["source_id"] for s in sources_list]

        summaries = []
        if source_ids:
            doc_counts = await self._get_document_counts_batch(source_ids)
            code_counts = await self._get_code_example_counts_batch(source_ids)
            first_urls = await self._get_first_urls_batch(source_ids)

            for source in sources_list:
                source_id = source["source_id"]
                metadata = source.get("metadata", {})
                if isinstance(metadata, str):
                    metadata = json.loads(metadata)

                source_url = source.get("source_url")
                first_url = source_url if source_url else first_urls.get(source_id, f"source://{source_id}")
                source_type = metadata.get("source_type", "file" if first_url.startswith("file://") else "url")
                kt = metadata.get("knowledge_type", "technical")

                summary = {
                    "source_id": source_id,
                    "title": source.get("title", source.get("summary", "Untitled")),
                    "url": first_url,
                    "status": "active",
                    "document_count": doc_counts.get(source_id, 0),
                    "code_examples_count": code_counts.get(source_id, 0),
                    "knowledge_type": kt,
                    "source_type": source_type,
                    "created_at": str(source.get("created_at")) if source.get("created_at") else None,
                    "updated_at": str(source.get("updated_at")) if source.get("updated_at") else None,
                    "metadata": metadata,
                }
                summaries.append(summary)

        safe_logfire_info(f"Knowledge summaries fetched | count={len(summaries)} | total={total}")

        return {
            "items": summaries,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page if per_page > 0 else 0,
        }

    async def _get_summaries_supabase(
        self,
        page: int,
        per_page: int,
        knowledge_type: Optional[str],
        search: Optional[str],
    ) -> dict[str, Any]:
        """Get summaries using Supabase (legacy)."""
        # Build base query - select only needed fields, including source_url
        query = self.supabase.from_("archon_sources").select(
            "source_id, title, summary, metadata, source_url, created_at, updated_at"
        )

        # Apply filters
        if knowledge_type:
            query = query.contains("metadata", {"knowledge_type": knowledge_type})

        if search:
            search_pattern = f"%{search}%"
            query = query.or_(
                f"title.ilike.{search_pattern},summary.ilike.{search_pattern}"
            )

        # Get total count
        count_query = self.supabase.from_("archon_sources").select(
            "*", count="exact", head=True
        )

        if knowledge_type:
            count_query = count_query.contains("metadata", {"knowledge_type": knowledge_type})

        if search:
            search_pattern = f"%{search}%"
            count_query = count_query.or_(
                f"title.ilike.{search_pattern},summary.ilike.{search_pattern}"
            )

        count_result = count_query.execute()
        total = count_result.count if hasattr(count_result, "count") else 0

        # Apply pagination
        start_idx = (page - 1) * per_page
        query = query.range(start_idx, start_idx + per_page - 1)
        query = query.order("updated_at", desc=True)

        # Execute main query
        result = query.execute()
        sources = result.data if result.data else []

        # Get source IDs for batch operations
        source_ids = [s["source_id"] for s in sources]

        # Batch fetch counts only (no content!)
        summaries = []

        if source_ids:
            # Get document counts in a single query
            doc_counts = await self._get_document_counts_batch(source_ids)

            # Get code example counts in a single query
            code_counts = await self._get_code_example_counts_batch(source_ids)

            # Get first URLs in a single query
            first_urls = await self._get_first_urls_batch(source_ids)

            # Build summaries
            for source in sources:
                source_id = source["source_id"]
                metadata = source.get("metadata", {})

                # Use the original source_url from the source record (the URL the user entered)
                # Fall back to first crawled page URL, then to source:// format as last resort
                source_url = source.get("source_url")
                if source_url:
                    first_url = source_url
                else:
                    first_url = first_urls.get(source_id, f"source://{source_id}")

                source_type = metadata.get("source_type", "file" if first_url.startswith("file://") else "url")

                # Extract knowledge_type - check metadata first, otherwise default based on source content
                # The metadata should always have it if it was crawled properly
                kt = metadata.get("knowledge_type")
                if not kt:
                    # Fallback: If not in metadata, default to "technical" for now
                    # This handles legacy data that might not have knowledge_type set
                    safe_logfire_info(f"Knowledge type not found in metadata for {source_id}, defaulting to technical")
                    kt = "technical"

                summary = {
                    "source_id": source_id,
                    "title": source.get("title", source.get("summary", "Untitled")),
                    "url": first_url,
                    "status": "active",  # Always active for now
                    "document_count": doc_counts.get(source_id, 0),
                    "code_examples_count": code_counts.get(source_id, 0),
                    "knowledge_type": kt,
                    "source_type": source_type,
                    "created_at": source.get("created_at"),
                    "updated_at": source.get("updated_at"),
                    "metadata": metadata,  # Include full metadata (contains tags)
                }
                summaries.append(summary)

        safe_logfire_info(
            f"Knowledge summaries fetched | count={len(summaries)} | total={total}"
        )

        return {
            "items": summaries,
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page if per_page > 0 else 0,
        }
    
    async def _get_document_counts_batch(self, source_ids: list[str]) -> dict[str, int]:
        """
        Get document counts for multiple sources in a single query.

        Args:
            source_ids: List of source IDs

        Returns:
            Dict mapping source_id to document count
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient
                rows = await AsyncPGClient.fetch(
                    """
                    SELECT source_id, COUNT(*) as count
                    FROM archon_crawled_pages
                    WHERE source_id = ANY($1)
                    GROUP BY source_id
                    """,
                    source_ids
                )
                counts = {row["source_id"]: row["count"] for row in rows}
                # Ensure all source_ids have a count
                for sid in source_ids:
                    if sid not in counts:
                        counts[sid] = 0
                return counts
            else:
                # Use a raw SQL query for efficient counting
                # Group by source_id and count
                counts = {}

                # For now, use individual queries but optimize later with raw SQL
                for source_id in source_ids:
                    result = (
                        self.supabase.from_("archon_crawled_pages")
                        .select("id", count="exact", head=True)
                        .eq("source_id", source_id)
                        .execute()
                    )
                    counts[source_id] = result.count if hasattr(result, "count") else 0

                return counts

        except Exception as e:
            safe_logfire_error(f"Failed to get document counts | error={str(e)}")
            return {sid: 0 for sid in source_ids}
    
    async def _get_code_example_counts_batch(self, source_ids: list[str]) -> dict[str, int]:
        """
        Get code example counts for multiple sources efficiently.

        Args:
            source_ids: List of source IDs

        Returns:
            Dict mapping source_id to code example count
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient
                rows = await AsyncPGClient.fetch(
                    """
                    SELECT source_id, COUNT(*) as count
                    FROM archon_code_examples
                    WHERE source_id = ANY($1)
                    GROUP BY source_id
                    """,
                    source_ids
                )
                counts = {row["source_id"]: row["count"] for row in rows}
                # Ensure all source_ids have a count
                for sid in source_ids:
                    if sid not in counts:
                        counts[sid] = 0
                return counts
            else:
                counts = {}

                # For now, use individual queries but can optimize with raw SQL later
                for source_id in source_ids:
                    result = (
                        self.supabase.from_("archon_code_examples")
                        .select("id", count="exact", head=True)
                        .eq("source_id", source_id)
                        .execute()
                    )
                    counts[source_id] = result.count if hasattr(result, "count") else 0

                return counts

        except Exception as e:
            safe_logfire_error(f"Failed to get code example counts | error={str(e)}")
            return {sid: 0 for sid in source_ids}
    
    async def _get_first_urls_batch(self, source_ids: list[str]) -> dict[str, str]:
        """
        Get first URL for each source in a batch.

        Args:
            source_ids: List of source IDs

        Returns:
            Dict mapping source_id to first URL
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient
                # Use DISTINCT ON to get first URL per source_id
                rows = await AsyncPGClient.fetch(
                    """
                    SELECT DISTINCT ON (source_id) source_id, url
                    FROM archon_crawled_pages
                    WHERE source_id = ANY($1)
                    ORDER BY source_id, created_at ASC
                    """,
                    source_ids
                )
                urls = {row["source_id"]: row["url"] for row in rows}

                # Provide defaults for any missing
                for source_id in source_ids:
                    if source_id not in urls:
                        urls[source_id] = f"source://{source_id}"

                return urls
            else:
                # Get all first URLs in one query
                result = (
                    self.supabase.from_("archon_crawled_pages")
                    .select("source_id, url")
                    .in_("source_id", source_ids)
                    .order("created_at", desc=False)
                    .execute()
                )

                # Group by source_id, keeping first URL for each
                urls = {}
                for item in result.data or []:
                    source_id = item["source_id"]
                    if source_id not in urls:
                        urls[source_id] = item["url"]

                # Provide defaults for any missing
                for source_id in source_ids:
                    if source_id not in urls:
                        urls[source_id] = f"source://{source_id}"

                return urls

        except Exception as e:
            safe_logfire_error(f"Failed to get first URLs | error={str(e)}")
            return {sid: f"source://{sid}" for sid in source_ids}