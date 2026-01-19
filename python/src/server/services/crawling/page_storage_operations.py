"""
Page Storage Operations

Handles the storage of complete documentation pages in the archon_page_metadata table.
Pages are stored BEFORE chunking to maintain full context for agent retrieval.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from typing import Any

from postgrest.exceptions import APIError

from ...config.logfire_config import get_logger, safe_logfire_error, safe_logfire_info
from ..client_manager import get_database_mode, is_asyncpg_mode
from .helpers.llms_full_parser import parse_llms_full_sections

logger = get_logger(__name__)


class PageStorageOperations:
    """
    Handles page storage operations for crawled content.

    Pages are stored in the archon_page_metadata table with full content and metadata.
    This enables agents to retrieve complete documentation pages instead of just chunks.
    """

    def __init__(self, supabase_client=None):
        """
        Initialize page storage operations.

        Args:
            supabase_client: The Supabase client for database operations (legacy mode only)
        """
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

    async def store_pages(
        self,
        crawl_results: list[dict],
        source_id: str,
        request: dict[str, Any],
        crawl_type: str,
    ) -> dict[str, str]:
        """
        Store pages in archon_page_metadata table from regular crawl results.

        Args:
            crawl_results: List of crawled documents with url, markdown, title, etc.
            source_id: The source ID these pages belong to
            request: The original crawl request with knowledge_type, tags, etc.
            crawl_type: Type of crawl performed (sitemap, url, link_collection, etc.)

        Returns:
            {url: page_id} mapping for FK references in chunks
        """
        safe_logfire_info(
            f"store_pages called | source_id={source_id} | crawl_type={crawl_type} | num_results={len(crawl_results)}"
        )

        if is_asyncpg_mode():
            return await self._store_pages_asyncpg(crawl_results, source_id, request, crawl_type)
        else:
            return await self._store_pages_supabase(crawl_results, source_id, request, crawl_type)

    async def _store_pages_asyncpg(
        self,
        crawl_results: list[dict],
        source_id: str,
        request: dict[str, Any],
        crawl_type: str,
    ) -> dict[str, str]:
        """Store pages using asyncpg."""
        from ..database import AsyncPGClient

        url_to_page_id: dict[str, str] = {}
        pages_to_insert: list[dict[str, Any]] = []

        for doc in crawl_results:
            url = doc.get("url", "").strip()
            markdown = doc.get("markdown", "").strip()

            if not url or not markdown:
                continue

            word_count = len(markdown.split())
            char_count = len(markdown)

            page_record = {
                "source_id": source_id,
                "url": url,
                "full_content": markdown,
                "section_title": None,
                "section_order": 0,
                "word_count": word_count,
                "char_count": char_count,
                "chunk_count": 0,
                "metadata": {
                    "knowledge_type": request.get("knowledge_type", "documentation"),
                    "crawl_type": crawl_type,
                    "page_type": "documentation",
                    "tags": request.get("tags", []),
                },
            }
            pages_to_insert.append(page_record)

        if pages_to_insert:
            try:
                safe_logfire_info(
                    f"Upserting {len(pages_to_insert)} pages into archon_page_metadata table"
                )

                for page in pages_to_insert:
                    # Use INSERT ... ON CONFLICT for upsert
                    row = await AsyncPGClient.fetchrow(
                        """
                        INSERT INTO archon_page_metadata
                        (source_id, url, full_content, section_title, section_order,
                         word_count, char_count, chunk_count, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                        ON CONFLICT (url) DO UPDATE SET
                            source_id = EXCLUDED.source_id,
                            full_content = EXCLUDED.full_content,
                            section_title = EXCLUDED.section_title,
                            section_order = EXCLUDED.section_order,
                            word_count = EXCLUDED.word_count,
                            char_count = EXCLUDED.char_count,
                            chunk_count = EXCLUDED.chunk_count,
                            metadata = EXCLUDED.metadata,
                            updated_at = NOW()
                        RETURNING id, url
                        """,
                        page["source_id"],
                        page["url"],
                        page["full_content"],
                        page["section_title"],
                        page["section_order"],
                        page["word_count"],
                        page["char_count"],
                        page["chunk_count"],
                        json.dumps(page["metadata"]),
                    )
                    if row:
                        url_to_page_id[row["url"]] = str(row["id"])

                safe_logfire_info(
                    f"Successfully stored {len(url_to_page_id)}/{len(pages_to_insert)} pages in archon_page_metadata"
                )

            except Exception as e:
                safe_logfire_error(
                    f"Database error upserting pages | source_id={source_id} | attempted={len(pages_to_insert)} | error={str(e)}"
                )
                logger.error(f"Failed to upsert pages for source {source_id}: {e}", exc_info=True)

        return url_to_page_id

    async def _store_pages_supabase(
        self,
        crawl_results: list[dict],
        source_id: str,
        request: dict[str, Any],
        crawl_type: str,
    ) -> dict[str, str]:
        """Store pages using Supabase (legacy)."""
        url_to_page_id: dict[str, str] = {}
        pages_to_insert: list[dict[str, Any]] = []

        for doc in crawl_results:
            url = doc.get("url", "").strip()
            markdown = doc.get("markdown", "").strip()

            # Skip documents with empty content or missing URLs
            if not url or not markdown:
                continue

            # Prepare page record
            word_count = len(markdown.split())
            char_count = len(markdown)

            page_record = {
                "source_id": source_id,
                "url": url,
                "full_content": markdown,
                "section_title": None,  # Regular page, not a section
                "section_order": 0,
                "word_count": word_count,
                "char_count": char_count,
                "chunk_count": 0,  # Will be updated after chunking
                "metadata": {
                    "knowledge_type": request.get("knowledge_type", "documentation"),
                    "crawl_type": crawl_type,
                    "page_type": "documentation",
                    "tags": request.get("tags", []),
                },
            }
            pages_to_insert.append(page_record)

        # Batch upsert pages
        if pages_to_insert:
            try:
                safe_logfire_info(
                    f"Upserting {len(pages_to_insert)} pages into archon_page_metadata table"
                )
                result = (
                    self.supabase_client.table("archon_page_metadata")
                    .upsert(pages_to_insert, on_conflict="url")
                    .execute()
                )

                # Build url → page_id mapping
                for page in result.data:
                    url_to_page_id[page["url"]] = page["id"]

                safe_logfire_info(
                    f"Successfully stored {len(url_to_page_id)}/{len(pages_to_insert)} pages in archon_page_metadata"
                )

            except APIError as e:
                safe_logfire_error(
                    f"Database error upserting pages | source_id={source_id} | attempted={len(pages_to_insert)} | error={str(e)}"
                )
                logger.error(f"Failed to upsert pages for source {source_id}: {e}", exc_info=True)
                # Don't raise - allow chunking to continue even if page storage fails

            except Exception as e:
                safe_logfire_error(
                    f"Unexpected error upserting pages | source_id={source_id} | attempted={len(pages_to_insert)} | error={str(e)}"
                )
                logger.error(f"Unexpected error upserting pages for source {source_id}: {e}", exc_info=True)
                # Don't raise - allow chunking to continue

        return url_to_page_id

    async def store_llms_full_sections(
        self,
        base_url: str,
        content: str,
        source_id: str,
        request: dict[str, Any],
        crawl_type: str = "llms_full",
    ) -> dict[str, str]:
        """
        Store llms-full.txt sections as separate pages.

        Each H1 section gets its own page record with a synthetic URL.

        Args:
            base_url: Base URL of the llms-full.txt file
            content: Full text content of the file
            source_id: The source ID these sections belong to
            request: The original crawl request
            crawl_type: Type of crawl (defaults to "llms_full")

        Returns:
            {url: page_id} mapping for FK references in chunks
        """
        # Parse sections from content
        sections = parse_llms_full_sections(content, base_url)

        if not sections:
            logger.warning(f"No sections found in llms-full.txt file: {base_url}")
            return {}

        safe_logfire_info(
            f"Parsed {len(sections)} sections from llms-full.txt file: {base_url}"
        )

        if is_asyncpg_mode():
            return await self._store_llms_full_sections_asyncpg(
                sections, source_id, request, crawl_type, base_url
            )
        else:
            return await self._store_llms_full_sections_supabase(
                sections, source_id, request, crawl_type, base_url
            )

    async def _store_llms_full_sections_asyncpg(
        self,
        sections: list,
        source_id: str,
        request: dict[str, Any],
        crawl_type: str,
        base_url: str,
    ) -> dict[str, str]:
        """Store llms-full sections using asyncpg."""
        from ..database import AsyncPGClient

        url_to_page_id: dict[str, str] = {}

        try:
            safe_logfire_info(
                f"Upserting {len(sections)} section pages into archon_page_metadata"
            )

            for section in sections:
                metadata = {
                    "knowledge_type": request.get("knowledge_type", "documentation"),
                    "crawl_type": crawl_type,
                    "page_type": "llms_full_section",
                    "tags": request.get("tags", []),
                    "section_metadata": {
                        "section_title": section.section_title,
                        "section_order": section.section_order,
                        "base_url": base_url,
                    },
                }

                row = await AsyncPGClient.fetchrow(
                    """
                    INSERT INTO archon_page_metadata
                    (source_id, url, full_content, section_title, section_order,
                     word_count, char_count, chunk_count, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                    ON CONFLICT (url) DO UPDATE SET
                        source_id = EXCLUDED.source_id,
                        full_content = EXCLUDED.full_content,
                        section_title = EXCLUDED.section_title,
                        section_order = EXCLUDED.section_order,
                        word_count = EXCLUDED.word_count,
                        char_count = EXCLUDED.char_count,
                        chunk_count = EXCLUDED.chunk_count,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    RETURNING id, url
                    """,
                    source_id,
                    section.url,
                    section.content,
                    section.section_title,
                    section.section_order,
                    section.word_count,
                    len(section.content),
                    0,  # chunk_count
                    json.dumps(metadata),
                )
                if row:
                    url_to_page_id[row["url"]] = str(row["id"])

            safe_logfire_info(
                f"Successfully stored {len(url_to_page_id)}/{len(sections)} section pages"
            )

        except Exception as e:
            safe_logfire_error(
                f"Database error upserting sections | base_url={base_url} | attempted={len(sections)} | error={str(e)}"
            )
            logger.error(f"Failed to upsert sections for {base_url}: {e}", exc_info=True)

        return url_to_page_id

    async def _store_llms_full_sections_supabase(
        self,
        sections: list,
        source_id: str,
        request: dict[str, Any],
        crawl_type: str,
        base_url: str,
    ) -> dict[str, str]:
        """Store llms-full sections using Supabase (legacy)."""
        url_to_page_id: dict[str, str] = {}

        # Prepare page records for each section
        pages_to_insert: list[dict[str, Any]] = []

        for section in sections:
            page_record = {
                "source_id": source_id,
                "url": section.url,
                "full_content": section.content,
                "section_title": section.section_title,
                "section_order": section.section_order,
                "word_count": section.word_count,
                "char_count": len(section.content),
                "chunk_count": 0,  # Will be updated after chunking
                "metadata": {
                    "knowledge_type": request.get("knowledge_type", "documentation"),
                    "crawl_type": crawl_type,
                    "page_type": "llms_full_section",
                    "tags": request.get("tags", []),
                    "section_metadata": {
                        "section_title": section.section_title,
                        "section_order": section.section_order,
                        "base_url": base_url,
                    },
                },
            }
            pages_to_insert.append(page_record)

        # Batch upsert pages
        if pages_to_insert:
            try:
                safe_logfire_info(
                    f"Upserting {len(pages_to_insert)} section pages into archon_page_metadata"
                )
                result = (
                    self.supabase_client.table("archon_page_metadata")
                    .upsert(pages_to_insert, on_conflict="url")
                    .execute()
                )

                # Build url → page_id mapping
                for page in result.data:
                    url_to_page_id[page["url"]] = page["id"]

                safe_logfire_info(
                    f"Successfully stored {len(url_to_page_id)}/{len(pages_to_insert)} section pages"
                )

            except APIError as e:
                safe_logfire_error(
                    f"Database error upserting sections | base_url={base_url} | attempted={len(pages_to_insert)} | error={str(e)}"
                )
                logger.error(f"Failed to upsert sections for {base_url}: {e}", exc_info=True)
                # Don't raise - allow process to continue

            except Exception as e:
                safe_logfire_error(
                    f"Unexpected error upserting sections | base_url={base_url} | attempted={len(pages_to_insert)} | error={str(e)}"
                )
                logger.error(f"Unexpected error upserting sections for {base_url}: {e}", exc_info=True)
                # Don't raise - allow process to continue

        return url_to_page_id

    async def update_page_chunk_count(self, page_id: str, chunk_count: int) -> None:
        """
        Update the chunk_count field for a page after chunking is complete.

        Args:
            page_id: The UUID of the page to update
            chunk_count: Number of chunks created from this page
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient
                await AsyncPGClient.execute(
                    "UPDATE archon_page_metadata SET chunk_count = $1, updated_at = NOW() WHERE id = $2",
                    chunk_count,
                    page_id,
                )
            else:
                self.supabase_client.table("archon_page_metadata").update(
                    {"chunk_count": chunk_count}
                ).eq("id", page_id).execute()

            safe_logfire_info(f"Updated chunk_count={chunk_count} for page_id={page_id}")

        except APIError as e:
            logger.warning(
                f"Database error updating chunk_count for page {page_id}: {e}", exc_info=True
            )
        except Exception as e:
            logger.warning(
                f"Unexpected error updating chunk_count for page {page_id}: {e}", exc_info=True
            )
