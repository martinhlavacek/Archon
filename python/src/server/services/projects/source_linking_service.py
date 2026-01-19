"""
Source Linking Service Module for Archon

This module provides centralized logic for managing project-source relationships,
handling both technical and business source associations.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

from typing import Any

from ...config.logfire_config import get_logger
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)


class SourceLinkingService:
    """Service class for managing project-source relationships"""

    def __init__(self, supabase_client=None):
        """Initialize with optional supabase client (legacy mode only)"""
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

    async def get_project_sources(self, project_id: str) -> tuple[bool, dict[str, list[str]]]:
        """
        Get all linked sources for a project, separated by type.

        Returns:
            Tuple of (success, {"technical_sources": [...], "business_sources": [...]})
        """
        try:
            if is_asyncpg_mode():
                return await self._get_project_sources_asyncpg(project_id)
            else:
                return self._get_project_sources_supabase(project_id)
        except Exception as e:
            logger.error(f"Error getting project sources: {e}")
            return False, {
                "error": f"Failed to retrieve linked sources: {str(e)}",
                "technical_sources": [],
                "business_sources": [],
            }

    async def _get_project_sources_asyncpg(self, project_id: str) -> tuple[bool, dict[str, list[str]]]:
        """Get project sources using asyncpg."""
        from ..database import AsyncPGClient

        rows = await AsyncPGClient.fetch(
            """
            SELECT source_id, notes FROM archon_project_sources
            WHERE project_id = $1
            """,
            project_id
        )

        technical_sources = []
        business_sources = []

        for row in rows:
            if row.get("notes") == "technical":
                technical_sources.append(str(row["source_id"]))
            elif row.get("notes") == "business":
                business_sources.append(str(row["source_id"]))

        return True, {
            "technical_sources": technical_sources,
            "business_sources": business_sources,
        }

    def _get_project_sources_supabase(self, project_id: str) -> tuple[bool, dict[str, list[str]]]:
        """Get project sources using Supabase (legacy)."""
        response = (
            self.supabase_client.table("archon_project_sources")
            .select("source_id, notes")
            .eq("project_id", project_id)
            .execute()
        )

        technical_sources = []
        business_sources = []

        for source_link in response.data:
            if source_link.get("notes") == "technical":
                technical_sources.append(source_link["source_id"])
            elif source_link.get("notes") == "business":
                business_sources.append(source_link["source_id"])

        return True, {
            "technical_sources": technical_sources,
            "business_sources": business_sources,
        }

    async def update_project_sources(
        self,
        project_id: str,
        technical_sources: list[str] | None = None,
        business_sources: list[str] | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Update project sources, replacing existing ones if provided.

        Returns:
            Tuple of (success, result_dict with counts)
        """
        result = {
            "technical_success": 0,
            "technical_failed": 0,
            "business_success": 0,
            "business_failed": 0,
        }

        try:
            if is_asyncpg_mode():
                return await self._update_project_sources_asyncpg(
                    project_id, technical_sources, business_sources, result
                )
            else:
                return self._update_project_sources_supabase(
                    project_id, technical_sources, business_sources, result
                )
        except Exception as e:
            logger.error(f"Error updating project sources: {e}")
            return False, {"error": str(e), **result}

    async def _update_project_sources_asyncpg(
        self, project_id: str,
        technical_sources: list[str] | None,
        business_sources: list[str] | None,
        result: dict[str, int]
    ) -> tuple[bool, dict[str, Any]]:
        """Update project sources using asyncpg."""
        from ..database import AsyncPGClient

        # Update technical sources if provided
        if technical_sources is not None:
            # Remove existing technical sources
            await AsyncPGClient.execute(
                """
                DELETE FROM archon_project_sources
                WHERE project_id = $1 AND notes = 'technical'
                """,
                project_id
            )

            # Add new technical sources
            for source_id in technical_sources:
                try:
                    await AsyncPGClient.execute(
                        """
                        INSERT INTO archon_project_sources (project_id, source_id, notes)
                        VALUES ($1, $2, 'technical')
                        """,
                        project_id, source_id
                    )
                    result["technical_success"] += 1
                except Exception as e:
                    result["technical_failed"] += 1
                    logger.warning(f"Failed to link technical source {source_id}: {e}")

        # Update business sources if provided
        if business_sources is not None:
            # Remove existing business sources
            await AsyncPGClient.execute(
                """
                DELETE FROM archon_project_sources
                WHERE project_id = $1 AND notes = 'business'
                """,
                project_id
            )

            # Add new business sources
            for source_id in business_sources:
                try:
                    await AsyncPGClient.execute(
                        """
                        INSERT INTO archon_project_sources (project_id, source_id, notes)
                        VALUES ($1, $2, 'business')
                        """,
                        project_id, source_id
                    )
                    result["business_success"] += 1
                except Exception as e:
                    result["business_failed"] += 1
                    logger.warning(f"Failed to link business source {source_id}: {e}")

        return True, result

    def _update_project_sources_supabase(
        self, project_id: str,
        technical_sources: list[str] | None,
        business_sources: list[str] | None,
        result: dict[str, int]
    ) -> tuple[bool, dict[str, Any]]:
        """Update project sources using Supabase (legacy)."""
        # Update technical sources if provided
        if technical_sources is not None:
            # Remove existing technical sources
            self.supabase_client.table("archon_project_sources").delete().eq(
                "project_id", project_id
            ).eq("notes", "technical").execute()

            # Add new technical sources
            for source_id in technical_sources:
                try:
                    self.supabase_client.table("archon_project_sources").insert({
                        "project_id": project_id,
                        "source_id": source_id,
                        "notes": "technical",
                    }).execute()
                    result["technical_success"] += 1
                except Exception as e:
                    result["technical_failed"] += 1
                    logger.warning(f"Failed to link technical source {source_id}: {e}")

        # Update business sources if provided
        if business_sources is not None:
            # Remove existing business sources
            self.supabase_client.table("archon_project_sources").delete().eq(
                "project_id", project_id
            ).eq("notes", "business").execute()

            # Add new business sources
            for source_id in business_sources:
                try:
                    self.supabase_client.table("archon_project_sources").insert({
                        "project_id": project_id,
                        "source_id": source_id,
                        "notes": "business",
                    }).execute()
                    result["business_success"] += 1
                except Exception as e:
                    result["business_failed"] += 1
                    logger.warning(f"Failed to link business source {source_id}: {e}")

        return True, result

    async def format_project_with_sources(self, project: dict[str, Any]) -> dict[str, Any]:
        """
        Format a project dict with its linked sources included.
        Also handles datetime conversion for JSON compatibility.

        Returns:
            Formatted project dict with technical_sources and business_sources
        """
        # Get linked sources
        success, sources = await self.get_project_sources(project["id"])
        if not success:
            logger.warning(f"Failed to get sources for project {project['id']}")
            sources = {"technical_sources": [], "business_sources": []}

        # Ensure datetime objects are converted to strings
        created_at = project.get("created_at", "")
        updated_at = project.get("updated_at", "")
        if hasattr(created_at, "isoformat"):
            created_at = created_at.isoformat()
        if hasattr(updated_at, "isoformat"):
            updated_at = updated_at.isoformat()

        return {
            "id": str(project["id"]),
            "title": project["title"],
            "description": project.get("description", ""),
            "github_repo": project.get("github_repo"),
            "created_at": created_at,
            "updated_at": updated_at,
            "docs": project.get("docs", []),
            "features": project.get("features", []),
            "data": project.get("data", []),
            "technical_sources": sources["technical_sources"],
            "business_sources": sources["business_sources"],
            "pinned": project.get("pinned", False),
        }

    async def format_projects_with_sources(self, projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Format a list of projects with their linked sources.

        Returns:
            List of formatted project dicts
        """
        formatted_projects = []
        for project in projects:
            formatted_projects.append(await self.format_project_with_sources(project))
        return formatted_projects
