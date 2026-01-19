"""
Versioning Service Module for Archon

This module provides core business logic for document versioning operations
that can be shared between MCP tools and FastAPI endpoints.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from datetime import datetime
from typing import Any

from ...config.logfire_config import get_logger
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)


class VersioningService:
    """Service class for document versioning operations"""

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

    async def create_version(
        self,
        project_id: str,
        field_name: str,
        content: dict[str, Any] | list[dict[str, Any]],
        change_summary: str | None = None,
        change_type: str = "update",
        document_id: str | None = None,
        created_by: str = "system",
    ) -> tuple[bool, dict[str, Any]]:
        """
        Create a version snapshot for a project JSONB field.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                return await self._create_version_asyncpg(
                    project_id, field_name, content, change_summary,
                    change_type, document_id, created_by
                )
            else:
                return self._create_version_supabase(
                    project_id, field_name, content, change_summary,
                    change_type, document_id, created_by
                )

        except Exception as e:
            logger.error(f"Error creating version: {e}")
            return False, {"error": f"Error creating version: {str(e)}"}

    async def _create_version_asyncpg(
        self, project_id: str, field_name: str, content: Any,
        change_summary: str | None, change_type: str,
        document_id: str | None, created_by: str
    ) -> tuple[bool, dict[str, Any]]:
        """Create version using asyncpg."""
        from ..database import AsyncPGClient

        # Get current highest version number for this project/field
        result = await AsyncPGClient.fetchrow(
            """
            SELECT version_number FROM archon_document_versions
            WHERE project_id = $1 AND field_name = $2
            ORDER BY version_number DESC
            LIMIT 1
            """,
            project_id, field_name
        )

        next_version = 1
        if result:
            next_version = result["version_number"] + 1

        now = datetime.now().isoformat()

        # Create new version record
        version = await AsyncPGClient.fetchrow(
            """
            INSERT INTO archon_document_versions (
                project_id, field_name, version_number, content,
                change_summary, change_type, document_id, created_by, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            """,
            project_id, field_name, next_version,
            json.dumps(content) if isinstance(content, (dict, list)) else content,
            change_summary or f"{change_type.capitalize()} {field_name}",
            change_type, document_id, created_by, now
        )

        if version:
            version_dict = dict(version)
            version_dict["id"] = str(version_dict.get("id", ""))
            return True, {
                "version": version_dict,
                "project_id": project_id,
                "field_name": field_name,
                "version_number": next_version,
            }
        else:
            return False, {"error": "Failed to create version snapshot"}

    def _create_version_supabase(
        self, project_id: str, field_name: str, content: Any,
        change_summary: str | None, change_type: str,
        document_id: str | None, created_by: str
    ) -> tuple[bool, dict[str, Any]]:
        """Create version using Supabase (legacy)."""
        # Get current highest version number for this project/field
        existing_versions = (
            self.supabase_client.table("archon_document_versions")
            .select("version_number")
            .eq("project_id", project_id)
            .eq("field_name", field_name)
            .order("version_number", desc=True)
            .limit(1)
            .execute()
        )

        next_version = 1
        if existing_versions.data:
            next_version = existing_versions.data[0]["version_number"] + 1

        # Create new version record
        version_data = {
            "project_id": project_id,
            "field_name": field_name,
            "version_number": next_version,
            "content": content,
            "change_summary": change_summary or f"{change_type.capitalize()} {field_name}",
            "change_type": change_type,
            "document_id": document_id,
            "created_by": created_by,
            "created_at": datetime.now().isoformat(),
        }

        result = (
            self.supabase_client.table("archon_document_versions")
            .insert(version_data)
            .execute()
        )

        if result.data:
            return True, {
                "version": result.data[0],
                "project_id": project_id,
                "field_name": field_name,
                "version_number": next_version,
            }
        else:
            return False, {"error": "Failed to create version snapshot"}

    async def list_versions(
        self, project_id: str, field_name: str | None = None
    ) -> tuple[bool, dict[str, Any]]:
        """
        Get version history for project JSONB fields.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                if field_name:
                    rows = await AsyncPGClient.fetch(
                        """
                        SELECT * FROM archon_document_versions
                        WHERE project_id = $1 AND field_name = $2
                        ORDER BY version_number DESC
                        """,
                        project_id, field_name
                    )
                else:
                    rows = await AsyncPGClient.fetch(
                        """
                        SELECT * FROM archon_document_versions
                        WHERE project_id = $1
                        ORDER BY version_number DESC
                        """,
                        project_id
                    )

                versions = []
                for row in rows:
                    version = dict(row)
                    version["id"] = str(version.get("id", ""))
                    if hasattr(version.get("created_at"), 'isoformat'):
                        version["created_at"] = version["created_at"].isoformat()
                    versions.append(version)

                return True, {
                    "project_id": project_id,
                    "field_name": field_name,
                    "versions": versions,
                    "total_count": len(versions),
                }
            else:
                # Build query
                query = (
                    self.supabase_client.table("archon_document_versions")
                    .select("*")
                    .eq("project_id", project_id)
                )

                if field_name:
                    query = query.eq("field_name", field_name)

                result = query.order("version_number", desc=True).execute()

                if result.data is not None:
                    return True, {
                        "project_id": project_id,
                        "field_name": field_name,
                        "versions": result.data,
                        "total_count": len(result.data),
                    }
                else:
                    return False, {"error": "Failed to retrieve version history"}

        except Exception as e:
            logger.error(f"Error getting version history: {e}")
            return False, {"error": f"Error getting version history: {str(e)}"}

    async def get_version_content(
        self, project_id: str, field_name: str, version_number: int
    ) -> tuple[bool, dict[str, Any]]:
        """
        Get the content of a specific version.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                version = await AsyncPGClient.fetchrow(
                    """
                    SELECT * FROM archon_document_versions
                    WHERE project_id = $1 AND field_name = $2 AND version_number = $3
                    """,
                    project_id, field_name, version_number
                )

                if version:
                    version_dict = dict(version)
                    content = version_dict.get("content", {})
                    if isinstance(content, str):
                        content = json.loads(content)
                    return True, {
                        "version": version_dict,
                        "content": content,
                        "field_name": field_name,
                        "version_number": version_number,
                    }
                else:
                    return False, {"error": f"Version {version_number} not found for {field_name}"}
            else:
                result = (
                    self.supabase_client.table("archon_document_versions")
                    .select("*")
                    .eq("project_id", project_id)
                    .eq("field_name", field_name)
                    .eq("version_number", version_number)
                    .execute()
                )

                if result.data:
                    version = result.data[0]
                    return True, {
                        "version": version,
                        "content": version["content"],
                        "field_name": field_name,
                        "version_number": version_number,
                    }
                else:
                    return False, {"error": f"Version {version_number} not found for {field_name}"}

        except Exception as e:
            logger.error(f"Error getting version content: {e}")
            return False, {"error": f"Error getting version content: {str(e)}"}

    async def restore_version(
        self, project_id: str, field_name: str, version_number: int, restored_by: str = "system"
    ) -> tuple[bool, dict[str, Any]]:
        """
        Restore a project JSONB field to a specific version.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                return await self._restore_version_asyncpg(
                    project_id, field_name, version_number, restored_by
                )
            else:
                return self._restore_version_supabase(
                    project_id, field_name, version_number, restored_by
                )

        except Exception as e:
            logger.error(f"Error restoring version: {e}")
            return False, {"error": f"Error restoring version: {str(e)}"}

    async def _restore_version_asyncpg(
        self, project_id: str, field_name: str, version_number: int, restored_by: str
    ) -> tuple[bool, dict[str, Any]]:
        """Restore version using asyncpg."""
        from ..database import AsyncPGClient

        # Get the version to restore
        version_to_restore = await AsyncPGClient.fetchrow(
            """
            SELECT * FROM archon_document_versions
            WHERE project_id = $1 AND field_name = $2 AND version_number = $3
            """,
            project_id, field_name, version_number
        )

        if not version_to_restore:
            return False, {
                "error": f"Version {version_number} not found for {field_name} in project {project_id}"
            }

        content_to_restore = version_to_restore["content"]
        if isinstance(content_to_restore, str):
            content_to_restore = json.loads(content_to_restore)

        # Get current content to create backup
        current_project = await AsyncPGClient.fetchrow(
            f"SELECT {field_name} FROM archon_projects WHERE id = $1",
            project_id
        )

        if current_project:
            current_content = current_project.get(field_name, {})
            if isinstance(current_content, str):
                current_content = json.loads(current_content)

            # Create backup version before restore
            backup_result = await self.create_version(
                project_id=project_id,
                field_name=field_name,
                content=current_content,
                change_summary=f"Backup before restoring to version {version_number}",
                change_type="backup",
                created_by=restored_by,
            )

            if not backup_result[0]:
                logger.warning(f"Failed to create backup version: {backup_result[1]}")

        now = datetime.now().isoformat()

        # Restore the content to project
        await AsyncPGClient.execute(
            f"""
            UPDATE archon_projects
            SET {field_name} = $1, updated_at = $2
            WHERE id = $3
            """,
            json.dumps(content_to_restore),
            now,
            project_id
        )

        # Create restore version record
        await self.create_version(
            project_id=project_id,
            field_name=field_name,
            content=content_to_restore,
            change_summary=f"Restored to version {version_number}",
            change_type="restore",
            created_by=restored_by,
        )

        return True, {
            "project_id": project_id,
            "field_name": field_name,
            "restored_version": version_number,
            "restored_by": restored_by,
        }

    def _restore_version_supabase(
        self, project_id: str, field_name: str, version_number: int, restored_by: str
    ) -> tuple[bool, dict[str, Any]]:
        """Restore version using Supabase (legacy)."""
        # Get the version to restore
        version_result = (
            self.supabase_client.table("archon_document_versions")
            .select("*")
            .eq("project_id", project_id)
            .eq("field_name", field_name)
            .eq("version_number", version_number)
            .execute()
        )

        if not version_result.data:
            return False, {
                "error": f"Version {version_number} not found for {field_name} in project {project_id}"
            }

        version_to_restore = version_result.data[0]
        content_to_restore = version_to_restore["content"]

        # Get current content to create backup
        current_project = (
            self.supabase_client.table("archon_projects")
            .select(field_name)
            .eq("id", project_id)
            .execute()
        )
        if current_project.data:
            current_content = current_project.data[0].get(field_name, {})

            # Create backup version before restore
            backup_result = self._create_version_supabase(
                project_id=project_id,
                field_name=field_name,
                content=current_content,
                change_summary=f"Backup before restoring to version {version_number}",
                change_type="backup",
                document_id=None,
                created_by=restored_by,
            )

            if not backup_result[0]:
                logger.warning(f"Failed to create backup version: {backup_result[1]}")

        # Restore the content to project
        update_data = {field_name: content_to_restore, "updated_at": datetime.now().isoformat()}

        restore_result = (
            self.supabase_client.table("archon_projects")
            .update(update_data)
            .eq("id", project_id)
            .execute()
        )

        if restore_result.data:
            # Create restore version record
            self._create_version_supabase(
                project_id=project_id,
                field_name=field_name,
                content=content_to_restore,
                change_summary=f"Restored to version {version_number}",
                change_type="restore",
                document_id=None,
                created_by=restored_by,
            )

            return True, {
                "project_id": project_id,
                "field_name": field_name,
                "restored_version": version_number,
                "restored_by": restored_by,
            }
        else:
            return False, {"error": "Failed to restore version"}
