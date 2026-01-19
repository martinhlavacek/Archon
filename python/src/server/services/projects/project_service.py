"""
Project Service Module for Archon

This module provides core business logic for project operations that can be
shared between MCP tools and FastAPI endpoints. It follows the pattern of
separating business logic from transport-specific code.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from datetime import datetime
from typing import Any

from ...config.logfire_config import get_logger
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)


class ProjectService:
    """Service class for project operations"""

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

    async def create_project(self, title: str, github_repo: str | None = None) -> tuple[bool, dict[str, Any]]:
        """
        Create a new project with optional PRD and GitHub repo.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            # Validate inputs
            if not title or not isinstance(title, str) or len(title.strip()) == 0:
                return False, {"error": "Project title is required and must be a non-empty string"}

            now = datetime.now().isoformat()

            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                # Insert project
                project = await AsyncPGClient.fetchrow(
                    """
                    INSERT INTO archon_projects (title, github_repo, docs, features, data, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING *
                    """,
                    title.strip(),
                    github_repo.strip() if github_repo else None,
                    json.dumps([]),  # docs
                    json.dumps([]),  # features
                    json.dumps([]),  # data
                    now,
                    now
                )

                if not project:
                    logger.error("Database returned empty data for project creation")
                    return False, {"error": "Failed to create project - database returned no data"}

                project_id = str(project["id"])
                logger.info(f"Project created successfully with ID: {project_id}")

                return True, {
                    "project": {
                        "id": project_id,
                        "title": project["title"],
                        "github_repo": project.get("github_repo"),
                        "created_at": project["created_at"].isoformat() if hasattr(project["created_at"], 'isoformat') else project["created_at"],
                    }
                }
            else:
                # Supabase mode
                project_data = {
                    "title": title.strip(),
                    "docs": [],
                    "features": [],
                    "data": [],
                    "created_at": now,
                    "updated_at": now,
                }

                if github_repo and isinstance(github_repo, str) and len(github_repo.strip()) > 0:
                    project_data["github_repo"] = github_repo.strip()

                response = self.supabase_client.table("archon_projects").insert(project_data).execute()

                if not response.data:
                    logger.error("Supabase returned empty data for project creation")
                    return False, {"error": "Failed to create project - database returned no data"}

                project = response.data[0]
                project_id = project["id"]
                logger.info(f"Project created successfully with ID: {project_id}")

                return True, {
                    "project": {
                        "id": project_id,
                        "title": project["title"],
                        "github_repo": project.get("github_repo"),
                        "created_at": project["created_at"],
                    }
                }

        except Exception as e:
            logger.error(f"Error creating project: {e}")
            return False, {"error": f"Database error: {str(e)}"}

    async def list_projects(self, include_content: bool = True) -> tuple[bool, dict[str, Any]]:
        """
        List all projects.

        Args:
            include_content: If True (default), includes docs, features, data fields.
                           If False, returns lightweight metadata only with counts.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                return await self._list_projects_asyncpg(include_content)
            else:
                return self._list_projects_supabase(include_content)

        except Exception as e:
            logger.error(f"Error listing projects: {e}")
            return False, {"error": f"Error listing projects: {str(e)}"}

    async def _list_projects_asyncpg(self, include_content: bool) -> tuple[bool, dict[str, Any]]:
        """List projects using asyncpg."""
        from ..database import AsyncPGClient

        rows = await AsyncPGClient.fetch(
            "SELECT * FROM archon_projects ORDER BY created_at DESC"
        )

        projects = []
        for project in rows:
            # Parse JSONB fields
            docs = project.get("docs", [])
            features = project.get("features", [])
            data = project.get("data", [])

            if isinstance(docs, str):
                docs = json.loads(docs)
            if isinstance(features, str):
                features = json.loads(features)
            if isinstance(data, str):
                data = json.loads(data)

            project_data = {
                "id": str(project["id"]),
                "title": project["title"],
                "github_repo": project.get("github_repo"),
                "created_at": project["created_at"].isoformat() if hasattr(project["created_at"], 'isoformat') else project["created_at"],
                "updated_at": project["updated_at"].isoformat() if hasattr(project["updated_at"], 'isoformat') else project["updated_at"],
                "pinned": project.get("pinned", False),
                "description": project.get("description", ""),
            }

            if include_content:
                project_data["docs"] = docs
                project_data["features"] = features
                project_data["data"] = data
            else:
                project_data["stats"] = {
                    "docs_count": len(docs) if docs else 0,
                    "features_count": len(features) if features else 0,
                    "has_data": bool(data)
                }

            projects.append(project_data)

        return True, {"projects": projects, "total_count": len(projects)}

    def _list_projects_supabase(self, include_content: bool) -> tuple[bool, dict[str, Any]]:
        """List projects using Supabase (legacy)."""
        response = (
            self.supabase_client.table("archon_projects")
            .select("*")
            .order("created_at", desc=True)
            .execute()
        )

        projects = []
        for project in response.data:
            project_data = {
                "id": project["id"],
                "title": project["title"],
                "github_repo": project.get("github_repo"),
                "created_at": project["created_at"],
                "updated_at": project["updated_at"],
                "pinned": project.get("pinned", False),
                "description": project.get("description", ""),
            }

            if include_content:
                project_data["docs"] = project.get("docs", [])
                project_data["features"] = project.get("features", [])
                project_data["data"] = project.get("data", [])
            else:
                docs_count = len(project.get("docs", []))
                features_count = len(project.get("features", []))
                has_data = bool(project.get("data", []))
                project_data["stats"] = {
                    "docs_count": docs_count,
                    "features_count": features_count,
                    "has_data": has_data
                }

            projects.append(project_data)

        return True, {"projects": projects, "total_count": len(projects)}

    async def get_project(self, project_id: str) -> tuple[bool, dict[str, Any]]:
        """
        Get a specific project by ID.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                return await self._get_project_asyncpg(project_id)
            else:
                return self._get_project_supabase(project_id)

        except Exception as e:
            logger.error(f"Error getting project: {e}")
            return False, {"error": f"Error getting project: {str(e)}"}

    async def _get_project_asyncpg(self, project_id: str) -> tuple[bool, dict[str, Any]]:
        """Get project using asyncpg."""
        from ..database import AsyncPGClient

        project = await AsyncPGClient.fetchrow(
            "SELECT * FROM archon_projects WHERE id = $1",
            project_id
        )

        if not project:
            return False, {"error": f"Project with ID {project_id} not found"}

        project_dict = dict(project)
        project_dict["id"] = str(project_dict["id"])

        # Convert datetime fields
        if hasattr(project_dict.get("created_at"), 'isoformat'):
            project_dict["created_at"] = project_dict["created_at"].isoformat()
        if hasattr(project_dict.get("updated_at"), 'isoformat'):
            project_dict["updated_at"] = project_dict["updated_at"].isoformat()

        # Get linked sources
        technical_sources = []
        business_sources = []

        try:
            sources_links = await AsyncPGClient.fetch(
                "SELECT source_id, notes FROM archon_project_sources WHERE project_id = $1",
                project_id
            )

            technical_source_ids = []
            business_source_ids = []

            for source_link in sources_links:
                if source_link.get("notes") == "technical":
                    technical_source_ids.append(str(source_link["source_id"]))
                elif source_link.get("notes") == "business":
                    business_source_ids.append(str(source_link["source_id"]))

            if technical_source_ids:
                placeholders = ", ".join(f"${i+1}" for i in range(len(technical_source_ids)))
                tech_sources = await AsyncPGClient.fetch(
                    f"SELECT * FROM archon_sources WHERE source_id IN ({placeholders})",
                    *technical_source_ids
                )
                technical_sources = [dict(s) for s in tech_sources]

            if business_source_ids:
                placeholders = ", ".join(f"${i+1}" for i in range(len(business_source_ids)))
                biz_sources = await AsyncPGClient.fetch(
                    f"SELECT * FROM archon_sources WHERE source_id IN ({placeholders})",
                    *business_source_ids
                )
                business_sources = [dict(s) for s in biz_sources]

        except Exception as e:
            logger.warning(f"Failed to retrieve linked sources for project {project_id}: {e}")

        project_dict["technical_sources"] = technical_sources
        project_dict["business_sources"] = business_sources

        return True, {"project": project_dict}

    def _get_project_supabase(self, project_id: str) -> tuple[bool, dict[str, Any]]:
        """Get project using Supabase (legacy)."""
        response = (
            self.supabase_client.table("archon_projects")
            .select("*")
            .eq("id", project_id)
            .execute()
        )

        if not response.data:
            return False, {"error": f"Project with ID {project_id} not found"}

        project = response.data[0]

        # Get linked sources
        technical_sources = []
        business_sources = []

        try:
            sources_response = (
                self.supabase_client.table("archon_project_sources")
                .select("source_id, notes")
                .eq("project_id", project["id"])
                .execute()
            )

            technical_source_ids = []
            business_source_ids = []

            for source_link in sources_response.data:
                if source_link.get("notes") == "technical":
                    technical_source_ids.append(source_link["source_id"])
                elif source_link.get("notes") == "business":
                    business_source_ids.append(source_link["source_id"])

            if technical_source_ids:
                tech_sources_response = (
                    self.supabase_client.table("archon_sources")
                    .select("*")
                    .in_("source_id", technical_source_ids)
                    .execute()
                )
                technical_sources = tech_sources_response.data

            if business_source_ids:
                biz_sources_response = (
                    self.supabase_client.table("archon_sources")
                    .select("*")
                    .in_("source_id", business_source_ids)
                    .execute()
                )
                business_sources = biz_sources_response.data

        except Exception as e:
            logger.warning(f"Failed to retrieve linked sources for project {project['id']}: {e}")

        project["technical_sources"] = technical_sources
        project["business_sources"] = business_sources

        return True, {"project": project}

    async def delete_project(self, project_id: str) -> tuple[bool, dict[str, Any]]:
        """
        Delete a project and all its associated tasks.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                # Check if project exists
                project = await AsyncPGClient.fetchrow(
                    "SELECT id FROM archon_projects WHERE id = $1",
                    project_id
                )
                if not project:
                    return False, {"error": f"Project with ID {project_id} not found"}

                # Get task count for reporting
                task_count_result = await AsyncPGClient.fetchval(
                    "SELECT COUNT(*) FROM archon_tasks WHERE project_id = $1",
                    project_id
                )
                tasks_count = task_count_result or 0

                # Delete the project (tasks will be deleted by cascade)
                await AsyncPGClient.execute(
                    "DELETE FROM archon_projects WHERE id = $1",
                    project_id
                )

                return True, {
                    "project_id": project_id,
                    "deleted_tasks": tasks_count,
                    "message": "Project deleted successfully",
                }
            else:
                # Check if project exists
                check_response = (
                    self.supabase_client.table("archon_projects")
                    .select("id")
                    .eq("id", project_id)
                    .execute()
                )
                if not check_response.data:
                    return False, {"error": f"Project with ID {project_id} not found"}

                # Get task count for reporting
                tasks_response = (
                    self.supabase_client.table("archon_tasks")
                    .select("id")
                    .eq("project_id", project_id)
                    .execute()
                )
                tasks_count = len(tasks_response.data) if tasks_response.data else 0

                # Delete the project (tasks will be deleted by cascade)
                self.supabase_client.table("archon_projects").delete().eq("id", project_id).execute()

                return True, {
                    "project_id": project_id,
                    "deleted_tasks": tasks_count,
                    "message": "Project deleted successfully",
                }

        except Exception as e:
            logger.error(f"Error deleting project: {e}")
            return False, {"error": f"Error deleting project: {str(e)}"}

    async def get_project_features(self, project_id: str) -> tuple[bool, dict[str, Any]]:
        """
        Get features from a project's features JSONB field.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                project = await AsyncPGClient.fetchrow(
                    "SELECT features FROM archon_projects WHERE id = $1",
                    project_id
                )

                if not project:
                    return False, {"error": "Project not found"}

                features = project.get("features", [])
                if isinstance(features, str):
                    features = json.loads(features)
            else:
                response = (
                    self.supabase_client.table("archon_projects")
                    .select("features")
                    .eq("id", project_id)
                    .single()
                    .execute()
                )

                if not response.data:
                    return False, {"error": "Project not found"}

                features = response.data.get("features", [])

            # Extract feature labels for dropdown options
            feature_options = []
            for feature in features:
                if isinstance(feature, dict) and "data" in feature and "label" in feature["data"]:
                    feature_options.append({
                        "id": feature.get("id", ""),
                        "label": feature["data"]["label"],
                        "type": feature["data"].get("type", ""),
                        "feature_type": feature.get("type", "page"),
                    })

            return True, {"features": feature_options, "count": len(feature_options)}

        except Exception as e:
            error_message = str(e)
            if "The result contains 0 rows" in error_message or "PGRST116" in error_message:
                return False, {"error": "Project not found"}

            logger.error(f"Error getting project features: {e}")
            return False, {"error": f"Error getting project features: {str(e)}"}

    async def update_project(
        self, project_id: str, update_fields: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """
        Update a project with specified fields.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            now = datetime.now().isoformat()

            # Build update data
            update_data = {"updated_at": now}

            # Add allowed fields
            allowed_fields = [
                "title",
                "description",
                "github_repo",
                "docs",
                "features",
                "data",
                "technical_sources",
                "business_sources",
                "pinned",
            ]

            for field in allowed_fields:
                if field in update_fields:
                    update_data[field] = update_fields[field]

            if is_asyncpg_mode():
                return await self._update_project_asyncpg(project_id, update_data, update_fields)
            else:
                return self._update_project_supabase(project_id, update_data, update_fields)

        except Exception as e:
            logger.error(f"Error updating project: {e}")
            return False, {"error": f"Error updating project: {str(e)}"}

    async def _update_project_asyncpg(
        self, project_id: str, update_data: dict[str, Any], update_fields: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """Update project using asyncpg."""
        from ..database import AsyncPGClient

        # Handle pinning logic - only one project can be pinned at a time
        if update_fields.get("pinned") is True:
            await AsyncPGClient.execute(
                """
                UPDATE archon_projects
                SET pinned = false
                WHERE id != $1 AND pinned = true
                """,
                project_id
            )
            logger.debug(f"Unpinned other projects before pinning {project_id}")

        # Build SET clause dynamically
        set_parts = []
        params = []
        param_idx = 1

        for key, value in update_data.items():
            # Convert lists/dicts to JSON for JSONB columns
            if key in ["docs", "features", "data", "technical_sources", "business_sources"]:
                value = json.dumps(value) if value is not None else json.dumps([])
            set_parts.append(f"{key} = ${param_idx}")
            params.append(value)
            param_idx += 1

        params.append(project_id)

        query = f"""
            UPDATE archon_projects
            SET {", ".join(set_parts)}
            WHERE id = ${param_idx}
            RETURNING *
        """

        project = await AsyncPGClient.fetchrow(query, *params)

        if project:
            project_dict = dict(project)
            project_dict["id"] = str(project_dict["id"])
            if hasattr(project_dict.get("created_at"), 'isoformat'):
                project_dict["created_at"] = project_dict["created_at"].isoformat()
            if hasattr(project_dict.get("updated_at"), 'isoformat'):
                project_dict["updated_at"] = project_dict["updated_at"].isoformat()
            return True, {"project": project_dict, "message": "Project updated successfully"}
        else:
            return False, {"error": f"Project with ID {project_id} not found"}

    def _update_project_supabase(
        self, project_id: str, update_data: dict[str, Any], update_fields: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """Update project using Supabase (legacy)."""
        # Handle pinning logic - only one project can be pinned at a time
        if update_fields.get("pinned") is True:
            unpin_response = (
                self.supabase_client.table("archon_projects")
                .update({"pinned": False})
                .neq("id", project_id)
                .eq("pinned", True)
                .execute()
            )
            logger.debug(f"Unpinned {len(unpin_response.data or [])} other projects before pinning {project_id}")

        # Update the target project
        response = (
            self.supabase_client.table("archon_projects")
            .update(update_data)
            .eq("id", project_id)
            .execute()
        )

        if response.data and len(response.data) > 0:
            project = response.data[0]
            return True, {"project": project, "message": "Project updated successfully"}
        else:
            # If update didn't return data, fetch the project
            get_response = (
                self.supabase_client.table("archon_projects")
                .select("*")
                .eq("id", project_id)
                .execute()
            )
            if get_response.data and len(get_response.data) > 0:
                project = get_response.data[0]
                return True, {"project": project, "message": "Project updated successfully"}
            else:
                return False, {"error": f"Project with ID {project_id} not found"}
