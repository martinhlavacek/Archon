"""
Project Creation Service Module for Archon

This module handles the complex project creation workflow including
AI-assisted documentation generation and progress tracking.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from datetime import UTC, datetime
from typing import Any

from ...config.logfire_config import get_logger
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)


class ProjectCreationService:
    """Service class for advanced project creation with AI assistance"""

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

    async def create_project_with_ai(
        self,
        progress_id: str,
        title: str,
        description: str | None = None,
        github_repo: str | None = None,
        **kwargs,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Create a project with AI-assisted documentation generation.

        Args:
            progress_id: Progress tracking identifier
            title: Project title
            description: Project description
            github_repo: GitHub repository URL
            **kwargs: Additional project data

        Returns:
            Tuple of (success, result_dict)
        """
        logger.info(
            f"🏗️ [PROJECT-CREATION] Starting create_project_with_ai for progress_id: {progress_id}, title: {title}"
        )
        try:
            if is_asyncpg_mode():
                return await self._create_project_with_ai_asyncpg(
                    progress_id, title, description, github_repo, **kwargs
                )
            else:
                return await self._create_project_with_ai_supabase(
                    progress_id, title, description, github_repo, **kwargs
                )
        except Exception as e:
            logger.error(
                f"🚨 [PROJECT-CREATION] Project creation failed for progress_id={progress_id}, title={title}: {e}",
                exc_info=True,
            )
            return False, {"error": str(e)}

    async def _create_project_with_ai_asyncpg(
        self,
        progress_id: str,
        title: str,
        description: str | None,
        github_repo: str | None,
        **kwargs,
    ) -> tuple[bool, dict[str, Any]]:
        """Create project with AI using asyncpg."""
        from ..database import AsyncPGClient

        now = datetime.now(UTC)  # asyncpg needs datetime object, not ISO string

        # Create basic project structure
        features = kwargs.get("features", {})
        data = kwargs.get("data", {})
        pinned = kwargs.get("pinned", False)

        # Create the project in database
        project = await AsyncPGClient.fetchrow(
            """
            INSERT INTO archon_projects (
                title, description, github_repo, created_at, updated_at,
                docs, features, data, pinned
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            """,
            title, description or "", github_repo, now, now,
            json.dumps([]), json.dumps(features), json.dumps(data), pinned
        )

        if not project:
            raise RuntimeError(f"Insert returned no data for project '{title}'")

        project_id = str(project["id"])
        logger.info(f"Created project {project_id} in database")

        # Generate AI documentation if API key is available
        ai_success = await self._generate_ai_documentation(
            progress_id, project_id, title, description, github_repo
        )

        # Fetch complete project data
        final_project = await AsyncPGClient.fetchrow(
            "SELECT * FROM archon_projects WHERE id = $1",
            project_id
        )

        if final_project:
            final_project_dict = dict(final_project)

            # Handle JSONB fields
            docs = final_project_dict.get("docs", [])
            if isinstance(docs, str):
                docs = json.loads(docs)
            features = final_project_dict.get("features", {})
            if isinstance(features, str):
                features = json.loads(features)
            data = final_project_dict.get("data", {})
            if isinstance(data, str):
                data = json.loads(data)

            # Convert datetime
            created_at = final_project_dict.get("created_at", "")
            updated_at = final_project_dict.get("updated_at", "")
            if hasattr(created_at, "isoformat"):
                created_at = created_at.isoformat()
            if hasattr(updated_at, "isoformat"):
                updated_at = updated_at.isoformat()

            project_data_for_frontend = {
                "id": str(final_project_dict["id"]),
                "title": final_project_dict["title"],
                "description": final_project_dict.get("description", ""),
                "github_repo": final_project_dict.get("github_repo"),
                "created_at": created_at,
                "updated_at": updated_at,
                "docs": docs,
                "features": features,
                "data": data,
                "pinned": final_project_dict.get("pinned", False),
                "technical_sources": [],
                "business_sources": [],
            }

            return True, {
                "project_id": project_id,
                "project": project_data_for_frontend,
                "ai_documentation_generated": ai_success,
            }
        else:
            return True, {"project_id": project_id, "ai_documentation_generated": ai_success}

    async def _create_project_with_ai_supabase(
        self,
        progress_id: str,
        title: str,
        description: str | None,
        github_repo: str | None,
        **kwargs,
    ) -> tuple[bool, dict[str, Any]]:
        """Create project with AI using Supabase (legacy)."""
        now = datetime.now(UTC).isoformat()

        # Create basic project structure
        project_data = {
            "title": title,
            "description": description or "",
            "github_repo": github_repo,
            "created_at": now,
            "updated_at": now,
            "docs": [],
            "features": kwargs.get("features", {}),
            "data": kwargs.get("data", {}),
        }

        # Add any additional fields from kwargs
        for key in ["pinned"]:
            if key in kwargs:
                project_data[key] = kwargs[key]

        # Create the project in database
        response = self.supabase_client.table("archon_projects").insert(project_data).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase insert failed for project '{title}': {response.error}")
        if not response.data:
            raise RuntimeError(f"Insert returned no data for project '{title}'")

        project_id = response.data[0]["id"]
        logger.info(f"Created project {project_id} in database")

        # Generate AI documentation if API key is available
        ai_success = await self._generate_ai_documentation(
            progress_id, project_id, title, description, github_repo
        )

        # Fetch complete project data
        final_project_response = (
            self.supabase_client.table("archon_projects")
            .select("*")
            .eq("id", project_id)
            .execute()
        )
        if final_project_response.data:
            final_project = final_project_response.data[0]

            project_data_for_frontend = {
                "id": final_project["id"],
                "title": final_project["title"],
                "description": final_project.get("description", ""),
                "github_repo": final_project.get("github_repo"),
                "created_at": final_project["created_at"],
                "updated_at": final_project["updated_at"],
                "docs": final_project.get("docs", []),
                "features": final_project.get("features", {}),
                "data": final_project.get("data", {}),
                "pinned": final_project.get("pinned", False),
                "technical_sources": [],
                "business_sources": [],
            }

            return True, {
                "project_id": project_id,
                "project": project_data_for_frontend,
                "ai_documentation_generated": ai_success,
            }
        else:
            return True, {"project_id": project_id, "ai_documentation_generated": ai_success}

    async def _generate_ai_documentation(
        self,
        progress_id: str,
        project_id: str,
        title: str,
        description: str | None,
        github_repo: str | None,
    ) -> bool:
        """
        Generate AI documentation for the project.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if LLM provider is configured
            from ..credential_service import credential_service
            provider_config = await credential_service.get_active_provider("llm")

            if not provider_config:
                # No LLM provider configured, skip AI documentation
                return False

            # Import DocumentAgent (lazy import to avoid startup issues)
            from ...agents.document_agent import DocumentAgent



            # Initialize DocumentAgent
            document_agent = DocumentAgent()

            # Generate comprehensive PRD using conversation
            prd_request = f"Create a PRD document titled '{title} - Product Requirements Document' for a project called '{title}'"
            if description:
                prd_request += f" with the following description: {description}"
            if github_repo:
                prd_request += f" (GitHub repo: {github_repo})"

            # Create a progress callback for the document agent
            async def agent_progress_callback(update_data):
                pass  # Progress tracking removed

            # Run the document agent to create PRD
            agent_result = await document_agent.run_conversation(
                user_message=prd_request,
                project_id=project_id,
                user_id="system",
                progress_callback=agent_progress_callback,
            )

            if agent_result.success:

                return True
            else:
                return False

        except Exception as ai_error:
            logger.warning(f"AI generation failed, continuing with basic project: {ai_error}")

            return False
