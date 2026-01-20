"""
Document Service Module for Archon

This module provides core business logic for document operations within projects
that can be shared between MCP tools and FastAPI endpoints.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
import uuid
from datetime import datetime
from typing import Any

from ...config.logfire_config import get_logger
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)


class DocumentService:
    """Service class for document operations within projects"""

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

    async def add_document(
        self,
        project_id: str,
        document_type: str,
        title: str,
        content: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        author: str | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Add a new document to a project's docs JSONB field.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                return await self._add_document_asyncpg(
                    project_id, document_type, title, content, tags, author
                )
            else:
                return self._add_document_supabase(
                    project_id, document_type, title, content, tags, author
                )

        except Exception as e:
            logger.error(f"Error adding document: {e}")
            return False, {"error": f"Error adding document: {str(e)}"}

    async def _add_document_asyncpg(
        self, project_id: str, document_type: str, title: str,
        content: dict | None, tags: list | None, author: str | None
    ) -> tuple[bool, dict[str, Any]]:
        """Add document using asyncpg."""
        from ..database import AsyncPGClient

        # Get current project
        project = await AsyncPGClient.fetchrow(
            "SELECT docs FROM archon_projects WHERE id = $1",
            project_id
        )
        if not project:
            return False, {"error": f"Project with ID {project_id} not found"}

        current_docs = project.get("docs", [])
        if isinstance(current_docs, str):
            current_docs = json.loads(current_docs)

        # Create new document entry
        new_doc = {
            "id": str(uuid.uuid4()),
            "document_type": document_type,
            "title": title,
            "content": content or {},
            "tags": tags or [],
            "status": "draft",
            "version": "1.0",
        }

        if author:
            new_doc["author"] = author

        # Add to docs array
        updated_docs = current_docs + [new_doc]

        # Update project
        result = await AsyncPGClient.fetchrow(
            """
            UPDATE archon_projects
            SET docs = $1, updated_at = $2
            WHERE id = $3
            RETURNING id
            """,
            json.dumps(updated_docs),
            datetime.now(),  # asyncpg needs datetime object
            project_id
        )

        if result:
            return True, {
                "document": {
                    "id": new_doc["id"],
                    "project_id": project_id,
                    "document_type": new_doc["document_type"],
                    "title": new_doc["title"],
                    "status": new_doc["status"],
                    "version": new_doc["version"],
                }
            }
        else:
            return False, {"error": "Failed to add document to project"}

    def _add_document_supabase(
        self, project_id: str, document_type: str, title: str,
        content: dict | None, tags: list | None, author: str | None
    ) -> tuple[bool, dict[str, Any]]:
        """Add document using Supabase (legacy)."""
        # Get current project
        project_response = (
            self.supabase_client.table("archon_projects")
            .select("docs")
            .eq("id", project_id)
            .execute()
        )
        if not project_response.data:
            return False, {"error": f"Project with ID {project_id} not found"}

        current_docs = project_response.data[0].get("docs", [])

        # Create new document entry
        new_doc = {
            "id": str(uuid.uuid4()),
            "document_type": document_type,
            "title": title,
            "content": content or {},
            "tags": tags or [],
            "status": "draft",
            "version": "1.0",
        }

        if author:
            new_doc["author"] = author

        # Add to docs array
        updated_docs = current_docs + [new_doc]

        # Update project
        response = (
            self.supabase_client.table("archon_projects")
            .update({"docs": updated_docs})
            .eq("id", project_id)
            .execute()
        )

        if response.data:
            return True, {
                "document": {
                    "id": new_doc["id"],
                    "project_id": project_id,
                    "document_type": new_doc["document_type"],
                    "title": new_doc["title"],
                    "status": new_doc["status"],
                    "version": new_doc["version"],
                }
            }
        else:
            return False, {"error": "Failed to add document to project"}

    async def list_documents(
        self, project_id: str, include_content: bool = False
    ) -> tuple[bool, dict[str, Any]]:
        """
        List all documents in a project's docs JSONB field.

        Args:
            project_id: The project ID
            include_content: If True, includes full document content.
                           If False (default), returns metadata only.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                project = await AsyncPGClient.fetchrow(
                    "SELECT docs FROM archon_projects WHERE id = $1",
                    project_id
                )
                if not project:
                    return False, {"error": f"Project with ID {project_id} not found"}

                docs = project.get("docs", [])
                if isinstance(docs, str):
                    docs = json.loads(docs)
            else:
                response = (
                    self.supabase_client.table("archon_projects")
                    .select("docs")
                    .eq("id", project_id)
                    .execute()
                )

                if not response.data:
                    return False, {"error": f"Project with ID {project_id} not found"}

                docs = response.data[0].get("docs", [])

            # Format documents for response
            documents = []
            for doc in docs:
                if include_content:
                    documents.append(doc)
                else:
                    documents.append({
                        "id": doc.get("id"),
                        "document_type": doc.get("document_type"),
                        "title": doc.get("title"),
                        "status": doc.get("status"),
                        "version": doc.get("version"),
                        "tags": doc.get("tags", []),
                        "author": doc.get("author"),
                        "created_at": doc.get("created_at"),
                        "updated_at": doc.get("updated_at"),
                        "stats": {
                            "content_size": len(str(doc.get("content", {})))
                        }
                    })

            return True, {
                "project_id": project_id,
                "documents": documents,
                "total_count": len(documents),
            }

        except Exception as e:
            logger.error(f"Error listing documents: {e}")
            return False, {"error": f"Error listing documents: {str(e)}"}

    async def get_document(self, project_id: str, doc_id: str) -> tuple[bool, dict[str, Any]]:
        """
        Get a specific document from a project's docs JSONB field.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                project = await AsyncPGClient.fetchrow(
                    "SELECT docs FROM archon_projects WHERE id = $1",
                    project_id
                )
                if not project:
                    return False, {"error": f"Project with ID {project_id} not found"}

                docs = project.get("docs", [])
                if isinstance(docs, str):
                    docs = json.loads(docs)
            else:
                response = (
                    self.supabase_client.table("archon_projects")
                    .select("docs")
                    .eq("id", project_id)
                    .execute()
                )

                if not response.data:
                    return False, {"error": f"Project with ID {project_id} not found"}

                docs = response.data[0].get("docs", [])

            # Find the specific document
            document = None
            for doc in docs:
                if doc.get("id") == doc_id:
                    document = doc
                    break

            if document:
                return True, {"document": document}
            else:
                return False, {
                    "error": f"Document with ID {doc_id} not found in project {project_id}"
                }

        except Exception as e:
            logger.error(f"Error getting document: {e}")
            return False, {"error": f"Error getting document: {str(e)}"}

    async def update_document(
        self,
        project_id: str,
        doc_id: str,
        update_fields: dict[str, Any],
        create_version: bool = True,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Update a document in a project's docs JSONB field.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                return await self._update_document_asyncpg(
                    project_id, doc_id, update_fields, create_version
                )
            else:
                return self._update_document_supabase(
                    project_id, doc_id, update_fields, create_version
                )

        except Exception as e:
            logger.error(f"Error updating document: {e}")
            return False, {"error": f"Error updating document: {str(e)}"}

    async def _update_document_asyncpg(
        self, project_id: str, doc_id: str,
        update_fields: dict[str, Any], create_version: bool
    ) -> tuple[bool, dict[str, Any]]:
        """Update document using asyncpg."""
        from ..database import AsyncPGClient

        # Get current project docs
        project = await AsyncPGClient.fetchrow(
            "SELECT docs FROM archon_projects WHERE id = $1",
            project_id
        )
        if not project:
            return False, {"error": f"Project with ID {project_id} not found"}

        current_docs = project.get("docs", [])
        if isinstance(current_docs, str):
            current_docs = json.loads(current_docs)

        # Create version snapshot if requested
        if create_version and current_docs:
            try:
                from .versioning_service import VersioningService
                versioning = VersioningService()
                change_summary = self._build_change_summary(doc_id, update_fields)
                await versioning.create_version(
                    project_id=project_id,
                    field_name="docs",
                    content=current_docs,
                    change_summary=change_summary,
                    change_type="update",
                    document_id=doc_id,
                    created_by=update_fields.get("author", "system"),
                )
            except Exception as version_error:
                logger.warning(f"Version creation failed for document {doc_id}: {version_error}")

        # Make a copy to modify
        docs = current_docs.copy()

        # Find and update the document
        updated = False
        for i, doc in enumerate(docs):
            if doc.get("id") == doc_id:
                if "title" in update_fields:
                    docs[i]["title"] = update_fields["title"]
                if "content" in update_fields:
                    docs[i]["content"] = update_fields["content"]
                if "status" in update_fields:
                    docs[i]["status"] = update_fields["status"]
                if "tags" in update_fields:
                    docs[i]["tags"] = update_fields["tags"]
                if "author" in update_fields:
                    docs[i]["author"] = update_fields["author"]
                if "version" in update_fields:
                    docs[i]["version"] = update_fields["version"]

                docs[i]["updated_at"] = datetime.now().isoformat()  # doc timestamp stays ISO string in JSONB
                updated = True
                break

        if not updated:
            return False, {
                "error": f"Document with ID {doc_id} not found in project {project_id}"
            }

        # Update the project
        result = await AsyncPGClient.fetchrow(
            """
            UPDATE archon_projects
            SET docs = $1, updated_at = $2
            WHERE id = $3
            RETURNING id
            """,
            json.dumps(docs),
            datetime.now(),  # asyncpg needs datetime object for project.updated_at
            project_id
        )

        if result:
            # Find the updated document to return
            updated_doc = None
            for doc in docs:
                if doc.get("id") == doc_id:
                    updated_doc = doc
                    break
            return True, {"document": updated_doc}
        else:
            return False, {"error": "Failed to update document"}

    def _update_document_supabase(
        self, project_id: str, doc_id: str,
        update_fields: dict[str, Any], create_version: bool
    ) -> tuple[bool, dict[str, Any]]:
        """Update document using Supabase (legacy)."""
        # Get current project docs
        project_response = (
            self.supabase_client.table("archon_projects")
            .select("docs")
            .eq("id", project_id)
            .execute()
        )
        if not project_response.data:
            return False, {"error": f"Project with ID {project_id} not found"}

        current_docs = project_response.data[0].get("docs", [])

        # Create version snapshot if requested
        if create_version and current_docs:
            try:
                from .versioning_service import VersioningService
                versioning = VersioningService(self.supabase_client)
                change_summary = self._build_change_summary(doc_id, update_fields)
                versioning.create_version(
                    project_id=project_id,
                    field_name="docs",
                    content=current_docs,
                    change_summary=change_summary,
                    change_type="update",
                    document_id=doc_id,
                    created_by=update_fields.get("author", "system"),
                )
            except Exception as version_error:
                logger.warning(f"Version creation failed for document {doc_id}: {version_error}")

        # Make a copy to modify
        docs = current_docs.copy()

        # Find and update the document
        updated = False
        for i, doc in enumerate(docs):
            if doc.get("id") == doc_id:
                if "title" in update_fields:
                    docs[i]["title"] = update_fields["title"]
                if "content" in update_fields:
                    docs[i]["content"] = update_fields["content"]
                if "status" in update_fields:
                    docs[i]["status"] = update_fields["status"]
                if "tags" in update_fields:
                    docs[i]["tags"] = update_fields["tags"]
                if "author" in update_fields:
                    docs[i]["author"] = update_fields["author"]
                if "version" in update_fields:
                    docs[i]["version"] = update_fields["version"]

                docs[i]["updated_at"] = datetime.now().isoformat()
                updated = True
                break

        if not updated:
            return False, {
                "error": f"Document with ID {doc_id} not found in project {project_id}"
            }

        # Update the project
        response = (
            self.supabase_client.table("archon_projects")
            .update({"docs": docs, "updated_at": datetime.now().isoformat()})
            .eq("id", project_id)
            .execute()
        )

        if response.data:
            updated_doc = None
            for doc in docs:
                if doc.get("id") == doc_id:
                    updated_doc = doc
                    break
            return True, {"document": updated_doc}
        else:
            return False, {"error": "Failed to update document"}

    async def delete_document(self, project_id: str, doc_id: str) -> tuple[bool, dict[str, Any]]:
        """
        Delete a document from a project's docs JSONB field.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                # Get current project docs
                project = await AsyncPGClient.fetchrow(
                    "SELECT docs FROM archon_projects WHERE id = $1",
                    project_id
                )
                if not project:
                    return False, {"error": f"Project with ID {project_id} not found"}

                docs = project.get("docs", [])
                if isinstance(docs, str):
                    docs = json.loads(docs)

                # Remove the document
                original_length = len(docs)
                docs = [doc for doc in docs if doc.get("id") != doc_id]

                if len(docs) == original_length:
                    return False, {
                        "error": f"Document with ID {doc_id} not found in project {project_id}"
                    }

                # Update the project
                result = await AsyncPGClient.fetchrow(
                    """
                    UPDATE archon_projects
                    SET docs = $1, updated_at = $2
                    WHERE id = $3
                    RETURNING id
                    """,
                    json.dumps(docs),
                    datetime.now(),  # asyncpg needs datetime object
                    project_id
                )

                if result:
                    return True, {"project_id": project_id, "doc_id": doc_id}
                else:
                    return False, {"error": "Failed to delete document"}
            else:  # Supabase branch
                # Get current project docs
                project_response = (
                    self.supabase_client.table("archon_projects")
                    .select("docs")
                    .eq("id", project_id)
                    .execute()
                )
                if not project_response.data:
                    return False, {"error": f"Project with ID {project_id} not found"}

                docs = project_response.data[0].get("docs", [])

                # Remove the document
                original_length = len(docs)
                docs = [doc for doc in docs if doc.get("id") != doc_id]

                if len(docs) == original_length:
                    return False, {
                        "error": f"Document with ID {doc_id} not found in project {project_id}"
                    }

                # Update the project
                response = (
                    self.supabase_client.table("archon_projects")
                    .update({"docs": docs, "updated_at": datetime.now().isoformat()})
                    .eq("id", project_id)
                    .execute()
                )

                if response.data:
                    return True, {"project_id": project_id, "doc_id": doc_id}
                else:
                    return False, {"error": "Failed to delete document"}

        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return False, {"error": f"Error deleting document: {str(e)}"}

    def _build_change_summary(self, doc_id: str, update_fields: dict[str, Any]) -> str:
        """Build a human-readable change summary"""
        changes = []
        if "title" in update_fields:
            changes.append(f"title to '{update_fields['title']}'")
        if "content" in update_fields:
            changes.append("content")
        if "status" in update_fields:
            changes.append(f"status to '{update_fields['status']}'")

        if changes:
            return f"Updated document '{doc_id}': {', '.join(changes)}"
        else:
            return f"Updated document '{doc_id}'"
