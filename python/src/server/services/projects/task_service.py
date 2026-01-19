"""
Task Service Module for Archon

This module provides core business logic for task operations that can be
shared between MCP tools and FastAPI endpoints.

Supports both asyncpg (K8s) and Supabase (legacy) database backends.
"""

import json
from datetime import datetime
from typing import Any

from ...config.logfire_config import get_logger
from ..client_manager import get_database_mode, is_asyncpg_mode

logger = get_logger(__name__)


class TaskService:
    """Service class for task operations"""

    VALID_STATUSES = ["todo", "doing", "review", "done"]

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

    def validate_status(self, status: str) -> tuple[bool, str]:
        """Validate task status"""
        if status not in self.VALID_STATUSES:
            return (
                False,
                f"Invalid status '{status}'. Must be one of: {', '.join(self.VALID_STATUSES)}",
            )
        return True, ""

    def validate_assignee(self, assignee: str) -> tuple[bool, str]:
        """Validate task assignee"""
        if not assignee or not isinstance(assignee, str) or len(assignee.strip()) == 0:
            return False, "Assignee must be a non-empty string"
        return True, ""

    def validate_priority(self, priority: str) -> tuple[bool, str]:
        """Validate task priority against allowed enum values"""
        VALID_PRIORITIES = ["low", "medium", "high", "critical"]
        if priority not in VALID_PRIORITIES:
            return (
                False,
                f"Invalid priority '{priority}'. Must be one of: {', '.join(VALID_PRIORITIES)}",
            )
        return True, ""

    async def create_task(
        self,
        project_id: str,
        title: str,
        description: str = "",
        assignee: str = "User",
        task_order: int = 0,
        priority: str = "medium",
        feature: str | None = None,
        sources: list[dict[str, Any]] | None = None,
        code_examples: list[dict[str, Any]] | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Create a new task under a project with automatic reordering.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            # Validate inputs
            if not title or not isinstance(title, str) or len(title.strip()) == 0:
                return False, {"error": "Task title is required and must be a non-empty string"}

            if not project_id or not isinstance(project_id, str):
                return False, {"error": "Project ID is required and must be a string"}

            # Validate assignee
            is_valid, error_msg = self.validate_assignee(assignee)
            if not is_valid:
                return False, {"error": error_msg}

            # Validate priority
            is_valid, error_msg = self.validate_priority(priority)
            if not is_valid:
                return False, {"error": error_msg}

            task_status = "todo"

            if is_asyncpg_mode():
                return await self._create_task_asyncpg(
                    project_id, title, description, assignee, task_order,
                    priority, feature, sources, code_examples, task_status
                )
            else:
                return await self._create_task_supabase(
                    project_id, title, description, assignee, task_order,
                    priority, feature, sources, code_examples, task_status
                )

        except Exception as e:
            logger.error(f"Error creating task: {e}")
            return False, {"error": f"Error creating task: {str(e)}"}

    async def _create_task_asyncpg(
        self, project_id: str, title: str, description: str, assignee: str,
        task_order: int, priority: str, feature: str | None,
        sources: list | None, code_examples: list | None, task_status: str
    ) -> tuple[bool, dict[str, Any]]:
        """Create task using asyncpg."""
        from ..database import AsyncPGClient

        now = datetime.now().isoformat()

        # REORDERING LOGIC: If inserting at a specific position, increment existing tasks
        if task_order > 0:
            existing_tasks = await AsyncPGClient.fetch(
                """
                SELECT id, task_order FROM archon_tasks
                WHERE project_id = $1 AND status = $2 AND task_order >= $3
                """,
                project_id, task_status, task_order
            )

            if existing_tasks:
                logger.info(f"Reordering {len(existing_tasks)} existing tasks")
                for existing_task in existing_tasks:
                    await AsyncPGClient.execute(
                        """
                        UPDATE archon_tasks
                        SET task_order = $1, updated_at = $2
                        WHERE id = $3
                        """,
                        existing_task["task_order"] + 1, now, existing_task["id"]
                    )

        # Insert the new task
        task = await AsyncPGClient.fetchrow(
            """
            INSERT INTO archon_tasks (
                project_id, title, description, status, assignee,
                task_order, priority, feature, sources, code_examples,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
            """,
            project_id, title, description, task_status, assignee,
            task_order, priority, feature,
            json.dumps(sources or []), json.dumps(code_examples or []),
            now, now
        )

        if task:
            return True, {
                "task": {
                    "id": str(task["id"]),
                    "project_id": str(task["project_id"]),
                    "title": task["title"],
                    "description": task["description"],
                    "status": task["status"],
                    "assignee": task["assignee"],
                    "task_order": task["task_order"],
                    "priority": task["priority"],
                    "created_at": task["created_at"].isoformat() if hasattr(task["created_at"], 'isoformat') else task["created_at"],
                }
            }
        else:
            return False, {"error": "Failed to create task"}

    async def _create_task_supabase(
        self, project_id: str, title: str, description: str, assignee: str,
        task_order: int, priority: str, feature: str | None,
        sources: list | None, code_examples: list | None, task_status: str
    ) -> tuple[bool, dict[str, Any]]:
        """Create task using Supabase (legacy)."""
        now = datetime.now().isoformat()

        # REORDERING LOGIC: If inserting at a specific position, increment existing tasks
        if task_order > 0:
            existing_tasks_response = (
                self.supabase_client.table("archon_tasks")
                .select("id, task_order")
                .eq("project_id", project_id)
                .eq("status", task_status)
                .gte("task_order", task_order)
                .execute()
            )

            if existing_tasks_response.data:
                logger.info(f"Reordering {len(existing_tasks_response.data)} existing tasks")
                for existing_task in existing_tasks_response.data:
                    new_order = existing_task["task_order"] + 1
                    self.supabase_client.table("archon_tasks").update({
                        "task_order": new_order,
                        "updated_at": now,
                    }).eq("id", existing_task["id"]).execute()

        task_data = {
            "project_id": project_id,
            "title": title,
            "description": description,
            "status": task_status,
            "assignee": assignee,
            "task_order": task_order,
            "priority": priority,
            "sources": sources or [],
            "code_examples": code_examples or [],
            "created_at": now,
            "updated_at": now,
        }

        if feature:
            task_data["feature"] = feature

        response = self.supabase_client.table("archon_tasks").insert(task_data).execute()

        if response.data:
            task = response.data[0]
            return True, {
                "task": {
                    "id": task["id"],
                    "project_id": task["project_id"],
                    "title": task["title"],
                    "description": task["description"],
                    "status": task["status"],
                    "assignee": task["assignee"],
                    "task_order": task["task_order"],
                    "priority": task["priority"],
                    "created_at": task["created_at"],
                }
            }
        else:
            return False, {"error": "Failed to create task"}

    async def list_tasks(
        self,
        project_id: str | None = None,
        status: str | None = None,
        include_closed: bool = False,
        exclude_large_fields: bool = False,
        include_archived: bool = False,
        search_query: str | None = None
    ) -> tuple[bool, dict[str, Any]]:
        """
        List tasks with various filters.

        Args:
            project_id: Filter by project
            status: Filter by status
            include_closed: Include done tasks
            exclude_large_fields: If True, excludes sources and code_examples fields
            include_archived: If True, includes archived tasks
            search_query: Keyword search in title, description, and feature fields

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            # Validate status if provided
            if status:
                is_valid, error_msg = self.validate_status(status)
                if not is_valid:
                    return False, {"error": error_msg}

            if is_asyncpg_mode():
                return await self._list_tasks_asyncpg(
                    project_id, status, include_closed, exclude_large_fields,
                    include_archived, search_query
                )
            else:
                return self._list_tasks_supabase(
                    project_id, status, include_closed, exclude_large_fields,
                    include_archived, search_query
                )

        except Exception as e:
            logger.error(f"Error listing tasks: {e}")
            return False, {"error": f"Error listing tasks: {str(e)}"}

    async def _list_tasks_asyncpg(
        self, project_id: str | None, status: str | None, include_closed: bool,
        exclude_large_fields: bool, include_archived: bool, search_query: str | None
    ) -> tuple[bool, dict[str, Any]]:
        """List tasks using asyncpg."""
        from ..database import AsyncPGClient

        # Build query dynamically
        conditions = []
        params = []
        param_idx = 1

        if project_id:
            conditions.append(f"project_id = ${param_idx}")
            params.append(project_id)
            param_idx += 1

        if status:
            conditions.append(f"status = ${param_idx}")
            params.append(status)
            param_idx += 1
        elif not include_closed:
            conditions.append("status != 'done'")

        if not include_archived:
            conditions.append("(archived IS NULL OR archived = false)")

        if search_query:
            search_pattern = f"%{search_query.lower()}%"
            conditions.append(f"(LOWER(title) LIKE ${param_idx} OR LOWER(description) LIKE ${param_idx} OR LOWER(feature) LIKE ${param_idx})")
            params.append(search_pattern)
            param_idx += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"""
            SELECT * FROM archon_tasks
            WHERE {where_clause}
            ORDER BY task_order ASC, created_at ASC
        """

        rows = await AsyncPGClient.fetch(query, *params)

        tasks = []
        for task in rows:
            task_data = {
                "id": str(task["id"]),
                "project_id": str(task["project_id"]),
                "title": task["title"],
                "description": task["description"],
                "status": task["status"],
                "assignee": task.get("assignee", "User"),
                "task_order": task.get("task_order", 0),
                "priority": task.get("priority", "medium"),
                "feature": task.get("feature"),
                "created_at": task["created_at"].isoformat() if hasattr(task["created_at"], 'isoformat') else task["created_at"],
                "updated_at": task["updated_at"].isoformat() if hasattr(task["updated_at"], 'isoformat') else task["updated_at"],
                "archived": task.get("archived", False),
            }

            # Handle JSONB fields
            sources = task.get("sources", [])
            code_examples = task.get("code_examples", [])

            # Parse if string (asyncpg returns JSONB as Python objects, but just in case)
            if isinstance(sources, str):
                sources = json.loads(sources)
            if isinstance(code_examples, str):
                code_examples = json.loads(code_examples)

            if not exclude_large_fields:
                task_data["sources"] = sources
                task_data["code_examples"] = code_examples
            else:
                task_data["stats"] = {
                    "sources_count": len(sources) if sources else 0,
                    "code_examples_count": len(code_examples) if code_examples else 0
                }

            tasks.append(task_data)

        filter_info = []
        if project_id:
            filter_info.append(f"project_id={project_id}")
        if status:
            filter_info.append(f"status={status}")
        if not include_closed:
            filter_info.append("excluding closed tasks")

        return True, {
            "tasks": tasks,
            "total_count": len(tasks),
            "filters_applied": ", ".join(filter_info) if filter_info else "none",
            "include_closed": include_closed,
        }

    def _list_tasks_supabase(
        self, project_id: str | None, status: str | None, include_closed: bool,
        exclude_large_fields: bool, include_archived: bool, search_query: str | None
    ) -> tuple[bool, dict[str, Any]]:
        """List tasks using Supabase (legacy)."""
        # Start with base query
        if exclude_large_fields:
            query = self.supabase_client.table("archon_tasks").select(
                "id, project_id, parent_task_id, title, description, "
                "status, assignee, task_order, priority, feature, archived, "
                "archived_at, archived_by, created_at, updated_at, "
                "sources, code_examples"
            )
        else:
            query = self.supabase_client.table("archon_tasks").select("*")

        # Track filters for debugging
        filters_applied = []

        # Apply filters
        if project_id:
            query = query.eq("project_id", project_id)
            filters_applied.append(f"project_id={project_id}")

        if status:
            query = query.eq("status", status)
            filters_applied.append(f"status={status}")
        elif not include_closed:
            query = query.neq("status", "done")
            filters_applied.append("exclude done tasks")

        # Apply keyword search if provided
        if search_query:
            search_terms = search_query.lower().split()
            if len(search_terms) == 1:
                term = search_terms[0]
                query = query.or_(
                    f"title.ilike.%{term}%,"
                    f"description.ilike.%{term}%,"
                    f"feature.ilike.%{term}%"
                )
            else:
                full_query = search_query.lower()
                query = query.or_(
                    f"title.ilike.%{full_query}%,"
                    f"description.ilike.%{full_query}%,"
                    f"feature.ilike.%{full_query}%"
                )
            filters_applied.append(f"search={search_query}")

        # Filter out archived tasks only if not including them
        if not include_archived:
            query = query.or_("archived.is.null,archived.is.false")
            filters_applied.append("exclude archived tasks (null or false)")

        logger.debug(f"Listing tasks with filters: {', '.join(filters_applied)}")

        response = (
            query.order("task_order", desc=False).order("created_at", desc=False).execute()
        )

        tasks = []
        for task in response.data:
            task_data = {
                "id": task["id"],
                "project_id": task["project_id"],
                "title": task["title"],
                "description": task["description"],
                "status": task["status"],
                "assignee": task.get("assignee", "User"),
                "task_order": task.get("task_order", 0),
                "priority": task.get("priority", "medium"),
                "feature": task.get("feature"),
                "created_at": task["created_at"],
                "updated_at": task["updated_at"],
                "archived": task.get("archived", False),
            }

            if not exclude_large_fields:
                task_data["sources"] = task.get("sources", [])
                task_data["code_examples"] = task.get("code_examples", [])
            else:
                task_data["stats"] = {
                    "sources_count": len(task.get("sources", [])),
                    "code_examples_count": len(task.get("code_examples", []))
                }

            tasks.append(task_data)

        filter_info = []
        if project_id:
            filter_info.append(f"project_id={project_id}")
        if status:
            filter_info.append(f"status={status}")
        if not include_closed:
            filter_info.append("excluding closed tasks")

        return True, {
            "tasks": tasks,
            "total_count": len(tasks),
            "filters_applied": ", ".join(filter_info) if filter_info else "none",
            "include_closed": include_closed,
        }

    async def get_task(self, task_id: str) -> tuple[bool, dict[str, Any]]:
        """
        Get a specific task by ID.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            if is_asyncpg_mode():
                from ..database import AsyncPGClient
                task = await AsyncPGClient.fetchrow(
                    "SELECT * FROM archon_tasks WHERE id = $1",
                    task_id
                )
                if task:
                    task_dict = dict(task)
                    # Convert UUID to string
                    task_dict["id"] = str(task_dict["id"])
                    task_dict["project_id"] = str(task_dict["project_id"])
                    # Convert datetime to ISO string
                    if hasattr(task_dict.get("created_at"), 'isoformat'):
                        task_dict["created_at"] = task_dict["created_at"].isoformat()
                    if hasattr(task_dict.get("updated_at"), 'isoformat'):
                        task_dict["updated_at"] = task_dict["updated_at"].isoformat()
                    return True, {"task": task_dict}
                else:
                    return False, {"error": f"Task with ID {task_id} not found"}
            else:
                response = (
                    self.supabase_client.table("archon_tasks").select("*").eq("id", task_id).execute()
                )
                if response.data:
                    task = response.data[0]
                    return True, {"task": task}
                else:
                    return False, {"error": f"Task with ID {task_id} not found"}

        except Exception as e:
            logger.error(f"Error getting task: {e}")
            return False, {"error": f"Error getting task: {str(e)}"}

    async def update_task(
        self, task_id: str, update_fields: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """
        Update task with specified fields.

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            # Build update data
            update_data = {"updated_at": datetime.now().isoformat()}

            # Validate and add fields
            if "title" in update_fields:
                update_data["title"] = update_fields["title"]

            if "description" in update_fields:
                update_data["description"] = update_fields["description"]

            if "status" in update_fields:
                is_valid, error_msg = self.validate_status(update_fields["status"])
                if not is_valid:
                    return False, {"error": error_msg}
                update_data["status"] = update_fields["status"]

            if "assignee" in update_fields:
                is_valid, error_msg = self.validate_assignee(update_fields["assignee"])
                if not is_valid:
                    return False, {"error": error_msg}
                update_data["assignee"] = update_fields["assignee"]

            if "priority" in update_fields:
                is_valid, error_msg = self.validate_priority(update_fields["priority"])
                if not is_valid:
                    return False, {"error": error_msg}
                update_data["priority"] = update_fields["priority"]

            if "task_order" in update_fields:
                update_data["task_order"] = update_fields["task_order"]

            if "feature" in update_fields:
                update_data["feature"] = update_fields["feature"]

            if is_asyncpg_mode():
                return await self._update_task_asyncpg(task_id, update_data)
            else:
                return await self._update_task_supabase(task_id, update_data)

        except Exception as e:
            logger.error(f"Error updating task: {e}")
            return False, {"error": f"Error updating task: {str(e)}"}

    async def _update_task_asyncpg(
        self, task_id: str, update_data: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """Update task using asyncpg."""
        from ..database import AsyncPGClient

        # Build SET clause dynamically
        set_parts = []
        params = []
        param_idx = 1

        for key, value in update_data.items():
            set_parts.append(f"{key} = ${param_idx}")
            params.append(value)
            param_idx += 1

        params.append(task_id)  # For WHERE clause

        query = f"""
            UPDATE archon_tasks
            SET {", ".join(set_parts)}
            WHERE id = ${param_idx}
            RETURNING *
        """

        task = await AsyncPGClient.fetchrow(query, *params)

        if task:
            task_dict = dict(task)
            task_dict["id"] = str(task_dict["id"])
            task_dict["project_id"] = str(task_dict["project_id"])
            if hasattr(task_dict.get("created_at"), 'isoformat'):
                task_dict["created_at"] = task_dict["created_at"].isoformat()
            if hasattr(task_dict.get("updated_at"), 'isoformat'):
                task_dict["updated_at"] = task_dict["updated_at"].isoformat()
            return True, {"task": task_dict, "message": "Task updated successfully"}
        else:
            return False, {"error": f"Task with ID {task_id} not found"}

    async def _update_task_supabase(
        self, task_id: str, update_data: dict[str, Any]
    ) -> tuple[bool, dict[str, Any]]:
        """Update task using Supabase (legacy)."""
        response = (
            self.supabase_client.table("archon_tasks")
            .update(update_data)
            .eq("id", task_id)
            .execute()
        )

        if response.data:
            task = response.data[0]
            return True, {"task": task, "message": "Task updated successfully"}
        else:
            return False, {"error": f"Task with ID {task_id} not found"}

    async def archive_task(
        self, task_id: str, archived_by: str = "mcp"
    ) -> tuple[bool, dict[str, Any]]:
        """
        Archive a task and all its subtasks (soft delete).

        Returns:
            Tuple of (success, result_dict)
        """
        try:
            now = datetime.now().isoformat()

            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                # Check if task exists and is not already archived
                task = await AsyncPGClient.fetchrow(
                    "SELECT * FROM archon_tasks WHERE id = $1",
                    task_id
                )
                if not task:
                    return False, {"error": f"Task with ID {task_id} not found"}

                if task.get("archived") is True:
                    return False, {"error": f"Task with ID {task_id} is already archived"}

                # Archive the task
                await AsyncPGClient.execute(
                    """
                    UPDATE archon_tasks
                    SET archived = true, archived_at = $1, archived_by = $2, updated_at = $3
                    WHERE id = $4
                    """,
                    now, archived_by, now, task_id
                )
                return True, {"task_id": task_id, "message": "Task archived successfully"}
            else:
                # Check if task exists and is not already archived
                task_response = (
                    self.supabase_client.table("archon_tasks").select("*").eq("id", task_id).execute()
                )
                if not task_response.data:
                    return False, {"error": f"Task with ID {task_id} not found"}

                task = task_response.data[0]
                if task.get("archived") is True:
                    return False, {"error": f"Task with ID {task_id} is already archived"}

                # Archive the task
                archive_data = {
                    "archived": True,
                    "archived_at": now,
                    "archived_by": archived_by,
                    "updated_at": now,
                }

                response = (
                    self.supabase_client.table("archon_tasks")
                    .update(archive_data)
                    .eq("id", task_id)
                    .execute()
                )

                if response.data:
                    return True, {"task_id": task_id, "message": "Task archived successfully"}
                else:
                    return False, {"error": f"Failed to archive task {task_id}"}

        except Exception as e:
            logger.error(f"Error archiving task: {e}")
            return False, {"error": f"Error archiving task: {str(e)}"}

    async def get_all_project_task_counts(self) -> tuple[bool, dict[str, dict[str, int]]]:
        """
        Get task counts for all projects in a single optimized query.

        Returns task counts grouped by project_id and status.

        Returns:
            Tuple of (success, counts_dict) where counts_dict is:
            {"project-id": {"todo": 5, "doing": 2, "review": 3, "done": 10}}
        """
        try:
            logger.debug("Fetching task counts for all projects in batch")

            if is_asyncpg_mode():
                from ..database import AsyncPGClient

                rows = await AsyncPGClient.fetch(
                    """
                    SELECT project_id, status FROM archon_tasks
                    WHERE archived IS NULL OR archived = false
                    """
                )
            else:
                response = (
                    self.supabase_client.table("archon_tasks")
                    .select("project_id, status")
                    .or_("archived.is.null,archived.is.false")
                    .execute()
                )
                rows = response.data

            if not rows:
                logger.debug("No tasks found")
                return True, {}

            # Process results into counts by project and status
            counts_by_project = {}

            for task in rows:
                project_id = str(task.get("project_id"))
                status = task.get("status")

                if not project_id or not status:
                    continue

                if project_id not in counts_by_project:
                    counts_by_project[project_id] = {
                        "todo": 0,
                        "doing": 0,
                        "review": 0,
                        "done": 0
                    }

                if status in ["todo", "doing", "review", "done"]:
                    counts_by_project[project_id][status] += 1

            logger.debug(f"Task counts fetched for {len(counts_by_project)} projects")

            return True, counts_by_project

        except Exception as e:
            logger.error(f"Error fetching task counts: {e}")
            return False, {"error": f"Error fetching task counts: {str(e)}"}
