"""
MCP Session Manager

This module provides simplified session management for MCP server connections,
enabling clients to reconnect after server restarts.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

# Removed direct logging import - using unified config
from ..config.logfire_config import get_logger

logger = get_logger(__name__)


@dataclass
class SessionInfo:
    """Metadata for a connected MCP client session."""

    created_at: datetime
    last_seen: datetime
    client_name: str = "Unknown Client"
    client_type: str = "unknown"  # claude-code | claude | cursor | windsurf | vscode | jetbrains | zed | unknown
    connected_at: str = ""  # ISO 8601 string for JSON serialization

    def __post_init__(self):
        if not self.connected_at:
            self.connected_at = self.created_at.isoformat()


def detect_client_type(user_agent: str | None) -> tuple[str, str]:
    """
    Detect client name and type from User-Agent header.

    Returns:
        Tuple of (client_name, client_type).
    """
    if not user_agent:
        return "Unknown Client", "unknown"

    ua_lower = user_agent.lower()
    if "claude-code" in ua_lower:
        return "Claude Code", "claude-code"
    elif "claude" in ua_lower:
        return "Claude", "claude"
    elif "cursor" in ua_lower:
        return "Cursor", "cursor"
    elif "windsurf" in ua_lower or "codeium" in ua_lower:
        return "Windsurf", "windsurf"
    elif "visual studio code" in ua_lower or "vscode" in ua_lower:
        return "VS Code", "vscode"
    elif "jetbrains" in ua_lower or "intellij" in ua_lower:
        return "JetBrains", "jetbrains"
    elif "zed" in ua_lower:
        return "Zed", "zed"
    else:
        # Use first part of User-Agent as name
        name = user_agent.split("/")[0].strip()[:50]  # max 50 chars
        return name or "Unknown Client", "unknown"


class SimplifiedSessionManager:
    """Simplified MCP session manager that tracks session IDs and expiration"""

    def __init__(self, timeout: int = 3600):
        """
        Initialize session manager

        Args:
            timeout: Session expiration time in seconds (default: 1 hour)
        """
        self.sessions: dict[str, SessionInfo] = {}  # session_id -> SessionInfo
        self.timeout = timeout

    def create_session(self, user_agent: str | None = None) -> str:
        """Create a new session and return its ID"""
        session_id = str(uuid.uuid4())
        client_name, client_type = detect_client_type(user_agent)
        now = datetime.now()
        self.sessions[session_id] = SessionInfo(
            created_at=now,
            last_seen=now,
            client_name=client_name,
            client_type=client_type,
        )
        logger.info(f"Created new session: {session_id} client={client_name}")
        return session_id

    def validate_session(self, session_id: str) -> bool:
        """Validate a session ID and update last seen time"""
        if session_id not in self.sessions:
            return False

        info = self.sessions[session_id]
        if datetime.now() - info.last_seen > timedelta(seconds=self.timeout):
            # Session expired, remove it
            del self.sessions[session_id]
            logger.info(f"Session {session_id} expired and removed")
            return False

        # Update last seen time
        info.last_seen = datetime.now()
        return True

    def cleanup_expired_sessions(self) -> int:
        """Remove expired sessions and return count of removed sessions"""
        now = datetime.now()
        expired = []

        for session_id, info in self.sessions.items():
            if now - info.last_seen > timedelta(seconds=self.timeout):
                expired.append(session_id)

        for session_id in expired:
            del self.sessions[session_id]
            logger.info(f"Cleaned up expired session: {session_id}")

        return len(expired)

    def get_active_session_count(self) -> int:
        """Get count of active sessions"""
        # Clean up expired sessions first
        self.cleanup_expired_sessions()
        return len(self.sessions)

    def get_clients(self) -> list[dict]:
        """Get list of active clients as JSON-serializable dicts."""
        self.cleanup_expired_sessions()
        return [
            {
                "id": session_id,
                "name": info.client_name,
                "type": info.client_type,
                "connected_at": info.connected_at,
                "status": "connected",
            }
            for session_id, info in self.sessions.items()
        ]


# Global session manager instance
_session_manager: SimplifiedSessionManager | None = None


def get_session_manager() -> SimplifiedSessionManager:
    """Get the global session manager instance"""
    global _session_manager
    if _session_manager is None:
        _session_manager = SimplifiedSessionManager()
    return _session_manager
