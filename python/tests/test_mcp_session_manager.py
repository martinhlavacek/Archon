"""
Tests for MCP Session Manager - SessionInfo, detect_client_type, and SimplifiedSessionManager.
"""

from datetime import datetime, timedelta

from src.server.services.mcp_session_manager import (
    SessionInfo,
    SimplifiedSessionManager,
    detect_client_type,
)


class TestSessionInfo:
    """Tests for SessionInfo dataclass."""

    def test_session_info_creation(self):
        """SessionInfo stores all fields correctly."""
        now = datetime.now()
        info = SessionInfo(
            created_at=now,
            last_seen=now,
            client_name="Claude Code",
            client_type="claude-code",
        )
        assert info.created_at == now
        assert info.last_seen == now
        assert info.client_name == "Claude Code"
        assert info.client_type == "claude-code"

    def test_session_info_connected_at_auto_fill(self):
        """connected_at is auto-filled from created_at if not provided."""
        now = datetime.now()
        info = SessionInfo(created_at=now, last_seen=now)
        assert info.connected_at == now.isoformat()

    def test_session_info_connected_at_explicit(self):
        """connected_at can be set explicitly."""
        now = datetime.now()
        info = SessionInfo(
            created_at=now,
            last_seen=now,
            connected_at="2026-01-01T00:00:00",
        )
        assert info.connected_at == "2026-01-01T00:00:00"

    def test_session_info_defaults(self):
        """Default values for client_name and client_type."""
        now = datetime.now()
        info = SessionInfo(created_at=now, last_seen=now)
        assert info.client_name == "Unknown Client"
        assert info.client_type == "unknown"


class TestDetectClientType:
    """Tests for detect_client_type helper function."""

    def test_detect_claude_code(self):
        """Detects Claude Code from user agent."""
        name, ctype = detect_client_type("claude-code/1.0.0")
        assert name == "Claude Code"
        assert ctype == "claude-code"

    def test_detect_claude_generic(self):
        """Detects Claude from generic user agent containing 'claude'."""
        name, ctype = detect_client_type("mcp-client/claude")
        assert name == "Claude"
        assert ctype == "claude"

    def test_detect_cursor(self):
        """Detects Cursor from user agent."""
        name, ctype = detect_client_type("cursor/0.45.0")
        assert name == "Cursor"
        assert ctype == "cursor"

    def test_detect_windsurf(self):
        """Detects Windsurf from user agent."""
        name, ctype = detect_client_type("windsurf/1.0.0")
        assert name == "Windsurf"
        assert ctype == "windsurf"

    def test_detect_windsurf_codeium(self):
        """Detects Windsurf via Codeium user agent."""
        name, ctype = detect_client_type("codeium-agent/2.0")
        assert name == "Windsurf"
        assert ctype == "windsurf"

    def test_detect_vscode(self):
        """Detects VS Code from user agent."""
        name, ctype = detect_client_type("Visual Studio Code/1.90.0")
        assert name == "VS Code"
        assert ctype == "vscode"

    def test_detect_vscode_short(self):
        """Detects VS Code from short 'vscode' user agent."""
        name, ctype = detect_client_type("vscode-mcp/0.1")
        assert name == "VS Code"
        assert ctype == "vscode"

    def test_detect_jetbrains(self):
        """Detects JetBrains IDE from user agent."""
        name, ctype = detect_client_type("JetBrains-IntelliJ/2025.1")
        assert name == "JetBrains"
        assert ctype == "jetbrains"

    def test_detect_jetbrains_intellij(self):
        """Detects JetBrains via IntelliJ user agent."""
        name, ctype = detect_client_type("IntelliJ IDEA/2025.1")
        assert name == "JetBrains"
        assert ctype == "jetbrains"

    def test_detect_zed(self):
        """Detects Zed editor from user agent."""
        name, ctype = detect_client_type("zed/0.150.0")
        assert name == "Zed"
        assert ctype == "zed"

    def test_detect_unknown(self):
        """Returns unknown for unrecognized user agents."""
        name, ctype = detect_client_type("some-editor/1.0")
        assert name == "some-editor"
        assert ctype == "unknown"

    def test_detect_none(self):
        """Returns defaults for None user agent."""
        name, ctype = detect_client_type(None)
        assert name == "Unknown Client"
        assert ctype == "unknown"

    def test_detect_empty_string(self):
        """Returns defaults for empty user agent."""
        name, ctype = detect_client_type("")
        assert name == "Unknown Client"
        assert ctype == "unknown"

    def test_detect_truncates_long_name(self):
        """Truncates very long user agent names to 50 chars."""
        long_agent = "a" * 100 + "/1.0"
        name, ctype = detect_client_type(long_agent)
        assert len(name) <= 50
        assert ctype == "unknown"


class TestSimplifiedSessionManager:
    """Tests for SimplifiedSessionManager."""

    def test_create_session_with_user_agent(self):
        """Creates session with correct client metadata from user agent."""
        mgr = SimplifiedSessionManager(timeout=3600)
        session_id = mgr.create_session("claude-code/1.0.0")
        assert session_id in mgr.sessions
        info = mgr.sessions[session_id]
        assert info.client_name == "Claude Code"
        assert info.client_type == "claude-code"
        assert info.connected_at

    def test_create_session_without_user_agent(self):
        """Backward compatibility - creates session without user agent."""
        mgr = SimplifiedSessionManager(timeout=3600)
        session_id = mgr.create_session()
        assert session_id in mgr.sessions
        info = mgr.sessions[session_id]
        assert info.client_name == "Unknown Client"
        assert info.client_type == "unknown"

    def test_validate_session_existing(self):
        """Valid session returns True and updates last_seen."""
        mgr = SimplifiedSessionManager(timeout=3600)
        session_id = mgr.create_session("cursor/1.0")
        original_last_seen = mgr.sessions[session_id].last_seen

        # Small delay to ensure time difference
        result = mgr.validate_session(session_id)
        assert result is True
        assert mgr.sessions[session_id].last_seen >= original_last_seen

    def test_validate_session_expired(self):
        """Expired session returns False and is removed."""
        mgr = SimplifiedSessionManager(timeout=1)  # 1 second timeout
        session_id = mgr.create_session()

        # Manually expire the session
        mgr.sessions[session_id].last_seen = datetime.now() - timedelta(seconds=10)

        result = mgr.validate_session(session_id)
        assert result is False
        assert session_id not in mgr.sessions

    def test_validate_session_nonexistent(self):
        """Nonexistent session returns False."""
        mgr = SimplifiedSessionManager(timeout=3600)
        result = mgr.validate_session("nonexistent-id")
        assert result is False

    def test_cleanup_expired_sessions(self):
        """Removes expired SessionInfo objects."""
        mgr = SimplifiedSessionManager(timeout=1)
        sid1 = mgr.create_session("claude-code/1.0")
        sid2 = mgr.create_session("cursor/1.0")

        # Expire sid1 only
        mgr.sessions[sid1].last_seen = datetime.now() - timedelta(seconds=10)

        removed = mgr.cleanup_expired_sessions()
        assert removed == 1
        assert sid1 not in mgr.sessions
        assert sid2 in mgr.sessions

    def test_get_active_session_count(self):
        """Returns correct count after cleanup."""
        mgr = SimplifiedSessionManager(timeout=3600)
        mgr.create_session("claude-code/1.0")
        mgr.create_session("cursor/1.0")
        assert mgr.get_active_session_count() == 2

    def test_get_active_session_count_with_expired(self):
        """Expired sessions are not counted."""
        mgr = SimplifiedSessionManager(timeout=1)
        sid1 = mgr.create_session()
        mgr.create_session()

        # Expire one
        mgr.sessions[sid1].last_seen = datetime.now() - timedelta(seconds=10)
        assert mgr.get_active_session_count() == 1

    def test_get_clients_empty(self):
        """Returns empty list when no sessions."""
        mgr = SimplifiedSessionManager(timeout=3600)
        assert mgr.get_clients() == []

    def test_get_clients_with_sessions(self):
        """Returns correct client data format."""
        mgr = SimplifiedSessionManager(timeout=3600)
        mgr.create_session("claude-code/1.0")
        mgr.create_session("cursor/2.0")

        clients = mgr.get_clients()
        assert len(clients) == 2

        # Check structure of first client
        client = clients[0]
        assert "id" in client
        assert "name" in client
        assert "type" in client
        assert "connected_at" in client
        assert "status" in client
        assert client["status"] == "connected"

        # Check we have both types
        types = {c["type"] for c in clients}
        assert "claude-code" in types
        assert "cursor" in types

    def test_get_clients_excludes_expired(self):
        """Expired sessions are not included in clients list."""
        mgr = SimplifiedSessionManager(timeout=1)
        sid1 = mgr.create_session("claude-code/1.0")
        mgr.create_session("cursor/1.0")

        # Expire one
        mgr.sessions[sid1].last_seen = datetime.now() - timedelta(seconds=10)

        clients = mgr.get_clients()
        assert len(clients) == 1
        assert clients[0]["type"] == "cursor"
