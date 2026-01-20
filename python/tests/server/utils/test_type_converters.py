"""Tests for type_converters module."""

from datetime import datetime
from uuid import UUID

from src.server.utils.type_converters import get_now, row_to_dict


def test_row_to_dict_converts_uuid():
    """Test that UUID objects are converted to strings."""
    row = {"id": UUID("550e8400-e29b-41d4-a716-446655440000"), "name": "test"}
    result = row_to_dict(row)
    assert result["id"] == "550e8400-e29b-41d4-a716-446655440000"
    assert isinstance(result["id"], str)


def test_row_to_dict_converts_datetime():
    """Test that datetime objects are converted to ISO strings."""
    now = datetime.now()
    row = {"created_at": now, "name": "test"}
    result = row_to_dict(row)
    assert result["created_at"] == now.isoformat()
    assert isinstance(result["created_at"], str)


def test_row_to_dict_handles_none():
    """Test that None input returns empty dict."""
    result = row_to_dict(None)
    assert result == {}


def test_row_to_dict_preserves_other_types():
    """Test that other types are preserved as-is."""
    row = {
        "id": UUID("550e8400-e29b-41d4-a716-446655440000"),
        "name": "test",
        "count": 42,
        "active": True,
        "tags": ["a", "b"],
    }
    result = row_to_dict(row)
    assert result["name"] == "test"
    assert result["count"] == 42
    assert result["active"] is True
    assert result["tags"] == ["a", "b"]


def test_row_to_dict_handles_mixed_types():
    """Test conversion with multiple UUID and datetime fields."""
    now = datetime.now()
    row = {
        "id": UUID("550e8400-e29b-41d4-a716-446655440000"),
        "project_id": UUID("660e8400-e29b-41d4-a716-446655440001"),
        "created_at": now,
        "updated_at": now,
        "title": "Test Task",
    }
    result = row_to_dict(row)
    assert result["id"] == "550e8400-e29b-41d4-a716-446655440000"
    assert result["project_id"] == "660e8400-e29b-41d4-a716-446655440001"
    assert result["created_at"] == now.isoformat()
    assert result["updated_at"] == now.isoformat()
    assert result["title"] == "Test Task"


def test_get_now_returns_datetime():
    """Test that get_now returns a datetime object."""
    result = get_now()
    assert isinstance(result, datetime)


def test_get_now_returns_current_time():
    """Test that get_now returns approximately current time."""
    before = datetime.now()
    result = get_now()
    after = datetime.now()
    assert before <= result <= after
