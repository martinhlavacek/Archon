"""
Type converters for asyncpg <-> Pydantic compatibility.

asyncpg returns native Python types (UUID, datetime) that need conversion
for Pydantic models which expect strings.
"""

from datetime import datetime
from typing import Any
from uuid import UUID


def row_to_dict(row: Any) -> dict[str, Any]:
    """
    Convert asyncpg Record to Pydantic-compatible dict.

    Converts:
    - UUID -> str
    - datetime -> ISO string
    """
    if row is None:
        return {}

    result = dict(row)
    for key, value in result.items():
        if isinstance(value, UUID):
            result[key] = str(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
    return result


def get_now() -> datetime:
    """
    Get current datetime for database insertion.

    Returns datetime object (required by asyncpg) not ISO string.
    """
    return datetime.now()
