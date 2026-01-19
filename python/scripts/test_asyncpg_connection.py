#!/usr/bin/env python3
"""
Test script to verify asyncpg connection to existing PostgreSQL/Supabase database.

Usage:
    # Using Supabase direct connection (get from Supabase dashboard > Settings > Database)
    export DATABASE_URL="postgresql://postgres.[project-ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres"

    # Or individual vars
    export POSTGRES_HOST="aws-0-eu-central-1.pooler.supabase.com"
    export POSTGRES_PORT="6543"
    export POSTGRES_USER="postgres.[project-ref]"
    export POSTGRES_PASSWORD="your-password"
    export POSTGRES_DB="postgres"

    uv run python scripts/test_asyncpg_connection.py
"""

import asyncio
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


async def test_direct_asyncpg():
    """Test direct asyncpg connection without our wrapper."""
    import asyncpg

    database_url = os.getenv("DATABASE_URL")

    if database_url:
        print(f"Connecting via DATABASE_URL...")
        conn = await asyncpg.connect(database_url)
    else:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")
        database = os.getenv("POSTGRES_DB", "postgres")

        print(f"Connecting to {host}:{port}/{database} as {user}...")
        conn = await asyncpg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl="require"  # Supabase requires SSL
        )

    # Test basic query
    version = await conn.fetchval("SELECT version()")
    print(f"✓ Connected! PostgreSQL version: {version[:50]}...")

    # Check if our tables exist
    tables = await conn.fetch("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE 'archon_%'
        ORDER BY table_name
    """)

    print(f"\n✓ Found {len(tables)} Archon tables:")
    for t in tables:
        print(f"  - {t['table_name']}")

    # Test a simple read from archon_projects
    projects = await conn.fetch("""
        SELECT id, title, created_at
        FROM archon_projects
        ORDER BY created_at DESC
        LIMIT 5
    """)

    print(f"\n✓ Latest projects ({len(projects)}):")
    for p in projects:
        print(f"  - {p['id'][:8]}... | {p['title'][:40]}")

    await conn.close()
    print("\n✓ Connection closed successfully")


async def test_asyncpg_client():
    """Test our AsyncPGClient wrapper."""
    # Force asyncpg mode
    os.environ["ARCHON_DB_MODE"] = "asyncpg"

    from server.services.database import AsyncPGClient

    print("\n--- Testing AsyncPGClient wrapper ---")

    # Initialize pool
    await AsyncPGClient.initialize()
    print("✓ Connection pool initialized")

    # Test fetchval
    version = await AsyncPGClient.fetchval("SELECT version()")
    print(f"✓ fetchval works: {version[:50]}...")

    # Test fetch
    projects = await AsyncPGClient.fetch("""
        SELECT id, title FROM archon_projects LIMIT 3
    """)
    print(f"✓ fetch works: got {len(projects)} projects")

    # Test fetchrow
    project = await AsyncPGClient.fetchrow("""
        SELECT id, title FROM archon_projects LIMIT 1
    """)
    if project:
        print(f"✓ fetchrow works: {project['title'][:40]}")

    # Close pool
    await AsyncPGClient.close()
    print("✓ Connection pool closed")


async def test_project_service():
    """Test ProjectService in asyncpg mode."""
    os.environ["ARCHON_DB_MODE"] = "asyncpg"

    from server.services.database import AsyncPGClient
    from server.services.projects import ProjectService

    print("\n--- Testing ProjectService with asyncpg ---")

    await AsyncPGClient.initialize()

    service = ProjectService()

    # Test list_projects
    success, result = await service.list_projects(include_content=False)

    if success:
        projects = result.get("projects", [])
        print(f"✓ list_projects works: {len(projects)} projects")
        if projects:
            print(f"  First project: {projects[0].get('title', 'N/A')[:40]}")
    else:
        print(f"✗ list_projects failed: {result.get('error')}")

    await AsyncPGClient.close()


async def main():
    print("=" * 60)
    print("Archon asyncpg Connection Test")
    print("=" * 60)

    # Check environment
    if not os.getenv("DATABASE_URL") and not os.getenv("POSTGRES_HOST"):
        print("\n⚠ No database connection configured!")
        print("Set DATABASE_URL or POSTGRES_* environment variables.")
        print("\nFor Supabase, get connection string from:")
        print("Dashboard > Settings > Database > Connection string > URI")
        return

    try:
        # Test 1: Direct asyncpg
        print("\n--- Test 1: Direct asyncpg connection ---")
        await test_direct_asyncpg()

        # Test 2: Our wrapper
        await test_asyncpg_client()

        # Test 3: Service layer
        await test_project_service()

        print("\n" + "=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
