#!/bin/bash
# Archon Data Migration: Supabase -> k3d PostgreSQL
#
# This script migrates all archon_* tables from Supabase to k3d PostgreSQL.
# It uses the k3d PostgreSQL pod to connect to both databases.
#
# Usage: ./migrate-supabase-to-k3d.sh [--dry-run]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
K8S_CONTEXT="k3d-tailadmin-dev"
NAMESPACE="archon"
POD="postgresql-0"

# Supabase connection (source)
SUPABASE_URL="postgresql://postgres.ndlibgxgrrdbgcxmsleb:Free784dom*@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"

# Local k3d connection (target)
K3D_URL="postgresql://postgres:postgres@localhost:5432/archon"

DRY_RUN=false
if [[ "$1" == "--dry-run" ]]; then
    DRY_RUN=true
    echo -e "${YELLOW}DRY RUN MODE - No data will be modified${NC}"
fi

echo -e "${GREEN}=== Archon Migration: Supabase -> k3d ===${NC}"
echo ""

# Function to run psql in the pod
run_psql() {
    local db_url="$1"
    local query="$2"
    kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" exec "$POD" -- \
        psql "$db_url" -t -A -c "$query" 2>/dev/null
}

# Function to copy table data
copy_table() {
    local table="$1"
    local columns="$2"

    echo -n "  Migrating $table... "

    if $DRY_RUN; then
        local count=$(run_psql "$SUPABASE_URL" "SELECT count(*) FROM $table;")
        echo -e "${YELLOW}[DRY RUN] Would copy $count rows${NC}"
        return
    fi

    # Export from Supabase and import to k3d in one pipe
    kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" exec "$POD" -- \
        psql "$SUPABASE_URL" -c "COPY $table ($columns) TO STDOUT WITH (FORMAT csv, HEADER false, NULL '')" 2>/dev/null | \
    kubectl --context "$K8S_CONTEXT" -n "$NAMESPACE" exec -i "$POD" -- \
        psql "$K3D_URL" -c "COPY $table ($columns) FROM STDIN WITH (FORMAT csv, HEADER false, NULL '')" 2>/dev/null

    local count=$(run_psql "$K3D_URL" "SELECT count(*) FROM $table;")
    echo -e "${GREEN}✓ $count rows${NC}"
}

# Check connectivity
echo "Checking connectivity..."
echo -n "  Supabase: "
if run_psql "$SUPABASE_URL" "SELECT 1;" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ Cannot connect to Supabase${NC}"
    exit 1
fi

echo -n "  k3d PostgreSQL: "
if run_psql "$K3D_URL" "SELECT 1;" > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC}"
else
    echo -e "${RED}✗ Cannot connect to k3d PostgreSQL${NC}"
    exit 1
fi

echo ""
echo "Source data counts:"
run_psql "$SUPABASE_URL" "
SELECT 'sources' as tbl, count(*) FROM archon_sources
UNION ALL SELECT 'crawled_pages', count(*) FROM archon_crawled_pages
UNION ALL SELECT 'code_examples', count(*) FROM archon_code_examples
UNION ALL SELECT 'page_metadata', count(*) FROM archon_page_metadata
UNION ALL SELECT 'projects', count(*) FROM archon_projects
UNION ALL SELECT 'tasks', count(*) FROM archon_tasks
UNION ALL SELECT 'prompts', count(*) FROM archon_prompts
UNION ALL SELECT 'settings', count(*) FROM archon_settings;
"

echo ""
echo "Target data counts (before migration):"
run_psql "$K3D_URL" "
SELECT 'sources' as tbl, count(*) FROM archon_sources
UNION ALL SELECT 'crawled_pages', count(*) FROM archon_crawled_pages
UNION ALL SELECT 'code_examples', count(*) FROM archon_code_examples
UNION ALL SELECT 'page_metadata', count(*) FROM archon_page_metadata
UNION ALL SELECT 'projects', count(*) FROM archon_projects
UNION ALL SELECT 'tasks', count(*) FROM archon_tasks
UNION ALL SELECT 'prompts', count(*) FROM archon_prompts
UNION ALL SELECT 'settings', count(*) FROM archon_settings;
"

if ! $DRY_RUN; then
    echo ""
    echo -e "${YELLOW}This will REPLACE all data in k3d PostgreSQL.${NC}"
    read -p "Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "Starting migration..."

# Step 1: Clear existing data in reverse dependency order
if ! $DRY_RUN; then
    echo "Clearing existing data in k3d..."
    run_psql "$K3D_URL" "
        TRUNCATE archon_tasks CASCADE;
        TRUNCATE archon_projects CASCADE;
        TRUNCATE archon_code_examples CASCADE;
        TRUNCATE archon_crawled_pages CASCADE;
        TRUNCATE archon_page_metadata CASCADE;
        TRUNCATE archon_sources CASCADE;
        TRUNCATE archon_prompts CASCADE;
        TRUNCATE archon_settings CASCADE;
    "
    echo -e "  ${GREEN}✓ Cleared${NC}"
fi

echo ""
echo "Copying tables (in dependency order)..."

# Step 2: Copy tables in correct order

# 1. Settings (no deps)
copy_table "archon_settings" "id,key,value,encrypted_value,is_encrypted,category,description,created_at,updated_at"

# 2. Prompts (no deps)
copy_table "archon_prompts" "id,prompt_name,prompt,description,created_at,updated_at"

# 3. Sources (no deps)
copy_table "archon_sources" "source_id,source_url,source_display_name,summary,total_word_count,title,metadata,created_at,updated_at"

# 4. Page metadata (depends on sources)
copy_table "archon_page_metadata" "id,source_id,url,full_content,section_title,section_order,word_count,char_count,chunk_count,created_at,updated_at,metadata"

# 5. Crawled pages (depends on sources, page_metadata) - includes vector embeddings
copy_table "archon_crawled_pages" "id,url,chunk_number,content,metadata,source_id,embedding_384,embedding_768,embedding_1024,embedding_1536,embedding_3072,llm_chat_model,embedding_model,embedding_dimension,created_at,page_id"

# 6. Code examples (depends on sources) - includes vector embeddings
copy_table "archon_code_examples" "id,url,chunk_number,content,summary,metadata,source_id,embedding_384,embedding_768,embedding_1024,embedding_1536,embedding_3072,llm_chat_model,embedding_model,embedding_dimension,created_at"

# 7. Projects (no deps)
copy_table "archon_projects" "id,title,description,docs,features,data,github_repo,pinned,created_at,updated_at"

# 8. Tasks (depends on projects)
copy_table "archon_tasks" "id,project_id,parent_task_id,title,description,status,assignee,task_order,priority,feature,sources,code_examples,archived,archived_at,archived_by,created_at,updated_at"

# Step 3: Reset sequences
if ! $DRY_RUN; then
    echo ""
    echo "Resetting sequences..."
    run_psql "$K3D_URL" "
        SELECT setval('archon_crawled_pages_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM archon_crawled_pages));
        SELECT setval('archon_code_examples_id_seq', (SELECT COALESCE(MAX(id), 0) + 1 FROM archon_code_examples));
    "
    echo -e "  ${GREEN}✓ Sequences reset${NC}"
fi

echo ""
echo "Target data counts (after migration):"
run_psql "$K3D_URL" "
SELECT 'sources' as tbl, count(*) FROM archon_sources
UNION ALL SELECT 'crawled_pages', count(*) FROM archon_crawled_pages
UNION ALL SELECT 'code_examples', count(*) FROM archon_code_examples
UNION ALL SELECT 'page_metadata', count(*) FROM archon_page_metadata
UNION ALL SELECT 'projects', count(*) FROM archon_projects
UNION ALL SELECT 'tasks', count(*) FROM archon_tasks
UNION ALL SELECT 'prompts', count(*) FROM archon_prompts
UNION ALL SELECT 'settings', count(*) FROM archon_settings;
"

echo ""
echo -e "${GREEN}=== Migration Complete ===${NC}"
