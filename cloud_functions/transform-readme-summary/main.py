import functions_framework
from google.cloud import bigquery
import base64
import re

PROJECT_ID = "ba-882-fall25-team8"
RAW_DATASET = "raw_github_data"
CLEAN_DATASET = "cleaned_github_data"

@functions_framework.http
def task(request):
    """Transform README data into summary metrics with decoded content"""
    
    print("Starting README transformation...")
    client = bigquery.Client()
    
    # Ensure cleaned dataset exists
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{CLEAN_DATASET}")
    try:
        client.get_dataset(dataset_ref)
        print(f"✓ Dataset {CLEAN_DATASET} exists")
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)
        print(f"✓ Created dataset {CLEAN_DATASET}")
    
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CLEAN_DATASET}.readme_summary` AS
    WITH decoded_readmes AS (
      SELECT 
        repo_full_name,
        name as readme_filename,
        size as readme_size_bytes,
        encoding,
        SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(content)) as decoded_content,
        extracted_at,
        snapshot_date
      FROM `{PROJECT_ID}.{RAW_DATASET}.github_readme_raw`
      WHERE content IS NOT NULL
    )
    SELECT 
      repo_full_name,
      readme_filename,
      readme_size_bytes,
      encoding,
      
      -- Content metrics
      LENGTH(decoded_content) as content_length,
      ARRAY_LENGTH(SPLIT(decoded_content, ' ')) as word_count_estimate,
      ARRAY_LENGTH(SPLIT(decoded_content, '\\n')) as line_count,
      
      -- Structure analysis
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(decoded_content, r'#')) as header_count,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(decoded_content, r'```')) / 2 as code_block_count,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(decoded_content, r'\\[')) as link_count,
      ARRAY_LENGTH(REGEXP_EXTRACT_ALL(decoded_content, r'!\\[')) as image_count,
      
      -- Section presence
      REGEXP_CONTAINS(LOWER(decoded_content), r'installation|install|setup') as has_installation,
      REGEXP_CONTAINS(LOWER(decoded_content), r'usage|example|quickstart') as has_usage,
      REGEXP_CONTAINS(LOWER(decoded_content), r'contribut') as has_contributing,
      REGEXP_CONTAINS(LOWER(decoded_content), r'license|copyright') as has_license,
      REGEXP_CONTAINS(LOWER(decoded_content), r'test') as has_tests,
      REGEXP_CONTAINS(LOWER(decoded_content), r'documentation|docs|api') as has_documentation,
      REGEXP_CONTAINS(LOWER(decoded_content), r'badge|shield') as has_badges,
      
      -- Quality score (0-100)
      LEAST(100, 
        -- Length points (max 30)
        LEAST(30, CAST(LENGTH(decoded_content) / 100 AS INT64)) +
        
        -- Header points (max 20)
        LEAST(20, ARRAY_LENGTH(REGEXP_EXTRACT_ALL(decoded_content, r'#')) * 2) +
        
        -- Section completeness (max 50)
        (CASE WHEN REGEXP_CONTAINS(LOWER(decoded_content), r'installation|install') THEN 15 ELSE 0 END) +
        (CASE WHEN REGEXP_CONTAINS(LOWER(decoded_content), r'usage|example') THEN 15 ELSE 0 END) +
        (CASE WHEN REGEXP_CONTAINS(LOWER(decoded_content), r'contribut') THEN 10 ELSE 0 END) +
        (CASE WHEN REGEXP_CONTAINS(LOWER(decoded_content), r'license') THEN 10 ELSE 0 END)
      ) as documentation_score,
      
      -- Content preview
      SUBSTR(decoded_content, 1, 500) as content_preview,
      
      extracted_at,
      snapshot_date
      
    FROM decoded_readmes
    WHERE decoded_content IS NOT NULL
    """
    
    print("Executing transformation query...")
    job = client.query(query)
    result = job.result()
    
    # Get row count
    count_query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{CLEAN_DATASET}.readme_summary`"
    count_result = client.query(count_query).result()
    row_count = list(count_result)[0].count
    
    print(f"✓ Created readme_summary table with {row_count} rows")
    
    return {
        "message": f"README transformation complete! Created {row_count} summaries.",
        "table": f"{PROJECT_ID}.{CLEAN_DATASET}.readme_summary",
        "row_count": row_count
    }, 200