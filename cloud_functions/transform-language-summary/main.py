import functions_framework
from google.cloud import bigquery

PROJECT_ID = "ba-882-fall25-team8"
RAW_DATASET = "raw_github_data"
CLEAN_DATASET = "cleaned_github_data"

@functions_framework.http
def task(request):
    """Transform language data into summary with percentages and diversity metrics"""
    
    print("Starting language transformation...")
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
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CLEAN_DATASET}.language_summary` AS
    WITH language_totals AS (
      SELECT 
        repo_full_name,
        SUM(bytes) as total_bytes
      FROM `{PROJECT_ID}.{RAW_DATASET}.github_languages_raw`
      GROUP BY repo_full_name
    ),
    language_details AS (
      SELECT 
        l.repo_full_name,
        l.language,
        l.bytes,
        ROUND(l.bytes / lt.total_bytes * 100, 2) as percentage,
        ROW_NUMBER() OVER (PARTITION BY l.repo_full_name ORDER BY l.bytes DESC) as language_rank
      FROM `{PROJECT_ID}.{RAW_DATASET}.github_languages_raw` l
      JOIN language_totals lt ON l.repo_full_name = lt.repo_full_name
    )
    SELECT 
      repo_full_name,
      COUNT(DISTINCT language) as language_count,
      MAX(CASE WHEN language_rank = 1 THEN language END) as primary_language,
      MAX(CASE WHEN language_rank = 1 THEN percentage END) as primary_language_pct,
      MAX(CASE WHEN language_rank = 2 THEN language END) as secondary_language,
      MAX(CASE WHEN language_rank = 2 THEN percentage END) as secondary_language_pct,
      MAX(CASE WHEN language_rank = 3 THEN language END) as tertiary_language,
      MAX(CASE WHEN language_rank = 3 THEN percentage END) as tertiary_language_pct,
      STRING_AGG(language ORDER BY bytes DESC LIMIT 5) as top_5_languages,
      SUM(bytes) as total_bytes,
      
      -- Factual metrics only
      ROUND(MAX(CASE WHEN language_rank = 1 THEN percentage END) - 
            COALESCE(MAX(CASE WHEN language_rank = 2 THEN percentage END), 0), 2) as language_dominance_gap,
      
      COUNTIF(percentage > 5) as languages_above_5_percent,
      COUNTIF(percentage > 10) as languages_above_10_percent,
      COUNTIF(bytes > 1000) as languages_above_1000_bytes,
      MIN(percentage) as smallest_language_pct,
      MAX(percentage) as largest_language_pct,
      
      -- Language type flags
      COUNTIF(language IN ('HTML', 'CSS')) > 0 as has_web_markup,
      COUNTIF(language IN ('JavaScript', 'TypeScript')) > 0 as has_javascript,
      COUNTIF(language IN ('Python', 'Ruby', 'PHP', 'Perl')) > 0 as has_scripting_language,
      COUNTIF(language IN ('C', 'C++', 'Java', 'C#', 'Go', 'Rust')) > 0 as has_compiled_language,
      COUNTIF(language IN ('Shell', 'Batchfile', 'PowerShell')) > 0 as has_shell_scripts,
      
      CURRENT_TIMESTAMP() as transformed_at
      
    FROM language_details
    GROUP BY repo_full_name
    """
    
    print("Executing transformation query...")
    job = client.query(query)
    result = job.result()
    
    # Get row count
    count_query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{CLEAN_DATASET}.language_summary`"
    count_result = client.query(count_query).result()
    row_count = list(count_result)[0].count
    
    print(f"✓ Created language_summary table with {row_count} rows")
    
    return {
        "message": f"Language transformation complete! Created {row_count} summaries.",
        "table": f"{PROJECT_ID}.{CLEAN_DATASET}.language_summary",
        "row_count": row_count
    }, 200