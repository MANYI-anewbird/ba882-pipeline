# === Function: Transform Repos Summary (Cleaned Data) ===
import functions_framework
from google.cloud import bigquery
import datetime

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SOURCE_DATASET = "raw_github_data"
TARGET_DATASET = "cleaned_github_data"
SOURCE_TABLE = "github_repos_raw"
TARGET_TABLE = "repos_summary"

@functions_framework.http
def task(request):
    """
    Cloud Function: Transform raw repos into cleaned summary table
    Selects key fields and applies data quality improvements
    """
    print("Starting repos_summary transformation...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Step 1: Ensure target dataset exists
    target_dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{TARGET_DATASET}")
    target_dataset_ref.location = "US"
    
    try:
        bq_client.get_dataset(target_dataset_ref)
        print(f"success:ataset {TARGET_DATASET} already exists")
    except:
        bq_client.create_dataset(target_dataset_ref)
        print(f"success:Created dataset: {TARGET_DATASET}")
    
    # Step 2: Get parameters
    run_date = request.args.get("date") or datetime.datetime.utcnow().strftime("%Y%m%d")
    limit = int(request.args.get("limit", 1000))
    
    print(f"Processing snapshot_date={run_date}, limit={limit}")
    
    # Step 3: Transform query
    transform_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` AS
    SELECT 
        full_name as name,
        stargazers_count as stars,
        forks_count as forks,
        COALESCE(language, 'Unknown') as language,
        COALESCE(description, 'No description provided') as description,
        html_url as url,
        updated_at,
        snapshot_date,
        created_at,
        
        -- Additional useful fields
        watchers_count as watchers,
        open_issues_count as open_issues,
        
        -- Computed fields
        DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) as repo_age_days,
        ROUND(stargazers_count / NULLIF(DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY), 0), 2) as stars_per_day
        
    FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
    WHERE snapshot_date = {run_date}
    ORDER BY stargazers_count DESC
    LIMIT {limit}
    """
    
    # Step 4: Execute transformation
    try:
        query_job = bq_client.query(transform_query)
        query_job.result()  # Wait for completion
        
        # Get row count
        count_query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`"
        count_result = list(bq_client.query(count_query).result())[0]
        row_count = count_result.cnt
        
        print(f"Successfully transformed {row_count} repos")
        print(f"Created/replaced table: {TARGET_DATASET}.{TARGET_TABLE}")
        
        return {
            "message": "Repos summary transformation completed successfully!",
            "dataset": TARGET_DATASET,
            "table": TARGET_TABLE,
            "rows_processed": row_count,
            "snapshot_date": run_date
        }, 200
        
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}"
        print(error_msg)
        return {"error": error_msg}, 500
