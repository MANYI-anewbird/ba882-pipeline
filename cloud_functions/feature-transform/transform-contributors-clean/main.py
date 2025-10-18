# === Function: Transform Contributors Clean ===
import functions_framework
from google.cloud import bigquery
import datetime

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SOURCE_DATASET = "raw_github_data"
TARGET_DATASET = "cleaned_github_data"
SOURCE_TABLE = "github_contributors_raw"
TARGET_TABLE = "contributors_clean"

@functions_framework.http
def task(request):
    """
    Cloud Function: Transform raw contributors into cleaned table
    - Deduplication (same repo+contributor+date keeps only one record)
    - Handle NULL values
    - Standardize field names
    - Add data quality flags
    - Calculate derived fields (contribution_rank)
    """
    print("Starting contributors_clean transformation...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Step 1: Ensure target dataset exists
    target_dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{TARGET_DATASET}")
    target_dataset_ref.location = "US"
    
    try:
        bq_client.get_dataset(target_dataset_ref)
        print(f"Dataset {TARGET_DATASET} already exists")
    except:
        bq_client.create_dataset(target_dataset_ref)
        print(f"Created dataset: {TARGET_DATASET}")
    
    # Step 2: Get parameters
    run_date = request.args.get("date") or datetime.datetime.utcnow().strftime("%Y%m%d")
    limit = int(request.args.get("limit", 10000))
    
    print(f"Processing snapshot_date={run_date}, limit={limit}")
    
    # Step 3: Transform query with deduplication and ranking
    transform_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` AS
    WITH deduplicated_contributors AS (
        SELECT 
            repo_full_name,
            COALESCE(contributor_login, 'unknown_contributor') as contributor_login,
            contributor_id,
            contributions,
            html_url,
            followers_url,
            snapshot_date,
            -- Deduplication: keep only the record with highest contributions for same repo+contributor+date
            ROW_NUMBER() OVER (
                PARTITION BY repo_full_name, contributor_login, snapshot_date 
                ORDER BY contributions DESC
            ) as row_num
        FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
        WHERE snapshot_date = {run_date}
    ),
    ranked_contributors AS (
        SELECT 
            repo_full_name,
            contributor_login,
            contributor_id,
            contributions,
            html_url,
            followers_url,
            snapshot_date,
            -- Calculate contribution rank within each repo
            ROW_NUMBER() OVER (
                PARTITION BY repo_full_name 
                ORDER BY contributions DESC
            ) as contribution_rank,
            -- Data quality flag
            CASE 
                WHEN contributor_login = 'unknown_contributor' THEN 'missing_login'
                WHEN contributions < 1 THEN 'invalid_contributions'
                WHEN contributor_id IS NULL THEN 'missing_id'
                ELSE 'valid'
            END as data_quality_flag
        FROM deduplicated_contributors
        WHERE row_num = 1  -- Keep only deduplicated records
    )
    SELECT 
        repo_full_name,
        contributor_login,
        contributor_id,
        contributions,
        html_url,
        followers_url,
        snapshot_date,
        contribution_rank,
        data_quality_flag,
        -- Flag for core contributors (top 10)
        CASE WHEN contribution_rank <= 10 THEN TRUE ELSE FALSE END as is_core_contributor,
        -- Contribution percentage for analysis
        ROUND(contributions / SUM(contributions) OVER (PARTITION BY repo_full_name) * 100, 2) as contribution_percentage
    FROM ranked_contributors
    WHERE data_quality_flag = 'valid'  -- Keep only valid data
    ORDER BY repo_full_name, contribution_rank
    LIMIT {limit}
    """
    
    # Step 4: Execute transformation
    try:
        query_job = bq_client.query(transform_query)
        query_job.result()  # Wait for completion
        
        # Get statistics
        stats_query = f"""
        SELECT 
            COUNT(*) as total_contributors,
            COUNT(DISTINCT repo_full_name) as total_repos,
            COUNT(DISTINCT contributor_login) as unique_contributors,
            SUM(CASE WHEN is_core_contributor THEN 1 ELSE 0 END) as core_contributors
        FROM `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`
        """
        stats_result = list(bq_client.query(stats_query).result())[0]
        
        print(f"Successfully transformed contributors")
        print(f"Created/replaced table: {TARGET_DATASET}.{TARGET_TABLE}")
        
        return {
            "message": "Contributors clean transformation completed successfully!",
            "dataset": TARGET_DATASET,
            "table": TARGET_TABLE,
            "total_contributors": stats_result.total_contributors,
            "total_repos": stats_result.total_repos,
            "unique_contributors": stats_result.unique_contributors,
            "core_contributors": stats_result.core_contributors,
            "snapshot_date": run_date
        }, 200
        
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}"
        print(error_msg)
        return {"error": error_msg}, 500
