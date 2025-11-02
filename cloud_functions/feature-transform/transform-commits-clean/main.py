# === Function: Transform Commits Clean ===
import functions_framework
from google.cloud import bigquery
import datetime

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SOURCE_DATASET = "raw_github_data"
TARGET_DATASET = "cleaned_github_data"
SOURCE_TABLE = "github_commits_raw"
TARGET_TABLE = "commits_clean"

@functions_framework.http
def task(request):
    """
    Cloud Function: Transform raw commits into cleaned table
    """
    print("Starting commits_clean transformation...")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    target_dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{TARGET_DATASET}")
    target_dataset_ref.location = "US"
    
    try:
        bq_client.get_dataset(target_dataset_ref)
        print(f"Dataset {TARGET_DATASET} already exists")
    except:
        bq_client.create_dataset(target_dataset_ref)
        print(f"Created dataset: {TARGET_DATASET}")
    
   
    run_date = request.args.get("date") or datetime.datetime.utcnow().strftime("%Y%m%d")
    repo_limit = int(request.args.get("limit", 300))  
    commit_limit = repo_limit * 100  
    
    print(f"Processing snapshot_date={run_date}, repo_limit={repo_limit}, commit_limit={commit_limit}")
    
    transform_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` AS
    WITH deduplicated_commits AS (
        SELECT 
            repo_full_name,
            sha,
            commit_message,
            author_name,
            LOWER(TRIM(author_email)) as author_email_clean,
            commit_date,
            html_url,
            snapshot_date,
            ROW_NUMBER() OVER (PARTITION BY sha ORDER BY commit_date DESC) as row_num
        FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
        WHERE snapshot_date = {run_date}
    ),
    parsed_commits AS (
        SELECT 
            repo_full_name,
            sha,
            commit_message,
            author_name,
            author_email_clean,
            TIMESTAMP(commit_date) as commit_timestamp,
            EXTRACT(DATE FROM commit_date) as commit_date_only,
            EXTRACT(HOUR FROM commit_date) as commit_hour,
            EXTRACT(DAYOFWEEK FROM commit_date) as commit_day_of_week,
            html_url,
            snapshot_date,
            CASE 
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^feat[:(]') THEN 'feat'
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^fix[:(]') THEN 'fix'
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^docs[:(]') THEN 'docs'
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^refactor[:(]') THEN 'refactor'
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^test[:(]') THEN 'test'
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^chore[:(]') THEN 'chore'
                ELSE 'other'
            END as commit_type,
            CASE 
                WHEN REGEXP_CONTAINS(LOWER(author_name), r'bot|dependabot|renovate|github-actions') THEN TRUE
                WHEN REGEXP_CONTAINS(LOWER(author_email_clean), r'bot|noreply|automated') THEN TRUE
                ELSE FALSE
            END as is_bot_commit,
            CASE 
                WHEN REGEXP_CONTAINS(author_email_clean, r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}$') THEN TRUE
                ELSE FALSE
            END as is_valid_email,
            LENGTH(commit_message) as message_length,
            CASE 
                WHEN REGEXP_CONTAINS(LOWER(commit_message), r'^merge|^merge pull request') THEN TRUE
                ELSE FALSE
            END as is_merge_commit
        FROM deduplicated_commits
        WHERE row_num = 1
    )
    SELECT 
        repo_full_name,
        sha,
        commit_message,
        author_name,
        author_email_clean as author_email,
        commit_timestamp,
        commit_date_only,
        commit_hour,
        commit_day_of_week,
        html_url,
        snapshot_date,
        commit_type,
        is_bot_commit,
        is_valid_email,
        message_length,
        is_merge_commit,
        CASE 
            WHEN is_bot_commit THEN 'bot_commit'
            WHEN NOT is_valid_email THEN 'invalid_email'
            WHEN message_length < 10 THEN 'short_message'
            ELSE 'valid'
        END as data_quality_flag
    FROM parsed_commits
    WHERE NOT is_bot_commit
    ORDER BY commit_timestamp DESC
    LIMIT {commit_limit}
    """  # 
    
    try:
        query_job = bq_client.query(transform_query)
        query_job.result()
        
        stats_query = f"""
        SELECT 
            COUNT(*) as total_commits,
            COUNT(DISTINCT repo_full_name) as total_repos,
            COUNT(DISTINCT author_email) as unique_authors,
            SUM(CASE WHEN is_merge_commit THEN 1 ELSE 0 END) as merge_commits
        FROM `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`
        """
        stats_result = list(bq_client.query(stats_query).result())[0]
        
        print(f"Successfully transformed commits")
        
        return {
            "message": "Commits clean transformation completed successfully!",
            "dataset": TARGET_DATASET,
            "table": TARGET_TABLE,
            "total_commits": stats_result.total_commits,
            "total_repos": stats_result.total_repos,
            "unique_authors": stats_result.unique_authors,
            "merge_commits": stats_result.merge_commits,
            "snapshot_date": run_date,
            "repo_limit": repo_limit,
            "commit_limit": commit_limit
        }, 200
        
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}"
        print(error_msg)
        return {"error": error_msg}, 500