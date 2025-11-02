# === Function: Transform Repos for Machine Learning ===
import functions_framework
from google.cloud import bigquery
import datetime

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SOURCE_DATASET = "raw_github_data"
TARGET_DATASET = "ml_github_data"
SOURCE_TABLE = "github_repos_raw"
TARGET_TABLE = "repos_for_ml"

@functions_framework.http
def task(request):
    """
    Cloud Function: Create ML-ready repos dataset
    - Deduplicates across all historical snapshots
    - Keeps only the latest version of each unique repo
    - Provides comprehensive dataset for machine learning
    """
    print("Starting repos ML transformation...")
    
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
    
    # Step 2: Transform query - deduplicate across all history
    transform_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}` AS
    WITH ranked_repos AS (
        SELECT 
            full_name,
            stargazers_count,
            forks_count,
            COALESCE(language, 'Unknown') as language,
            COALESCE(description, 'No description provided') as description,
            html_url,
            updated_at,
            snapshot_date,
            created_at,
            watchers_count,
            open_issues_count,
            open_issues,
            size,
            default_branch,
            topics,
            has_issues,
            has_projects,
            has_wiki,
            has_pages,
            has_downloads,
            archived,
            disabled,
            visibility,
            pushed_at,
            -- Rank: latest version of each repo
            ROW_NUMBER() OVER (
                PARTITION BY full_name 
                ORDER BY snapshot_date DESC, updated_at DESC
            ) as row_num
        FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
    )
    SELECT 
        full_name as name,
        stargazers_count as stars,
        forks_count as forks,
        language,
        description,
        html_url as url,
        updated_at,
        snapshot_date,
        created_at,
        watchers_count as watchers,
        open_issues_count as open_issues,
        size as repo_size_kb,
        default_branch,
        topics,
        has_issues,
        has_projects,
        has_wiki,
        has_pages,
        has_downloads,
        archived,
        disabled,
        visibility,
        pushed_at,
        
        -- Computed fields for ML
        DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) as repo_age_days,
        DATE_DIFF(CURRENT_DATE(), DATE(pushed_at), DAY) as days_since_last_push,
        DATE_DIFF(DATE(updated_at), DATE(created_at), DAY) as days_active,
        ROUND(stargazers_count / NULLIF(DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY), 0), 2) as stars_per_day,
        ROUND(forks_count / NULLIF(DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY), 0), 2) as forks_per_day,
        ROUND(SAFE_DIVIDE(stargazers_count, NULLIF(forks_count, 0)), 2) as stars_to_forks_ratio,
        
        -- Categorical features
        CASE 
            WHEN language IS NULL OR language = 'Unknown' THEN 'Other'
            ELSE language 
        END as language_clean,
        
        CASE 
            WHEN stargazers_count >= 50000 THEN 'Very High'
            WHEN stargazers_count >= 20000 THEN 'High'
            WHEN stargazers_count >= 10000 THEN 'Medium'
            ELSE 'Low'
        END as popularity_tier,
        
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(pushed_at), DAY) <= 7 THEN 'Very Active'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(pushed_at), DAY) <= 30 THEN 'Active'
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(pushed_at), DAY) <= 90 THEN 'Moderate'
            ELSE 'Inactive'
        END as activity_level
        
    FROM ranked_repos
    WHERE row_num = 1
    ORDER BY stars DESC
    """
    
    # Step 3: Execute transformation
    try:
        query_job = bq_client.query(transform_query)
        query_job.result()  # Wait for completion
        
        # Get statistics
        stats_query = f"""
        SELECT 
            COUNT(*) as total_repos,
            COUNT(DISTINCT language_clean) as unique_languages,
            COUNT(DISTINCT snapshot_date) as snapshots_covered,
            MIN(snapshot_date) as earliest_snapshot,
            MAX(snapshot_date) as latest_snapshot,
            AVG(stars) as avg_stars,
            AVG(forks) as avg_forks
        FROM `{PROJECT_ID}.{TARGET_DATASET}.{TARGET_TABLE}`
        """
        stats_result = list(bq_client.query(stats_query).result())[0]
        
        print(f"Successfully created ML dataset")
        print(f"Total unique repos: {stats_result.total_repos}")
        
        return {
            "message": "ML repos dataset created successfully!",
            "dataset": TARGET_DATASET,
            "table": TARGET_TABLE,
            "total_repos": stats_result.total_repos,
            "unique_languages": stats_result.unique_languages,
            "snapshots_covered": stats_result.snapshots_covered,
            "earliest_snapshot": str(stats_result.earliest_snapshot),
            "latest_snapshot": str(stats_result.latest_snapshot),
            "avg_stars": float(stats_result.avg_stars),
            "avg_forks": float(stats_result.avg_forks)
        }, 200
        
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}"
        print(error_msg)
        return {"error": error_msg}, 500
