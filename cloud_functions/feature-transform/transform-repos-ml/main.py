import functions_framework
from google.cloud import bigquery

PROJECT_ID = "ba-882-fall25-team8"
DATASET = "cleaned_github_data"
OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET}.repo_ml_dataset"

client_bq = bigquery.Client()


@functions_framework.http
def task(request):
    print("🚀 Starting ML dataset aggregation...")

    query = f"""
        CREATE OR REPLACE TABLE `{OUTPUT_TABLE}` AS
        WITH commits AS (
            SELECT
                repo_full_name,
                COUNT(*) AS total_commits,
                AVG(message_length) AS avg_message_length,
                AVG(CASE WHEN is_merge_commit THEN 1 ELSE 0 END) AS merge_commit_ratio,
                AVG(CASE WHEN is_bot_commit THEN 1 ELSE 0 END) AS bot_commit_ratio
            FROM `{PROJECT_ID}.{DATASET}.commits_clean`
            GROUP BY repo_full_name
        ),
        contributors AS (
            SELECT
                repo_full_name,
                COUNT(DISTINCT contributor_login) AS total_contributors,
                AVG(contributions) AS avg_contributions_per_person,
                AVG(CASE WHEN is_core_contributor THEN 1 ELSE 0 END) AS core_contributor_ratio
            FROM `{PROJECT_ID}.{DATASET}.contributors_clean`
            GROUP BY repo_full_name
        )
        SELECT
            r.name AS repo_name,
            r.url AS repo_url,
            r.language AS main_language,
            r.stars AS stars_count,
            r.forks AS forks_count,
            r.watchers,
            r.open_issues,
            r.repo_age_days,
            r.stars_per_day,
            COALESCE(c.total_commits, 0) AS total_commits,
            COALESCE(c.avg_message_length, 0) AS avg_message_length,
            COALESCE(c.merge_commit_ratio, 0) AS merge_commit_ratio,
            COALESCE(c.bot_commit_ratio, 0) AS bot_commit_ratio,
            COALESCE(ct.total_contributors, 0) AS total_contributors,
            COALESCE(ct.avg_contributions_per_person, 0) AS avg_contributions_per_person,
            COALESCE(ct.core_contributor_ratio, 0) AS core_contributor_ratio,
            COALESCE(l.primary_language, r.language) AS primary_language,
            COALESCE(l.primary_language_pct, 0.0) AS primary_language_pct,
            COALESCE(l.language_count, 0) AS language_count,
            COALESCE(l.languages_above_10_percent, 0) AS languages_above_10_percent,
            COALESCE(l.has_javascript, FALSE) AS has_javascript,
            COALESCE(l.has_compiled_language, FALSE) AS has_compiled_language,
            SAFE_DIVIDE(c.total_commits, NULLIF(ct.total_contributors, 0)) AS commits_per_contributor,
            SAFE_DIVIDE(r.forks, NULLIF(r.stars, 0)) AS forks_per_star,
            CURRENT_TIMESTAMP() AS transformed_at
        FROM `{PROJECT_ID}.{DATASET}.repos_summary` r
        LEFT JOIN commits c
            ON r.name = c.repo_full_name
        LEFT JOIN contributors ct
            ON r.name = ct.repo_full_name
        LEFT JOIN `{PROJECT_ID}.{DATASET}.language_summary` l
            ON r.name = l.repo_full_name
    """

    try:
        job = client_bq.query(query)
        job.result()
        print(f"✅ Successfully created table: {OUTPUT_TABLE}")

        result = list(client_bq.query(
            f"SELECT COUNT(*) AS total_rows FROM `{OUTPUT_TABLE}`"
        ).result())[0]
        row_count = result.total_rows

        return {
            "message": f"Repo ML dataset created successfully with {row_count} rows.",
            "output_table": OUTPUT_TABLE
        }, 200

    except Exception as e:
        print("❌ Error:", str(e))
        return {"status": "error", "details": str(e)}, 500
