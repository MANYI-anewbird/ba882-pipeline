from google.cloud import bigquery
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import functions_framework
import json

PROJECT_ID = "ba-882-fall25-team8"
INPUT_TABLE = "ba-882-fall25-team8.cleaned_github_data.repo_ml_dataset"
OUTPUT_TABLE = "ba-882-fall25-team8.ml_results.repo_cluster_summary"

@functions_framework.http
def task(request):
    print("🚀 Starting repository clustering task...")
    client = bigquery.Client(project=PROJECT_ID)

    # ====== Step 1. Load numeric features only ======
    query = f"""
        SELECT 
            repo_name,
            stars_count,
            forks_count,
            watchers,
            open_issues,
            repo_age_days,
            stars_per_day,
            total_commits,
            avg_message_length,
            merge_commit_ratio,
            bot_commit_ratio,
            total_contributors,
            avg_contributions_per_person,
            core_contributor_ratio,
            primary_language_pct,
            language_count,
            languages_above_10_percent,
            commits_per_contributor,
            forks_per_star
        FROM `{INPUT_TABLE}`
        WHERE stars_count IS NOT NULL
    """

    df = client.query(query).to_dataframe()
    print(f"✅ Loaded {len(df)} rows from {INPUT_TABLE}")

    # ====== Step 2. Preprocess ======
    numeric_cols = [
        c for c in df.columns if c != "repo_name"
    ]
    X = df[numeric_cols].fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ====== Step 3. Clustering ======
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df["cluster"] = kmeans.fit_predict(X_scaled)

    # ====== Step 4. Compute cluster summary ======
    cluster_summary = (
        df.groupby("cluster")
        .mean(numeric_only=True)
        .reset_index()
    )

    # ====== Step 5. Write to BigQuery ======
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    client.load_table_from_dataframe(
        df, OUTPUT_TABLE, job_config=job_config
    ).result()

    print(f"✅ Cluster results written to {OUTPUT_TABLE}")

    # ====== Step 6. Return result ======
    return (
        json.dumps({
            "status": "success",
            "rows": len(df),
            "clusters": df['cluster'].nunique(),
            "output_table": OUTPUT_TABLE
        }),
        200,
        {"Content-Type": "application/json"},
    )
