from google.cloud import bigquery
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import functions_framework
import json

PROJECT_ID = "ba-882-fall25-team8"
INPUT_TABLE = "ba-882-fall25-team8.cleaned_github_data.repo_ml_numeric_final"
OUTPUT_TABLE = "ba-882-fall25-team8.ml_results.repo_cluster_summary"


@functions_framework.http
def task(request):
    print("🚀 Starting enriched clustering task...")
    client = bigquery.Client(project=PROJECT_ID)

    # ====== Load ML-ready numeric table ======
    df = client.query(f"SELECT * FROM `{INPUT_TABLE}`").to_dataframe()
    print(f"✅ Loaded {len(df)} rows from {INPUT_TABLE}")

    # Keep repo_name but not used in clustering
    repo_names = df["repo_name"]

    # ====== Prepare numeric features ======
    feature_cols = [c for c in df.columns if c != "repo_name"]
    X = df[feature_cols]

    print(f"🔢 Final feature matrix shape: {X.shape}")

    # ====== Scale ======
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ====== KMeans ======
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    df["cluster"] = clusters

    # ====== Cluster summary ======
    cluster_summary = (
        df.groupby("cluster")[feature_cols]
        .mean()
        .reset_index()
    )

    # ====== Save output ======
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(cluster_summary, OUTPUT_TABLE, job_config=job_config).result()

    print(f"✅ Cluster results written to: {OUTPUT_TABLE}")

    return (
        json.dumps({
            "status": "success",
            "clusters": int(df['cluster'].nunique()),
            "output_table": OUTPUT_TABLE
        }),
        200,
        {"Content-Type": "application/json"},
    )
