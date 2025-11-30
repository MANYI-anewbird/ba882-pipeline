import functions_framework
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = "ba-882-fall25-team8"
DATASET = "cleaned_github_data"

ML_TABLE = f"{PROJECT_ID}.{DATASET}.repo_ml_dataset"
LLM_TABLE = f"{PROJECT_ID}.{DATASET}.repo_llm_enriched"
OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET}.repo_ml_ready"


@functions_framework.http
def task(request):
    client = bigquery.Client()

    print("🚀 Building ML-ready table...")

    # Load repo_ml_dataset
    df_ml = client.query(f"""
        SELECT *
        FROM `{ML_TABLE}`
    """).to_dataframe()

    # Load llm enrich table
    df_llm = client.query(f"""
        SELECT *
        FROM `{LLM_TABLE}`
    """).to_dataframe()

    print(f"📌 Loaded ML: {len(df_ml)} rows")
    print(f"📌 Loaded LLM: {len(df_llm)} rows")

    # Merge on repo_name
    df_merged = df_ml.merge(df_llm, on="repo_name", how="left")

    print(f"🔗 Merged rows: {len(df_merged)}")

    # Convert lists into strings so BigQuery accepts them
    list_cols = ["audience", "tech_stack", "use_cases"]

    for col in list_cols:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].astype(str)

    # Write result to BigQuery
    job = client.load_table_from_dataframe(
        df_merged,
        OUTPUT_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    print(f"✅ ML-ready table written to {OUTPUT_TABLE}")

    return {
        "message": "ML-ready table built!",
        "rows": len(df_merged),
        "output_table": OUTPUT_TABLE
    }
