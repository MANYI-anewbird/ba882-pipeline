from google.cloud import bigquery
import pandas as pd
import numpy as np
import functions_framework
import json
import time

PROJECT_ID = "ba-882-fall25-team8"
INPUT_TABLE = "ba-882-fall25-team8.cleaned_github_data.repo_ml_ready"
OUTPUT_TABLE = "ba-882-fall25-team8.cleaned_github_data.repo_ml_numeric_final"


# --- simple label encoder (no sklearn needed) ---
def simple_label_encode(series):
    unique_vals = series.dropna().unique()
    mapping = {val: idx + 1 for idx, val in enumerate(unique_vals)}  # start from 1
    return series.map(mapping).fillna(0).astype(int)


@functions_framework.http
def task(request):
    print("🚀 Starting FINAL ML numeric transform...")
    client = bigquery.Client(project=PROJECT_ID)

    # ====== Load Data ======
    df = client.query(f"SELECT * FROM `{INPUT_TABLE}`").to_dataframe()
    print(f"📥 Loaded {len(df)} rows with {df.shape[1]} columns")

    # ====== Define column groups ======
    numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns.tolist()
    bool_cols = df.select_dtypes(include=["bool"]).columns.tolist()
    string_cols = df.select_dtypes(include=["object"]).columns.tolist()
    timestamp_cols = df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()

    print(f"🔢 Numeric columns: {len(numeric_cols)}")
    print(f"🔘 Boolean columns: {len(bool_cols)}")
    print(f"🔤 String columns: {len(string_cols)}")
    print(f"⏳ Timestamp columns: {len(timestamp_cols)}")

    # ====== Create output dataframe ======
    final_df = pd.DataFrame()
    final_df["repo_name"] = df["repo_name"]  # keep for reference, not used in ML

    # ====== 1. numeric columns keep ======
    for col in numeric_cols:
        final_df[col] = df[col].fillna(0)

    # ====== 2. boolean → int ======
    for col in bool_cols:
        final_df[col] = df[col].astype(int)

    # ====== 3. timestamp → unix time ======
    for col in timestamp_cols:
        final_df[col] = df[col].astype("int64") // 1_000_000_000  # convert ns → seconds

    # ====== 4. string → label encoding ======
    for col in string_cols:
        final_df[col + "_label"] = simple_label_encode(df[col])

    print(f"🧪 Final feature count (columns): {final_df.shape[1]}")

    # ====== Write to BigQuery ======
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(final_df, OUTPUT_TABLE, job_config=job_config).result()

    print(f"✅ ML numeric table written to {OUTPUT_TABLE}")

    return (
        json.dumps({
            "status": "success",
            "rows": len(final_df),
            "output_table": OUTPUT_TABLE,
            "columns": final_df.shape[1]
        }),
        200,
        {"Content-Type": "application/json"},
    )
