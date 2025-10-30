# === parse_non_partitioned.py ===
import functions_framework
import json
import datetime
from google.cloud import storage, bigquery

PROJECT_ID = "ba-882-fall25-team8"
BUCKET_NAME = "ba882-t8-github"
DATASET_ID = "raw_github_data"

@functions_framework.http
def task(request):
    """
    Cloud Function:
    Parse GitHub JSON files (arrays) from GCS and load them into BigQuery (non-partitioned, append mode)
    """
    print("Starting raw-parse-github (non-partitioned)...")

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"success:Dataset {DATASET_ID} already exists.")
    except Exception:
        bq_client.create_dataset(dataset_ref, exists_ok=True)
        print(f"success:Created dataset: {DATASET_ID}")

    run_date = request.args.get("date") or datetime.datetime.utcnow().strftime("%Y%m%d")
    limit = request.args.get("limit", "300")
    print(f"Parsing snapshot {run_date}, limit={limit}")

    files = {
        "github_repos_raw": f"raw/github_repos_raw/date={run_date}/repos_{limit}.json",
        "github_contributors_raw": f"raw/github_contributors_raw/date={run_date}/contributors_{limit}.json",
        "github_commits_raw": f"raw/github_commits_raw/date={run_date}/commits_{limit}.json",
        "github_readme_raw": f"raw/github_readme_raw/date={run_date}/readmes_{limit}.json",
        "github_languages_raw": f"raw/github_languages_raw/date={run_date}/languages_{limit}.json",
    }

    for table_name, file_path in files.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)

        print(f"Reading from: gs://{BUCKET_NAME}/{file_path}")
        try:
            raw_data = json.loads(blob.download_as_text())
        except Exception as e:
            print(f"Failed to read or parse {file_path}: {e}")
            continue

        if isinstance(raw_data, dict):
            raw_data = [raw_data]

        print(f"success: Loaded {len(raw_data)} records from {file_path}")

        # Add snapshot_date
        for row in raw_data:
            row["snapshot_date"] = int(run_date)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
        )

        try:
            load_job = bq_client.load_table_from_json(raw_data, table_id, job_config=job_config)
            load_job.result()
            print(f"success: Loaded {table_name} ({len(raw_data)} rows)")
        except Exception as e:
            print(f"Error loading {table_name}: {e}")

    print("All JSON files parsed and appended to BigQuery successfully!")
    return {"message": f"All GitHub data parsed for {run_date}!"}, 200
