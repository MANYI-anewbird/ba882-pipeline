import functions_framework
import json
import datetime
from google.cloud import storage, bigquery

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
BUCKET_NAME = "ba882-t8-github"
DATASET_ID = "raw_github_data"

# === MAIN FUNCTION ===
@functions_framework.http
def task(request):
    """
    Cloud Function:
    Parse GitHub JSON files (arrays) from GCS and load them into BigQuery (append mode)
    """
    print("Starting raw-parse-github task...")

    # Step 1️ - Initialize clients
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Step 2️ - Ensure dataset exists
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} already exists.")
    except Exception:
        bq_client.create_dataset(dataset_ref, exists_ok=True)
        print(f"Created dataset: {DATASET_ID}")

    # Step 3️ - Get current date (or manual override)
    run_date = request.args.get("date") or datetime.datetime.utcnow().strftime("%Y%m%d")

    # Step 4️ - Define JSON source files
    files = {
        "github_repos_raw": f"raw/github_repos_raw/date={run_date}/repos_200.json",
        "github_contributors_raw": f"raw/github_contributors_raw/date={run_date}/contributors.json",
        "github_commits_raw": f"raw/github_commits_raw/date={run_date}/commits.json",
    }

    # Step 5️ - Loop through each JSON file and load to BigQuery
    for table_name, file_path in files.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(file_path)

        print(f"Reading from GCS: gs://{BUCKET_NAME}/{file_path}")

        try:
            raw_text = blob.download_as_text()
            raw_data = json.loads(raw_text)
        except Exception as e:
            print(f"Failed to read or parse {file_path}: {e}")
            continue

        # Normalize single object
        if isinstance(raw_data, dict):
            raw_data = [raw_data]

        print(f"Loaded {len(raw_data)} records from {file_path}")

        # Add snapshot_date
        for row in raw_data:
            row["snapshot_date"] = int(run_date)

        # Configure BigQuery load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",  # Append instead of overwrite
            time_partitioning=bigquery.TimePartitioning(  # Partition by snapshot_date
                type_=bigquery.TimePartitioningType.DAY,
                field="snapshot_date",
            ),
        )

        # Upload JSON directly
        try:
            load_job = bq_client.load_table_from_json(raw_data, table_id, job_config=job_config)
            load_job.result()
            print(f"Loaded {table_name} to BigQuery ({len(raw_data)} rows).")
        except Exception as e:
            print(f"Error loading {table_name} to BigQuery: {e}")

    print("All JSON files parsed and appended to BigQuery successfully!")
    return {"message": "All GitHub JSON data parsed and appended to BigQuery!"}, 200
