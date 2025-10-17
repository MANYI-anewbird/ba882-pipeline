# === Function: Extract GitHub Contributors for each Repo ===
import functions_framework
import requests
import json
import datetime
import time
from google.cloud import secretmanager, storage

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SECRET_NAME = "github_token"
BUCKET_NAME = "ba882-t8-github"

# === MAIN FUNCTION ===
@functions_framework.http
def task(request):
    """
    Cloud Function: extract contributors for top N GitHub repos
    Example:
        Default: read repos_200.json
        Custom: ?limit=300  → read repos_300.json
    """
    print("🚀 Starting GitHub contributors extraction task...")

    # Step 1 - Get GitHub Token
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    print("success: Retrieved GitHub token")

    # Step 2 - Read repo list from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    limit = int(request.args.get("limit", 200))  # allow ?limit=300
    repo_file_path = f"raw/github_repos_raw/date={run_date}/repos_{limit}.json"

    print(f"Reading repo list from gs://{BUCKET_NAME}/{repo_file_path}")
    blob = bucket.blob(repo_file_path)
    if not blob.exists():
        return {"error": f"Repo file not found: {repo_file_path}"}, 404

    repo_data = json.loads(blob.download_as_string())
    print(f"success: Loaded {len(repo_data)} repos from {repo_file_path}")

    # Step 3 - Extract contributors for each repo
    headers = {"Authorization": f"token {github_token}"}
    base_url = "https://api.github.com/repos"

    all_contributors = []
    for idx, repo in enumerate(repo_data[:limit], start=1):
        full_name = repo.get("full_name")
        if not full_name:
            continue

        url = f"{base_url}/{full_name}/contributors"
        print(f"[{idx}/{limit}] Fetching contributors for {full_name} ...")

        try:
            response = requests.get(url, headers=headers, params={"per_page": 100})
            
            # Handle rate limiting
            if response.status_code == 403:
                print("warning: Rate limit hit, waiting 60s...")
                time.sleep(60)
                continue
            elif response.status_code != 200:
                print(f"warning: Failed for {full_name}: {response.status_code}")
                continue

            contributors = response.json()
            for c in contributors:
                all_contributors.append({
                    "repo_full_name": full_name,
                    "contributor_login": c.get("login"),
                    "contributor_id": c.get("id"),
                    "contributions": c.get("contributions"),
                    "html_url": c.get("html_url"),
                    "followers_url": c.get("followers_url"),
                    "snapshot_date": run_date
                })
            
            print(f"success: {len(contributors)} contributors from {full_name}")
        
        except Exception as e:
            print(f"Error fetching {full_name}: {str(e)}")
        
        # Add delay to avoid hitting rate limits
        time.sleep(1)

    print(f"success: Total contributors collected: {len(all_contributors)}")

    # Step 4 - Upload to GCS
    output_path = f"raw/github_contributors_raw/date={run_date}/contributors_{limit}.json"
    blob_out = bucket.blob(output_path)
    blob_out.upload_from_string(json.dumps(all_contributors))
    print(f"Uploaded to gs://{BUCKET_NAME}/{output_path}")

    # Step 5 - Return metadata
    return {
        "message": f"success: GitHub contributors extracted successfully for top {limit} repos!",
        "contributors_count": len(all_contributors),
        "bucket": BUCKET_NAME,
        "path": output_path
    }, 200
