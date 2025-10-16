# === Function: Extract GitHub Contributors for each Repo ===
import functions_framework
import requests
import json
import datetime
from google.cloud import secretmanager, storage

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SECRET_NAME = "github_token"
BUCKET_NAME = "ba882-t8-github"

# === MAIN FUNCTION ===
@functions_framework.http
def task(request):
    """
    Cloud Function: extract contributors of top 200 GitHub repos
    """
    print("Starting GitHub contributors extraction task...")

    # Step 1️ - Get GitHub Token from Secret Manager
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    print("✅ Retrieved GitHub token")

    # Step 2️ - Read repo list from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # date
    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    repo_file_path = f"raw/github_repos_raw/date={run_date}/repos_200.json"

    print(f"Reading repo list from gs://{BUCKET_NAME}/{repo_file_path}")
    blob = bucket.blob(repo_file_path)
    repo_data = json.loads(blob.download_as_string())
    print(f"✅ Loaded {len(repo_data)} repos")

    # Step 3️ - Extract contributors for each repo
    headers = {"Authorization": f"token {github_token}"}
    base_url = "https://api.github.com/repos"

    all_contributors = []
    for repo in repo_data[:200]:  # limit to 200 repos
        full_name = repo.get("full_name")
        if not full_name:
            continue

        url = f"{base_url}/{full_name}/contributors"
        print(f"Fetching contributors for {full_name}")
        response = requests.get(url, headers=headers, params={"per_page": 100})
        if response.status_code != 200:
            print(f" Failed to fetch {full_name}: {response.status_code}")
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

    print(f"✅ Total contributors collected: {len(all_contributors)}")

    # Step 4️ - Save to GCS
    output_path = f"raw/github_contributors_raw/date={run_date}/contributors.json"
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(json.dumps(all_contributors))
    print(f"Uploaded contributors data to gs://{BUCKET_NAME}/{output_path}")

    # Step 5️ - Return metadata
    return {
        "message": "GitHub contributors extracted successfully!",
        "contributors_count": len(all_contributors),
        "bucket": BUCKET_NAME,
        "path": output_path
    }, 200
