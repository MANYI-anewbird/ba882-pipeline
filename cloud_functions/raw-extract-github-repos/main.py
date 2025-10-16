# === Function: Extract GitHub Repositories (Top 200) ===
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
    Cloud Function:
    most popular top 200 repo，and load to GCS
    """
    print(" Starting GitHub extraction task...")

    # Step 1️ — from Secret Manager get GitHub Token
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    print("✅ Retrieved GitHub token from Secret Manager")

    # Step 2️ — request GitHub API（top 200 repo）
    headers = {"Authorization": f"token {github_token}"}
    url = "https://api.github.com/search/repositories"

    all_items = []
    for page in range(1, 3):  # page=1,2 → top 200 repo
        params = {
            "q": "stars:>1",
            "sort": "stars",
            "order": "desc",
            "per_page": 100,
            "page": page
        }
        print(f"🔗 Requesting page {page} ...")
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"fall GitHub API failed on page {page}: {resp.status_code}")
            continue

        data = resp.json()
        items = data.get("items", [])
        print(f" Page {page}: Retrieved {len(items)} repos")
        all_items.extend(items)

    print(f"✅ Total repos collected: {len(all_items)}")

    if not all_items:
        return {"error": "No data retrieved from GitHub"}, 500

    # Step 3️ — upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    file_name = f"raw/github_repos_raw/date={run_date}/repos_200.json"
    blob = bucket.blob(file_name)

    blob.upload_from_string(json.dumps(all_items))
    print(f"Uploaded JSON to gs://{BUCKET_NAME}/{file_name}")

    # Step 4️
    return {
        "message": "GitHub repos extracted successfully!",
        "repo_count": len(all_items),
        "bucket": BUCKET_NAME,
        "path": file_name
    }, 200
    
