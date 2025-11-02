# === Function: Extract GitHub Repositories (Flexible Version) ===
import functions_framework
import requests
import json
import datetime
from google.cloud import secretmanager, storage

# === CONFIGURATION ===
PROJECT_ID = "ba-882-fall25-team8"
SECRET_NAME = "github_token"
BUCKET_NAME = "ba882-t8-github"

@functions_framework.http
def task(request):
    """
    Cloud Function:
    Extract top N most popular GitHub repositories and upload to GCS.
    Example usage:
        - Default: top 200 repos
        - Custom: ?limit=300
    """
    print("Starting GitHub extraction task...")

    # Step 1️ — Get GitHub Token securely
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    print("success: Retrieved GitHub token from Secret Manager")

    # Step 2️ — Determine how many repos to fetch
    limit = int(request.args.get("limit", 300))  # default = 200
    per_page = 100
    total_pages = (limit // per_page) + (1 if limit % per_page else 0)
    print(f"success: Will fetch top {limit} repositories ({total_pages} pages)")

    # Step 3️ — Call GitHub API
    headers = {"Authorization": f"token {github_token}"}
    base_url = "https://api.github.com/search/repositories"
    all_repos = []

    for page in range(1, total_pages + 1):
        params = {
            "q": "stars:>5000",
            "sort": "stars",
            "order": "desc",
            "per_page": per_page,
            "page": page
        }
        print(f"Requesting page {page}/{total_pages} ...")
        resp = requests.get(base_url, headers=headers, params=params)

        if resp.status_code != 200:
            print(f"GitHub API failed on page {page}: {resp.status_code}")
            break

        items = resp.json().get("items", [])
        print(f" success: Page {page}: Retrieved {len(items)} repos")
        all_repos.extend(items)

        if len(all_repos) >= limit:
            all_repos = all_repos[:limit]
            break

    print(f"Total repos collected: {len(all_repos)}")

    if not all_repos:
        return {"error": "No data retrieved from GitHub"}, 500

    # Step 4️ — Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    output_path = f"raw/github_repos_raw/date={run_date}/repos_{limit}.json"
    blob = bucket.blob(output_path)
    blob.upload_from_string(json.dumps(all_repos))
    print(f"Uploaded to gs://{BUCKET_NAME}/{output_path}")

    # Step 5️ — Return response
    return {
        "message": f"success:Extracted top {len(all_repos)} GitHub repos successfully!",
        "repos_count": len(all_repos),
        "bucket": BUCKET_NAME,
        "path": output_path
    }, 200
