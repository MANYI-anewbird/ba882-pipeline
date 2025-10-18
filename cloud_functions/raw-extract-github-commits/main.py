# === Function: Extract GitHub Commits for each Repo ===
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
    Cloud Function: extract latest commits for top N GitHub repos
    Example:
        Default: read repos_200.json
        Custom: ?limit=300 → read repos_300.json
    """
    print("Starting GitHub commits extraction task...")

    # Step 1️ — Retrieve GitHub Token
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    headers = {"Authorization": f"token {github_token}"}
    print("success: Retrieved GitHub token")

    # Step 2️ — Load repo list from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")

    # flexible limit
    limit = int(request.args.get("limit", 200))
    repo_file_path = f"raw/github_repos_raw/date={run_date}/repos_{limit}.json"

    print(f"Reading repo list from gs://{BUCKET_NAME}/{repo_file_path}")
    repo_blob = bucket.blob(repo_file_path)
    if not repo_blob.exists():
        return {"error": f"Repo file not found: {repo_file_path}"}, 404

    repo_data = json.loads(repo_blob.download_as_string())
    print(f"success: Loaded {len(repo_data)} repos from {repo_file_path}")

    # Step 3️ — Extract commits
    base_url = "https://api.github.com/repos"
    all_commits = []

    for idx, repo in enumerate(repo_data[:limit], start=1):
        full_name = repo.get("full_name")
        if not full_name:
            continue

        print(f"[{idx}/{limit}] Fetching commits for {full_name}")
        url = f"{base_url}/{full_name}/commits"

        try:
            response = requests.get(url, headers=headers, params={"per_page": 100})
           
            if response.status_code == 403:
                print("warning: Rate limit hit, waiting 60s...")
                time.sleep(60)
                # Retry the same repo
                response = requests.get(url, headers=headers, params={"per_page": 100})
                if response.status_code != 200:
                    print(f"warning: Retry failed for {full_name}")
                    continue
            elif response.status_code != 200:
                print(f"warning: Failed {response.status_code} for {full_name}")
                continue

            commits = response.json()
            for c in commits:
                commit_info = c.get("commit", {})
                author_info = commit_info.get("author", {})
                all_commits.append({
                    "repo_full_name": full_name,
                    "sha": c.get("sha"),
                    "commit_message": commit_info.get("message"),
                    "author_name": author_info.get("name"),
                    "author_email": author_info.get("email"),
                    "commit_date": author_info.get("date"),
                    "html_url": c.get("html_url"),
                    "snapshot_date": run_date
                })

            print(f"success: {len(commits)} commits from {full_name}")

        except Exception as e:
            print(f"Error fetching {full_name}: {str(e)}")

        time.sleep(1)  #  GitHub API limitation avoid

    print(f"success: Total commits collected: {len(all_commits)}")

    # Step 4️ — Upload to GCS
    output_path = f"raw/github_commits_raw/date={run_date}/commits_{limit}.json"
    blob = bucket.blob(output_path)
    blob.upload_from_string(json.dumps(all_commits))
    print(f"Uploaded commits data to gs://{BUCKET_NAME}/{output_path}")

    # Step 5️ — Return metadata
    return {
        "message": f"success: GitHub commits extracted successfully for top {limit} repos!",
        "commits_count": len(all_commits),
        "bucket": BUCKET_NAME,
        "path": output_path
    }, 200
