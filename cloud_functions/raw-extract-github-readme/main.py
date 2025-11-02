# === Function: Extract GitHub READMEs ===
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

@functions_framework.http
def task(request):
    """
    Cloud Function:
    Extract README files from GitHub repositories and upload to GCS.
    Handles rate limits with exponential backoff.
    """
    print("Starting GitHub README extraction...")

    # Step 1: Get GitHub Token
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    print("✓ Retrieved GitHub token")

    # Step 2: Get parameters
    limit = int(request.args.get("limit", 300))
    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    print(f"✓ Will process {limit} repositories for date={run_date}")

    # Step 3: Get list of repositories from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Read the repos file to get the list
    repos_blob_path = f"raw/github_repos_raw/date={run_date}/repos_{limit}.json"
    repos_blob = bucket.blob(repos_blob_path)
    
    try:
        repos_data = json.loads(repos_blob.download_as_text())
        print(f"✓ Loaded {len(repos_data)} repositories from GCS")
    except Exception as e:
        error_msg = f"Failed to load repos file: {e}"
        print(f"✗ {error_msg}")
        return {"error": error_msg}, 500

    # Step 4: Extract README for each repository
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    all_readmes = []
    success_count = 0
    error_count = 0
    
    for i, repo in enumerate(repos_data):
        full_name = repo.get("full_name")
        if not full_name:
            continue
            
        # Progress indicator
        if (i + 1) % 20 == 0:
            print(f"Progress: {i + 1}/{len(repos_data)} repos processed")
        
        # GitHub API endpoint for README
        readme_url = f"https://api.github.com/repos/{full_name}/readme"
        
        # Retry logic with exponential backoff
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(readme_url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    readme_data = response.json()
                    readme_data["repo_full_name"] = full_name
                    readme_data["extracted_at"] = datetime.datetime.utcnow().isoformat()
                    all_readmes.append(readme_data)
                    success_count += 1
                    break
                    
                elif response.status_code == 404:
                    # No README for this repo
                    print(f"  No README: {full_name}")
                    break
                    
                elif response.status_code == 403:
                    # Rate limit hit
                    rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
                    wait_time = max(rate_limit_reset - time.time(), 60)
                    print(f"⚠ Rate limit hit. Waiting {wait_time:.0f} seconds...")
                    time.sleep(wait_time)
                    
                else:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (2 ** attempt))
                    else:
                        error_count += 1
                        print(f"✗ Failed after {max_retries} attempts: {full_name}")
                        break
                        
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
                else:
                    error_count += 1
                    print(f"✗ Connection error for {full_name}: {e}")
                    break
        
        # Small delay between requests
        time.sleep(0.5)

    print(f"\n=== Extraction Complete ===")
    print(f"✓ Success: {success_count} READMEs")
    print(f"✗ Errors: {error_count}")
    
    if not all_readmes:
        return {"message": "No READMEs extracted", "success": 0, "errors": error_count}, 200

    # Step 5: Upload to GCS
    output_path = f"raw/github_readme_raw/date={run_date}/readmes_{limit}.json"
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(json.dumps(all_readmes, indent=2))
    
    print(f"✓ Uploaded to gs://{BUCKET_NAME}/{output_path}")

    return {
        "message": f"Extracted {success_count} READMEs successfully!",
        "bucket": BUCKET_NAME,
        "path": output_path,
        "success_count": success_count,
        "error_count": error_count,
        "date": run_date
    }, 200