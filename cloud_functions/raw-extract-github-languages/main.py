import functions_framework
import requests
import json
import datetime
from google.cloud import secretmanager, storage

PROJECT_ID = "ba-882-fall25-team8"
SECRET_NAME = "github_token"
BUCKET_NAME = "ba882-t8-github"

@functions_framework.http
def task(request):
    """Extract language breakdown for GitHub repositories"""
    print("Starting GitHub languages extraction...")

    # Get GitHub token
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    print("✓ Retrieved GitHub token")

    # Get parameters
    limit = int(request.args.get("limit", 300))
    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    print(f"✓ Will process {limit} repositories for date={run_date}")

    # Get repos list from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    repos_blob_path = f"raw/github_repos_raw/date={run_date}/repos_{limit}.json"
    repos_blob = bucket.blob(repos_blob_path)
    
    try:
        repos_data = json.loads(repos_blob.download_as_text())
        print(f"✓ Loaded {len(repos_data)} repositories from GCS")
    except Exception as e:
        return {"error": f"Failed to load repos: {e}"}, 500

    # Extract languages for each repo
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    all_languages = []
    success_count = 0
    error_count = 0
    
    for i, repo in enumerate(repos_data):
        full_name = repo.get("full_name")
        if not full_name:
            continue
        
        if (i + 1) % 20 == 0:
            print(f"Progress: {i + 1}/{len(repos_data)} repos processed")
        
        # GitHub API endpoint for languages
        languages_url = f"https://api.github.com/repos/{full_name}/languages"
        
        try:
            response = requests.get(languages_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                languages_data = response.json()
                
                # Convert to list format with repo info
                for language, bytes_count in languages_data.items():
                    all_languages.append({
                        "repo_full_name": full_name,
                        "language": language,
                        "bytes": bytes_count,
                        "extracted_at": datetime.datetime.utcnow().isoformat()
                    })
                
                success_count += 1
                
            elif response.status_code == 404:
                print(f"  No languages data: {full_name}")
            else:
                error_count += 1
                print(f"✗ Failed: {full_name} (status {response.status_code})")
                
        except Exception as e:
            error_count += 1
            print(f"✗ Error for {full_name}: {e}")

    print(f"\n=== Extraction Complete ===")
    print(f"✓ Success: {success_count} repos")
    print(f"✗ Errors: {error_count}")
    print(f"Total language entries: {len(all_languages)}")

    if not all_languages:
        return {"message": "No languages extracted", "success": 0}, 200

    # Upload to GCS
    output_path = f"raw/github_languages_raw/date={run_date}/languages_{limit}.json"
    output_blob = bucket.blob(output_path)
    output_blob.upload_from_string(json.dumps(all_languages, indent=2))
    
    print(f"✓ Uploaded to gs://{BUCKET_NAME}/{output_path}")

    return {
        "message": f"Extracted languages for {success_count} repos!",
        "bucket": BUCKET_NAME,
        "path": output_path,
        "success_count": success_count,
        "total_entries": len(all_languages),
        "date": run_date
    }, 200