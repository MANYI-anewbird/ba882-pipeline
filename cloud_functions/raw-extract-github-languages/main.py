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
    print("Starting GitHub languages extraction...")
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{PROJECT_ID}/secrets/{SECRET_NAME}/versions/latest"
    token_response = sm_client.access_secret_version(request={"name": secret_path})
    github_token = token_response.payload.data.decode("UTF-8")
    
    limit = int(request.args.get("limit", 200))
    run_date = datetime.datetime.utcnow().strftime("%Y%m%d")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    repos_blob = bucket.blob(f"raw/github_repos_raw/date={run_date}/repos_{limit}.json")
    repos_data = json.loads(repos_blob.download_as_text())
    
    headers = {"Authorization": f"token {github_token}", "Accept": "application/vnd.github.v3+json"}
    all_languages = []
    
    for repo in repos_data:
        full_name = repo.get("full_name")
        if not full_name:
            continue
        try:
            response = requests.get(f"https://api.github.com/repos/{full_name}/languages", headers=headers, timeout=10)
            if response.status_code == 200:
                for language, bytes_count in response.json().items():
                    all_languages.append({
                        "repo_full_name": full_name,
                        "language": language,
                        "bytes": bytes_count,
                        "extracted_at": datetime.datetime.utcnow().isoformat()
                    })
        except:
            pass
    
    output_path = f"raw/github_languages_raw/date={run_date}/languages_{limit}.json"
    bucket.blob(output_path).upload_from_string(json.dumps(all_languages, indent=2))
    
    return {"message": f"Extracted languages!", "path": output_path, "total_entries": len(all_languages)}, 200
