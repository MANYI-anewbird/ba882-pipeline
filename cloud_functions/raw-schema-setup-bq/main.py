import functions_framework
from google.cloud import bigquery

PROJECT_ID = "ba-882-fall25-team8"
DATASET_ID = "raw_github_data"


@functions_framework.http
def task(request):
    print("Creating or ensuring BigQuery tables exist...")

    bq = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = "US"

    try:
        bq.get_dataset(dataset_ref)
        print(f"✅ Dataset {DATASET_ID} already exists.")
    except:
        bq.create_dataset(dataset_ref)
        print(f"✅ Created dataset: {DATASET_ID}")

    tables = {
        "github_repos_raw": [
            bigquery.SchemaField("snapshot_date", "INTEGER"),
            bigquery.SchemaField("permissions", "RECORD"),
            bigquery.SchemaField("topics", "STRING", mode="REPEATED"),
            bigquery.SchemaField("web_commit_signoff_required", "BOOLEAN"),
            bigquery.SchemaField("allow_forking", "BOOLEAN"),
            bigquery.SchemaField("disabled", "BOOLEAN"),
            bigquery.SchemaField("mirror_url", "STRING"),
            bigquery.SchemaField("forks_count", "INTEGER"),
            bigquery.SchemaField("has_discussions", "BOOLEAN"),
            bigquery.SchemaField("license", "RECORD"),
            bigquery.SchemaField("has_downloads", "BOOLEAN"),
            bigquery.SchemaField("archived", "BOOLEAN"),
            bigquery.SchemaField("has_projects", "BOOLEAN"),
            bigquery.SchemaField("has_issues", "BOOLEAN"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("watchers_count", "INTEGER"),
            bigquery.SchemaField("stargazers_count", "INTEGER"),
            bigquery.SchemaField("homepage", "STRING"),
            bigquery.SchemaField("svn_url", "STRING"),
            bigquery.SchemaField("open_issues_count", "INTEGER"),
            bigquery.SchemaField("ssh_url", "STRING"),
            bigquery.SchemaField("git_url", "STRING"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
            bigquery.SchemaField("assignees_url", "STRING"),
            bigquery.SchemaField("milestones_url", "STRING"),
            bigquery.SchemaField("downloads_url", "STRING"),
            bigquery.SchemaField("issue_comment_url", "STRING"),
            bigquery.SchemaField("has_pages", "BOOLEAN"),
            bigquery.SchemaField("issues_url", "STRING"),
            bigquery.SchemaField("comments_url", "STRING"),
            bigquery.SchemaField("git_commits_url", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("deployments_url", "STRING"),
            bigquery.SchemaField("visibility", "STRING"),
            bigquery.SchemaField("blobs_url", "STRING"),
            bigquery.SchemaField("commits_url", "STRING"),
            bigquery.SchemaField("releases_url", "STRING"),
            bigquery.SchemaField("subscribers_url", "STRING"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("notifications_url", "STRING"),
            bigquery.SchemaField("stargazers_url", "STRING"),
            bigquery.SchemaField("pulls_url", "STRING"),
            bigquery.SchemaField("open_issues", "INTEGER"),
            bigquery.SchemaField("compare_url", "STRING"),
            bigquery.SchemaField("archive_url", "STRING"),
            bigquery.SchemaField("tags_url", "STRING"),
            bigquery.SchemaField("languages_url", "STRING"),
            bigquery.SchemaField("has_wiki", "BOOLEAN"),
            bigquery.SchemaField("size", "INTEGER"),
            bigquery.SchemaField("statuses_url", "STRING"),
            bigquery.SchemaField("trees_url", "STRING"),
            bigquery.SchemaField("git_refs_url", "STRING"),
            bigquery.SchemaField("clone_url", "STRING"),
            bigquery.SchemaField("keys_url", "STRING"),
            bigquery.SchemaField("issue_events_url", "STRING"),
            bigquery.SchemaField("git_tags_url", "STRING"),
            bigquery.SchemaField("forks_url", "STRING"),
            bigquery.SchemaField("branches_url", "STRING"),
            bigquery.SchemaField("labels_url", "STRING"),
            bigquery.SchemaField("subscription_url", "STRING"),
            bigquery.SchemaField("private", "BOOLEAN"),
            bigquery.SchemaField("events_url", "STRING"),
            bigquery.SchemaField("score", "FLOAT"),
            bigquery.SchemaField("default_branch", "STRING"),
            bigquery.SchemaField("watchers", "INTEGER"),
            bigquery.SchemaField("hooks_url", "STRING"),
            bigquery.SchemaField("teams_url", "STRING"),
            bigquery.SchemaField("pushed_at", "TIMESTAMP"),
            bigquery.SchemaField("collaborators_url", "STRING"),
            bigquery.SchemaField("contents_url", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("html_url", "STRING"),
            bigquery.SchemaField("merges_url", "STRING"),
            bigquery.SchemaField("is_template", "BOOLEAN"),
            bigquery.SchemaField("contributors_url", "STRING"),
            bigquery.SchemaField("owner", "RECORD"),
            bigquery.SchemaField("forks", "INTEGER"),
            bigquery.SchemaField("fork", "BOOLEAN"),
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("node_id", "STRING"),
            bigquery.SchemaField("description", "STRING")
        ],

        "github_contributors_raw": [
            bigquery.SchemaField("repo_full_name", "STRING"),
            bigquery.SchemaField("contributor_login", "STRING"),
            bigquery.SchemaField("contributor_id", "INTEGER"),
            bigquery.SchemaField("contributions", "INTEGER"),
            bigquery.SchemaField("html_url", "STRING"),
            bigquery.SchemaField("followers_url", "STRING"),
            bigquery.SchemaField("snapshot_date", "INTEGER"),
        ],

        "github_commits_raw": [
            bigquery.SchemaField("repo_full_name", "STRING"),
            bigquery.SchemaField("sha", "STRING"),
            bigquery.SchemaField("commit_message", "STRING"),
            bigquery.SchemaField("author_name", "STRING"),
            bigquery.SchemaField("author_email", "STRING"),
            bigquery.SchemaField("commit_date", "TIMESTAMP"),
            bigquery.SchemaField("html_url", "STRING"),
            bigquery.SchemaField("snapshot_date", "INTEGER")
        ]
    }

    for table_name, schema in tables.items():
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        try:
            bq.get_table(table_id)
            print(f"✅ Table {table_name} already exists.")
        except:
            table = bigquery.Table(table_id, schema=schema)
            bq.create_table(table)
            print(f"✅ Created table: {table_name}")

    return {"message": "All BigQuery tables are ready!"}, 200
