from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ID = "ba-882-fall25-team8"
LOCATION = "us-central1"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Function URLs
FUNCTIONS = {
    "schema_setup": "https://raw-schema-setup-bq-qentqpf6ya-uc.a.run.app",
    "extract_repos": "https://raw-extract-github-repos-qentqpf6ya-uc.a.run.app",
    "extract_contributors": "https://raw-extract-github-contributors-qentqpf6ya-uc.a.run.app",
    "extract_commits": "https://raw-extract-github-commits-qentqpf6ya-uc.a.run.app",
    "extract_readme": "https://raw-extract-github-readme-643469687953.us-central1.run.app",
    "extract_languages": "https://raw-extract-github-languages-qentqpf6ya-uc.a.run.app",  # ADD THIS
    "parse_github": "https://raw-parse-github-qentqpf6ya-uc.a.run.app",
    
    # Transform layer functions
    "transform_repos": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-repos-summary",
    "transform_contributors": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-contributors-clean",
    "transform_commits": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-commits-clean",
    "transform_readme": "https://transform-readme-summary-qentqpf6ya-uc.a.run.app",  # ADD THIS
    "transform_languages": "https://transform-language-summary-qentqpf6ya-uc.a.run.app",  # ADD THIS
}

with DAG(
    dag_id="github_data_pipeline_v3",
    default_args=default_args,
    start_date=datetime(2024, 10, 16),
    schedule="@weekly",
    catchup=False,
    tags=["github", "pipeline"],
) as dag:

    schema_setup = BashOperator(
        task_id="setup_bq_schema",
        bash_command=f'curl -X POST "{FUNCTIONS["schema_setup"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    extract_repos = BashOperator(
        task_id="extract_github_repos",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_repos"]}?limit=200" -H "Content-Type: application/json" -d \'{{}}\''
    )

    extract_contributors = BashOperator(
        task_id="extract_github_contributors",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_contributors"]}?limit=200" -H "Content-Type: application/json" -d \'{{}}\''
    )

    extract_commits = BashOperator(
        task_id="extract_github_commits",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_commits"]}?limit=200" -H "Content-Type: application/json" -d \'{{}}\''
    )

    extract_readme = BashOperator(
        task_id="extract_github_readme",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_readme"]}?limit=200" -H "Content-Type: application/json" -d \'{{}}\''
    )

    extract_languages = BashOperator(
        task_id="extract_github_languages",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_languages"]}?limit=200" -H "Content-Type: application/json" -d \'{{}}\''
    )

    parse_github = BashOperator(
        task_id="parse_github_data",
        bash_command=f'curl -X POST "{FUNCTIONS["parse_github"]}?limit=200" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # Transform layer tasks
    transform_repos = BashOperator(
        task_id="transform_repos_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_repos"]}?limit=1000" -H "Content-Type: application/json" -d \'{{}}\''
    )

    transform_contributors = BashOperator(
        task_id="transform_contributors_clean",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_contributors"]}?limit=10000" -H "Content-Type: application/json" -d \'{{}}\''
    )

    transform_commits = BashOperator(
        task_id="transform_commits_clean",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_commits"]}?limit=20000" -H "Content-Type: application/json" -d \'{{}}\''
    )

    transform_readme = BashOperator(
        task_id="transform_readme_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_readme"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    transform_languages = BashOperator(
        task_id="transform_language_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_languages"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # DAG dependencies
    # DAG dependencies
    schema_setup >> extract_repos
    extract_repos >> [extract_contributors, extract_commits, extract_readme, extract_languages]
    [extract_contributors, extract_commits, extract_readme, extract_languages] >> parse_github
    parse_github >> [transform_repos, transform_contributors, transform_commits, transform_readme, transform_languages]