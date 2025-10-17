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
    "parse_github": "https://raw-parse-github-qentqpf6ya-uc.a.run.app",
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
        bash_command=f'curl -X POST "{FUNCTIONS["extract_repos"]}" -H "Content-Type: application/json" -d \'{{\"limit\": 200}}\''
    )

    extract_contributors = BashOperator(
        task_id="extract_github_contributors",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_contributors"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    extract_commits = BashOperator(
        task_id="extract_github_commits",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_commits"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    parse_github = BashOperator(
        task_id="parse_github_data",
        bash_command=f'curl -X POST "{FUNCTIONS["parse_github"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # DAG dependencies
    schema_setup >> extract_repos
    extract_repos >> [extract_contributors, extract_commits]
    [extract_contributors, extract_commits] >> parse_github