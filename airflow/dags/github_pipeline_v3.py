from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ===== Project Config =====
PROJECT_ID = "ba-882-fall25-team8"
LOCATION = "us-central1"

# ===== Slack Webhook =====
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["hmy@bu.edu", "ebaykurt@bu.edu", "zehui@bu.edu", "sanjal@bu.edu"], 
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

# ===== Cloud Functions URLs =====
FUNCTIONS = {
    "schema_setup": "https://raw-schema-setup-bq-qentqpf6ya-uc.a.run.app",
    "extract_repos": "https://raw-extract-github-repos-qentqpf6ya-uc.a.run.app",
    "extract_contributors": "https://raw-extract-github-contributors-qentqpf6ya-uc.a.run.app",
    "extract_commits": "https://raw-extract-github-commits-qentqpf6ya-uc.a.run.app",
    "extract_readme": "https://raw-extract-github-readme-643469687953.us-central1.run.app",
    "extract_languages": "https://raw-extract-github-languages-qentqpf6ya-uc.a.run.app",
    "parse_github": "https://raw-parse-github-qentqpf6ya-uc.a.run.app",
    "transform_repos": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-repos-summary",
    "transform_contributors": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-contributors-clean",
    "transform_commits": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-commits-clean",
    "transform_languages": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-language-summary",
    "transform_repos_ml": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-repo-ml",
    "ml_cluster_repos": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/ml-cluster-repos",
}

# ===== Slack notification template =====
def slack_notify(status: str, color: str, message: str):
    return f"""
    curl -X POST -H 'Content-type: application/json' \
    --data '{{
        "attachments": [{{
            "color": "{color}",
            "blocks": [
                {{
                    "type": "header",
                    "text": {{
                        "type": "plain_text",
                        "text": "{status}"
                    }}
                }},
                {{
                    "type": "section",
                    "text": {{
                        "type": "mrkdwn",
                        "text": "{message}"
                    }}
                }},
                {{
                    "type": "context",
                    "elements": [
                        {{
                            "type": "mrkdwn",
                            "text": "🔗 <https://astro.astronomer.io | Open Airflow UI>"
                        }}
                    ]
                }}
            ]
        }}]
    }}' {SLACK_WEBHOOK_URL}
    """

# ===== Define DAG =====
with DAG(
    dag_id="github_data_pipeline_v3",
    default_args=default_args,
    start_date=datetime(2024, 10, 16),
    schedule="@weekly",
    catchup=False,
    tags=["github", "pipeline", "ml", "languages"],
) as dag:

    # ===== Setup =====
    schema_setup = BashOperator(
        task_id="setup_bq_schema",
        bash_command=f'curl -X POST "{FUNCTIONS["schema_setup"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # ===== Extract =====
    extract_repos = BashOperator(
        task_id="extract_github_repos",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_repos"]}?limit=300" -H "Content-Type: application/json" -d \'{{}}\''
    )
    extract_contributors = BashOperator(
        task_id="extract_github_contributors",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_contributors"]}?limit=300" -H "Content-Type: application/json" -d \'{{}}\''
    )
    extract_commits = BashOperator(
        task_id="extract_github_commits",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_commits"]}?limit=300" -H "Content-Type: application/json" -d \'{{}}\''
    )
    extract_readme = BashOperator(
        task_id="extract_github_readme",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_readme"]}?limit=300" -H "Content-Type: application/json" -d \'{{}}\''
    )
    extract_languages = BashOperator(
        task_id="extract_github_languages",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_languages"]}?limit=300" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # ===== Parse =====
    parse_github = BashOperator(
        task_id="parse_github_data",
        bash_command=f'curl -X POST "{FUNCTIONS["parse_github"]}?limit=300" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # ===== Transform =====
    transform_repos = BashOperator(
        task_id="transform_repos_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_repos"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )
    transform_contributors = BashOperator(
        task_id="transform_contributors_clean",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_contributors"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )
    transform_commits = BashOperator(
        task_id="transform_commits_clean",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_commits"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )
    transform_languages = BashOperator(
        task_id="transform_language_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_languages"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )
    transform_repos_ml = BashOperator(
        task_id="transform_repos_ml",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_repos_ml"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # ===== ML =====
    ml_cluster_repos = BashOperator(
        task_id="ml_cluster_repos",
        bash_command=f'curl -X POST "{FUNCTIONS["ml_cluster_repos"]}" -H "Content-Type: application/json" -d \'{{}}\''
    )

    # ===== Slack Notifications =====
    notify_success = BashOperator(
        task_id="notify_success_slack",
        bash_command=slack_notify(
            "✅ GitHub Data Pipeline Completed Successfully!",
            "#36a64f",
            "*DAG:* {{ dag.dag_id }}\\n"
            "*Run ID:* {{ run_id }}\\n"
            "*Triggered By:* {{ dag_run.conf.get('triggered_by', 'manual or schedule') }}\\n"
            "*Output:* `ba-882-fall25-team8.ml_results.repo_cluster_summary`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = BashOperator(
        task_id="notify_failure_slack",
        bash_command=slack_notify(
            "❌ GitHub Data Pipeline Failed!",
            "#ff0000",
            "*DAG:* {{ dag.dag_id }}\\n"
            "*Run ID:* {{ run_id }}\\n"
            "*Failed Task:* {{ ti.task_id if ti else 'Unknown' }}"
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ===== Dependencies =====
    schema_setup >> extract_repos
    extract_repos >> [extract_contributors, extract_commits, extract_readme, extract_languages]
    [extract_contributors, extract_commits, extract_readme, extract_languages] >> parse_github
    parse_github >> [transform_repos, transform_contributors, transform_commits, transform_languages]
    [transform_repos, transform_contributors, transform_commits, transform_languages] >> transform_repos_ml
    transform_repos_ml >> ml_cluster_repos
    ml_cluster_repos >> [notify_success, notify_failure]
