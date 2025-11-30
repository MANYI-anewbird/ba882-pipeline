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
    "retries": 0,
}

# ===== Cloud Functions URLs =====
FUNCTIONS = {
    # Extract layer
    "schema_setup": "https://raw-schema-setup-bq-qentqpf6ya-uc.a.run.app",
    "extract_repos": "https://raw-extract-github-repos-qentqpf6ya-uc.a.run.app",
    "extract_contributors": "https://raw-extract-github-contributors-qentqpf6ya-uc.a.run.app",
    "extract_commits": "https://raw-extract-github-commits-qentqpf6ya-uc.a.run.app",
    "extract_readme": "https://raw-extract-github-readme-643469687953.us-central1.run.app",
    "extract_languages": "https://raw-extract-github-languages-qentqpf6ya-uc.a.run.app",

    # Parse
    "parse_github": "https://raw-parse-github-qentqpf6ya-uc.a.run.app",

    # Transform (summary)
    "transform_repos": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-repos-summary",
    "transform_contributors": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-contributors-clean",
    "transform_commits": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-commits-clean",
    "transform_languages": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform-language-summary",

    # NEW: LLM enrich & ML ready
    "transform_repos_llm_enrich": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform_repos_llm_enrich",
    "repo_ml_ready": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform_repos_ml_ready",

    # NEW: Final numeric encoding
    "ml_transform_final": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/ml_transform_final",

    # ML model
    "ml_cluster_repos": "https://us-central1-ba-882-fall25-team8.cloudfunctions.net/transform_cluster_repos",
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
    tags=["github", "pipeline", "ml", "llm"],
) as dag:

    # ===== Setup =====
    schema_setup = BashOperator(
        task_id="setup_bq_schema",
        bash_command=f'curl -X POST "{FUNCTIONS["schema_setup"]}"'
    )

    # ===== Extract =====
    extract_repos = BashOperator(
        task_id="extract_github_repos",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_repos"]}?limit=300"'
    )
    extract_contributors = BashOperator(
        task_id="extract_github_contributors",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_contributors"]}?limit=300"'
    )
    extract_commits = BashOperator(
        task_id="extract_github_commits",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_commits"]}?limit=300"'
    )
    extract_readme = BashOperator(
        task_id="extract_github_readme",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_readme"]}?limit=300"'
    )
    extract_languages = BashOperator(
        task_id="extract_github_languages",
        bash_command=f'curl -X POST "{FUNCTIONS["extract_languages"]}?limit=300"'
    )

    # ===== Parse =====
    parse_github = BashOperator(
        task_id="parse_github_data",
        bash_command=f'curl -X POST "{FUNCTIONS["parse_github"]}?limit=300"'
    )

    # ===== Transform summary =====
    transform_repos = BashOperator(
        task_id="transform_repos_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_repos"]}"'
    )
    transform_contributors = BashOperator(
        task_id="transform_contributors_clean",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_contributors"]}"'
    )
    transform_commits = BashOperator(
        task_id="transform_commits_clean",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_commits"]}"'
    )
    transform_languages = BashOperator(
        task_id="transform_languages_summary",
        bash_command=f'curl -X POST "{FUNCTIONS["transform_languages"]}"'
    )

    # ===== NEW: LLM Enrich =====
    transform_repos_llm_enrich = BashOperator(
         task_id="transform_repos_llm_enrich",
         bash_command=f'curl "{FUNCTIONS["transform_repos_llm_enrich"]}?limit=300"'
    )

    # ===== NEW: Repo ML Ready =====
    repo_ml_ready = BashOperator(
        task_id="repo_ml_ready",
        bash_command=f'curl -X POST "{FUNCTIONS["repo_ml_ready"]}"'
    )

    # ===== NEW: Final numeric encoding =====
    ml_transform_final = BashOperator(
        task_id="ml_transform_final",
        bash_command=f'curl -X POST "{FUNCTIONS["ml_transform_final"]}"'
    )

    # ===== ML Clustering =====
    ml_cluster_repos = BashOperator(
        task_id="ml_cluster_repos",
        bash_command=f'curl -X POST "{FUNCTIONS["ml_cluster_repos"]}"'
    )

    # ===== Slack Notifications =====
    notify_success = BashOperator(
        task_id="notify_success_slack",
        bash_command=slack_notify(
            "✅ GitHub Data Pipeline Completed Successfully!",
            "#36a64f",
            "*Pipeline:* github_data_pipeline_v3\n"
            "*Output:* repo_cluster_summary"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = BashOperator(
        task_id="notify_failure_slack",
        bash_command=slack_notify(
            "❌ GitHub Data Pipeline Failed!",
            "#ff0000",
            "*Pipeline:* github_data_pipeline_v3\n"
            "*Failed Task:* {{ ti.task_id }}"
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ===== Dependencies =====

    # Setup → Extract
    schema_setup >> extract_repos
    extract_repos >> [extract_contributors, extract_commits, extract_readme, extract_languages]

    # Extract → Parse
    [extract_contributors, extract_commits, extract_readme, extract_languages] >> parse_github

    # Parse → Summary transforms
    parse_github >> [
        transform_repos,
        transform_contributors,
        transform_commits,
        transform_languages
    ]

    # Four transforms → LLM enrich
    [
        transform_repos,
        transform_contributors,
        transform_commits,
        transform_languages
    ] >> transform_repos_llm_enrich

    # LLM → ML-ready
    transform_repos_llm_enrich >> repo_ml_ready

    # ML-ready → Numeric encoding
    repo_ml_ready >> ml_transform_final

    # Numeric → Clustering
    ml_transform_final >> ml_cluster_repos

    # → Slack notifications
    ml_cluster_repos >> [notify_success, notify_failure]

