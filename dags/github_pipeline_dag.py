from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# === Default settings ===
default_args = {
    'owner': 'Manyi Hong',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# === DAG definition ===
with DAG(
    'github_pipeline_dag',
    default_args=default_args,
    description='Run GitHub extract and parse Cloud Functions in sequence',
    schedule_interval='@weekly',  # 每周自动运行
    start_date=days_ago(1),
    catchup=False,
    tags=['github', 'cloud-function'],
) as dag:

    start = DummyOperator(task_id='start')

    extract_repos = SimpleHttpOperator(
        task_id='extract_github_repos',
        http_conn_id='gcp_cloud_function',
        endpoint='raw-extract-github-repos?limit=300',
        method='GET',
    )

    extract_contributors = SimpleHttpOperator(
        task_id='extract_github_contributors',
        http_conn_id='gcp_cloud_function',
        endpoint='raw-extract-github-contributors?limit=300',
        method='GET',
    )

    extract_commits = SimpleHttpOperator(
        task_id='extract_github_commits',
        http_conn_id='gcp_cloud_function',
        endpoint='raw-extract-github-commits?limit=300',
        method='GET',
    )

    parse_to_bigquery = SimpleHttpOperator(
        task_id='parse_github_data',
        http_conn_id='gcp_cloud_function',
        endpoint='raw-parse-github?date={{ ds_nodash }}&limit=300',
        method='GET',
    )

    end = DummyOperator(task_id='end')

    start >> extract_repos >> extract_contributors >> extract_commits >> parse_to_bigquery >> end
