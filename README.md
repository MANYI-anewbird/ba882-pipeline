# BA882 Team 8 - Technology Adoption Trends via Open Source Repositories

## Overview
This project explores technology adoption trends using open-source developer data from GitHub, Stack Overflow, and Reddit.

## Team Members
Emre Can Baykurt, Manyi Hong, Sanjal Atul Desai, Zehui Wang

## Phase 1 Objective
Build an automated, cloud-native data pipeline that:
01 Fetches GitHub repository metadata weekly using the GitHub API.
02 Stores raw JSON responses in Google Cloud Storage (GCS).
03 Parses and loads cleaned structured data into BigQuery tables.
04 Prepares for later phases of analysis and dashboard visualization.

## Architecture

```bash
ba882-pipeline/
│
├── README.md
│
├── cloud_functions/
│   ├── raw-extract-github-commits/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── raw-extract-github-contributors/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── raw-extract-github-repos/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── raw-parse-github/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── raw-schema-setup-bq/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── README.md
│
├── notebooks/
│   ├── 01_github_api_test.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_visualization.ipynb
│   └── archive/
│
├── scripts/
│   ├── extract_github.py
│   ├── parse_to_bigquery.py
│   ├── create_schema_bq.py
│   └── utils.py
│
├── streamlit_app/
│   ├── app.py
│   └── requirements.txt
│
└── docs/
    ├── architecture_diagram.png
    ├── team_notes.md
    └── project_report_draft.md
```   

## Deployment Steps
1. Create GCS bucket: `ba882-t8-github`
2. Create BigQuery dataset: `ba-882-fall25-team8`
3. Deploy Cloud Functions:
   - raw-schema-setup-bq
   - raw-extract-github-repos
   - raw-extract-github-contributors
   - raw-extract-github-commits
   - raw-parse-github

## Tech Stack
- Google Cloud Platform (GCS, BigQuery, Cloud Functions)
- Python (requests, pandas, google-cloud-bigquery)
- Streamlit (for visualization)

