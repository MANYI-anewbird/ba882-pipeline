# BA882 Team 8 - Technology Adoption Trends via Open Source Repositories

## Overview
This project explores technology adoption trends using open-source developer data from GitHub, Stack Overflow, and Reddit.

## Team Members
Emre Can Baykurt, Manyi Hong, Sanjal Atul Desai, Zehui Wang

## Phase 1 Objective
Build a cloud-based data pipeline that:
1. Fetches GitHub repository metadata weekly using the GitHub API.
2. Stores raw JSON files in Google Cloud Storage.
3. Parses and loads structured data into BigQuery tables.

## Architecture
ba882-team8-github-pipeline/
│
├── README.md  
│  
├── requirements.txt                             
│
├── data/                             
│   ├── raw/                         
│   ├── processed/                   
│   └── schema/                       
│
├── notebooks/                        
│   ├── 01-github-api-test.ipynb
│   ├── 02-data-cleaning.ipynb
│   ├── 03-visualization.ipynb
│   └── archive/                      
│
├── scripts/                          
│   ├── extract_github.py             
│   ├── parse_to_bigquery.py          
│   ├── create_schema_bq.py           
│
├── cloud_functions/                  ）
│   ├── raw-schema-setup-bq/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── raw-extract-github/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── raw-parse-github/
│   │   ├── main.py
│   │   └── requirements.txt
│   └── README.md
│
├── streamlit_app/                    
│   ├── app.py
│   └── requirements.txt
│
└── docs/                             
    ├── architecture_diagram.png      
    ├── team_notes.md
    └── project_report_draft.md

## Deployment Steps
1. Create GCS bucket: `ba882-team8-github`
2. Create BigQuery dataset: `github_raw`
3. Deploy Cloud Functions:
   - raw-schema-setup-bq
   - raw-extract-github
   - raw-parse-github

## Tech Stack
- Google Cloud Platform (GCS, BigQuery, Cloud Functions)
- Python (requests, pandas, google-cloud-bigquery)
- Streamlit (for visualization)
