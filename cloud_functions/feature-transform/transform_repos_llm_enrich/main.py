import functions_framework
from google.cloud import bigquery
import requests
import json
import re
import os

# ========== CONFIG ==========
PROJECT_ID = "ba-882-fall25-team8"
DATASET = "cleaned_github_data"

INPUT_TABLE = f"{PROJECT_ID}.{DATASET}.repo_ml_dataset"
OUTPUT_TABLE = f"{PROJECT_ID}.{DATASET}.repo_llm_enriched"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


# ========== HELPERS ==========

def extract_json(text):
    """Extract JSON {...} from LLM text"""
    match = re.search(r"\{.*?\}", text, re.DOTALL)
    return match.group(0) if match else None


STRUCTURED_PROMPT = """
You are an AI that extracts structured metadata from a GitHub README.

Return ONLY valid JSON with these fields:

{
  "category": one of [
    "AI/ML","DevOps","System Tools","Databases","Web Tools","System Programming",
    "Graphics","Embedded","Networking","CLI Tools","Other"
  ],
  
  "tech_stack": array of programming languages only,

  "complexity_level": one of ["beginner","intermediate","advanced"],
  "complexity_score": number (1=beginner, 2=intermediate, 3=advanced),

  "audience": choose multiple from [
    "sysadmin","backend-dev","frontend-dev","ML-engineer","researcher",
    "IT-ops","security-engineer","data-engineer","general-user"
  ],

  "use_cases": choose multiple from [
    "monitoring","deployment","data-processing","computer-vision","graphics",
    "caching","database","cli-automation","web-server","networking","devops"
  ]
}

Return ONLY JSON. No explanation.
"""


def call_llm(repo_name, readme_text):
    """Call OpenAI structured extraction model"""
    prompt = f"{STRUCTURED_PROMPT}\n\nREADME:\n{readme_text}"

    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPENAI_API_KEY}",
            },
            json={
                "model": "gpt-4.1-mini",
                "input": prompt,
                "temperature": 0,
            }
        )
        response.raise_for_status()

        output = response.json()["output"][0]["content"][0]["text"]

        json_str = extract_json(output)
        if json_str:
            return json.loads(json_str)

        return None

    except Exception as e:
        print(f"❌ LLM error for {repo_name}: {e}")
        return None


def fetch_readme(repo_name):
    """Try multiple possible README file names"""
    paths = ["README.md", "Readme.md", "readme.md", "README.MD"]

    for path in paths:
        url = f"https://raw.githubusercontent.com/{repo_name}/master/{path}"
        r = requests.get(url)
        if r.status_code == 200:
            return r.text
    return None


# ========== MAIN FUNCTION ==========

@functions_framework.http
def task(request):
    print("🚀 Starting LLM enrichment task...")

    # -------- Limit handling (GET or POST) --------
    if request.method == "GET":
        raw_limit = request.args.get("limit", 300)
    else:
        body = request.get_json(silent=True) or {}
        raw_limit = body.get("limit", 300)

    try:
        limit = int(raw_limit)
    except:
        limit = 300

    print(f"📌 Using LIMIT = {limit}")

    client = bigquery.Client()

    # -------- Query (no pagination issue using .result()) --------
    query = f"""
        SELECT repo_name, repo_url
        FROM `{INPUT_TABLE}`
        LIMIT {limit}
    """

    print("📌 SQL Query:")
    print(query)

    repos = client.query(query).result().to_dataframe()
    print(f"📌 Loaded {len(repos)} repos")

    enriched_rows = []

    fallback = {
        "category": None,
        "tech_stack": None,
        "use_cases": None,
        "complexity_level": None,
        "complexity_score": None,
        "audience": None
    }

    # -------- Process each repo --------
    for _, row in repos.iterrows():
        repo_name = row["repo_name"]
        print("\n============================")
        print(f"📌 Processing: {repo_name}")

        readme = fetch_readme(repo_name)

        if not readme:
            print(f"⚠️ No README found for {repo_name}")
            enriched = fallback.copy()
        else:
            enriched = call_llm(repo_name, readme) or fallback.copy()

        enriched["repo_name"] = repo_name
        enriched_rows.append(enriched)

    # -------- Write to BigQuery (APPEND to avoid overwrite) --------
    print(f"\n📥 Writing {len(enriched_rows)} rows to BigQuery...")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",   # NEVER overwrite table
        autodetect=True
    )

    client.load_table_from_json(
        enriched_rows,
        OUTPUT_TABLE,
        job_config=job_config
    ).result()

    print(f"✅ Successfully appended {len(enriched_rows)} rows to {OUTPUT_TABLE}")

    return {
        "message": "LLM enrichment complete!",
        "rows_enriched": len(enriched_rows),
        "output_table": OUTPUT_TABLE
    }, 200
