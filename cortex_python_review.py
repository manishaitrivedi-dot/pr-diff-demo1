import os, json, re, subprocess
from pathlib import Path
from textwrap import dedent
from snowflake.snowpark import Session

# ---------------------
# Config
# ---------------------
MODEL = "meta/llama-3.1-8b"   # ✅ working base model
MAX_CODE_CHARS = 40_000
FILE_TO_REVIEW = "simple_test.py"

# ---------------------
# Snowflake session
# ---------------------
cfg = {
    "account": "XKB93357.us-west-2",
    "user": "MANISHAT007",
    "password": "Welcome@987654321",
    "role": "ORGADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "MY_DB",
    "schema": "PUBLIC",
}
session = Session.builder.configs(cfg).create()

# ---------------------
# Prompt template (strict structure)
# ---------------------
PROMPT_TEMPLATE = """Please act as a principal-level Python code reviewer.
DO NOT include any of the instructions below in your output.
Start your output exactly with:

Code Review: <function_names_or_filename>

— Instructions (apply silently; do not echo):

Environment: Python 3.9+. Standard library first; common frameworks may appear (FastAPI, pandas, Airflow, Requests, AsyncIO).

Review Priorities (strict order):  
1) Security & Correctness  
2) Reliability & Error-handling  
3) Performance & Complexity  
4) Readability & Maintainability  
5) Testability  

Eligibility Criteria for Findings (ALL must be met):  
- Evidence: Quote the exact snippet and cite line number(s).  
- Severity: Assign {Low | Medium | High | Critical}.  
- Impact: Explain why this materially matters (security, correctness, reliability, performance, maintainability, or testability).  
- Actionability: Provide a safe, minimal correction (tiny corrected snippet if relevant).  
- Non-trivial: Skip stylistic nits or subjective preferences.  

Hard Constraints (accuracy & anti-hallucination):  
- Do NOT propose APIs that don’t exist in Python’s stdlib for the imported modules.  
- sqlite3 specifics:  
  * Do NOT suggest `with conn.cursor() as cursor:` (unsupported in stdlib).  
  * `with sqlite3.connect(...) as conn:` is correct. Use `conn.execute(...)` or `cursor = conn.cursor()`.  
  * Do NOT claim a cursor leak when inside a `with sqlite3.connect(...)` block.  
- Complexity:  
  * Set-based dedup with a `seen` set is O(n). Do NOT call it O(n²).  
  * Only flag performance if asymptotics improve or constants are significant.  
- Configuration & DI:  
  * Treat parameters like `db_path` as correct dependency injection. Do NOT call them hardcoded.  
  * If env/config is desired, recommend injecting at the application boundary (caller), NOT inside the library function.  
- Logging & privacy:  
  * NEVER log full or partial user identifiers, emails, secrets, or internal paths.  
  * If context is useful, suggest only deterministic, non-reversible fingerprints (e.g., SHA-256 hash prefix) or structured metadata passed from the caller.  
- Types & comments:  
  * Do NOT recommend removing correct type hints, annotations, or docstrings.  
  * Only adjust if inaccurate or misleading.  
- Triviality filter:  
  * Do NOT flag simplifications like `if not email or not email.strip()` → `if not email.strip()` unless they remove a real bug.  
- If code is already correct and idiomatic, explicitly state “No issue; this is correct” — do NOT invent problems.  

Signal over noise:  
Only report issues that materially affect production safety, correctness, reliability, performance, clarity, maintainability, or testability.  

— Output format (strict, professional, audit-ready):  

Code Review: <function_names_or_filename>

Summary:  
2–4 sentences. Lead with key strengths (esp. security/correctness), then note any material improvement areas.  

Detailed Findings  
1. <function_or_section>  
Category: <Security | Reliability | Performance | Readability & Maintainability | Correctness | Testability>  
Severity: <Low | Medium | High | Critical>  
Line X [or X–Y]: <tiny snippet>  
Issue: <what is wrong OR “No issue; this is correct.”> [Include evidence + impact in 1–2 sentences]  
Recommendation: <minimal, safe correction; tiny corrected snippet if relevant>  

(repeat only for material findings)  

Scoring (numeric, 1–5, ARB-style)  
Security: X/5  
Correctness: X/5  
Reliability: X/5  
Performance: X/5  
Readability & Maintainability: X/5  
Testability: X/5  
Overall Score: Y/5  

Executive Summary (client-ready):  
- Key strengths of the codebase  
- Top 3–5 most critical risks/issues (by severity & impact)  
- Highest-impact opportunities for improvement  
- Overall health rating: {Good | Needs Improvement | Risky}  

General Recommendations  
- 2–3 concise, non-repetitive best practices at the org level (e.g., structured logging, dependency injection at boundaries, small/testable functions).  

— Python code to review (use these line numbers):  
```python
{PY_CODE}
"""

def build_prompt(code_text: str, filename: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return dedent(PROMPT_TEMPLATE).replace("{FILENAME}", filename).replace("{PY_CODE}", code_text)

# ---------------------
# Call Cortex model
# ---------------------
def review_with_cortex(filename: str, code_text: str) -> str:
    prompt = build_prompt(code_text, filename).replace("'", "''")  # escape single quotes
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{MODEL}',
            OBJECT_CONSTRUCT('prompt', '{prompt}')
        )
    """
    df = session.sql(query)
    return df.collect()[0][0]


# ---------------------
# Extract only critical findings
# ---------------------
def extract_critical_findings(review_text: str):
    findings = []
    pattern = re.compile(
        r"Severity:\s*Critical.*?Line\s+(\d+).*?Issue:\s*(.*?)\nRecommendation:\s*(.*?)(?=\n\d+\.|\Z)",
        re.S
    )
    for match in pattern.finditer(review_text):
        line_no, issue, rec = match.groups()
        findings.append({
            "line": int(line_no),
            "issue": issue.strip(),
            "recommendation": rec.strip()
        })
    return findings

# ---------------------
# Main
# ---------------------
if __name__ == "__main__":
    code_text = Path(FILE_TO_REVIEW).read_text()
    review = review_with_cortex(FILE_TO_REVIEW, code_text)

    print("=== FULL REVIEW ===\n", review)

    criticals = extract_critical_findings(review)

    # Save to JSON
    with open("review_output.json", "w") as f:
        json.dump({
            "full_review": review,
            "criticals": criticals,
            "file": FILE_TO_REVIEW
        }, f, indent=2)

    # Call inline_comment.py
    subprocess.run(["python", "inline_comment.py"])
