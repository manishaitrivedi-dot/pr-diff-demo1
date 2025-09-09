import os, json, re, subprocess
from pathlib import Path
from textwrap import dedent
from snowflake.snowpark import Session

# ---------------------
# Config
# ---------------------
MODEL = "mistral-large2"
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
# Prompt template (simplified for API compatibility)
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


def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CODE}", code_text)

# ---------------------
# Call Cortex model (fixed version)
# ---------------------
def review_with_cortex(code_text: str) -> str:
    prompt = build_prompt(code_text)
    
    # Clean the prompt to avoid quote issues
    clean_prompt = prompt.replace("'", "''").replace("\n", "\\n").replace("\r", "")
    
    # Use direct string approach (most compatible)
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{MODEL}',
            '{clean_prompt}'
        )
    """
    
    try:
        df = session.sql(query)
        result = df.collect()[0][0]
        return result
    except Exception as e:
        print(f"Cortex API error: {e}")
        # Fallback with even simpler prompt
        simple_prompt = f"Review this Python code for critical issues:\\n{code_text[:1000]}"
        simple_prompt = simple_prompt.replace("'", "''")
        
        fallback_query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3-8b',
                '{simple_prompt}'
            )
        """
        
        df = session.sql(fallback_query)
        return df.collect()[0][0]

# ---------------------
# Extract critical findings
# ---------------------
def extract_critical_findings(review_text: str):
    findings = []
    
    # Split by "---" or "LINE:" sections
    sections = re.split(r'(?:---|LINE:)', review_text)
    
    for section in sections[1:]:  # Skip first empty section
        lines = section.strip().split('\n')
        finding = {}
        
        for line in lines:
            if line.strip():
                if line.upper().startswith('SEVERITY:'):
                    severity = line.split(':', 1)[1].strip()
                    finding['severity'] = severity
                elif line.upper().startswith('ISSUE:'):
                    issue = line.split(':', 1)[1].strip()
                    finding['issue'] = issue
                elif line.upper().startswith('RECOMMENDATION:'):
                    rec = line.split(':', 1)[1].strip()
                    finding['recommendation'] = rec
                elif line.isdigit():
                    finding['line'] = int(line)
        
        # Only include Critical severity findings
        if finding.get('severity', '').upper() == 'CRITICAL' and finding.get('line'):
            findings.append({
                "line": finding['line'],
                "issue": finding.get('issue', 'Critical issue found'),
                "recommendation": finding.get('recommendation', 'Review and fix this issue')
            })
    
    return findings

# ---------------------
# GitHub comment posting
# ---------------------
def post_github_comments(criticals, full_review):
    """Post comments to GitHub using existing inline_comment.py approach"""
    
    # Get environment variables
    github_token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if not github_token:
        print("No GitHub token found, skipping comment posting")
        return
    
    import requests
    
    REPO_OWNER = "manishaitrivedi-dot"
    REPO_NAME = "pr-diff-demo1"
    PR_NUMBER = int(os.environ.get('PR_NUMBER', 3))
    
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github+json"
    }
    
    # Get latest commit SHA
    commits_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{PR_NUMBER}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    if commits_resp.status_code != 200:
        print("Failed to get commit SHA")
        return
    
    commit_sha = commits_resp.json()[-1]["sha"]
    
    # Post general PR review
    review_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{PR_NUMBER}/reviews"
    
    review_body = f"""## Automated LLM Code Review
    
**File Reviewed:** {FILE_TO_REVIEW}
**Critical Issues Found:** {len(criticals)}

### Summary
{full_review[:500]}...

### Critical Issues
"""
    
    for critical in criticals:
        review_body += f"""
**Line {critical['line']}:** {critical['issue']}
*Recommendation:* {critical['recommendation']}
"""
    
    review_data = {
        "body": review_body,
        "event": "COMMENT"
    }
    
    review_resp = requests.post(review_url, headers=headers, json=review_data)
    if review_resp.status_code == 200:
        print("Posted general PR review")
    else:
        print(f"Failed to post PR review: {review_resp.status_code}")
    
    # Post inline comments for critical issues
    comment_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{PR_NUMBER}/comments"
    
    posted_count = 0
    for critical in criticals:
        comment_data = {
            "body": f"**CRITICAL ISSUE**\\n\\n{critical['issue']}\\n\\n**Recommendation:** {critical['recommendation']}",
            "commit_id": commit_sha,
            "path": FILE_TO_REVIEW,
            "line": critical['line'],
            "side": "RIGHT"
        }
        
        comment_resp = requests.post(comment_url, headers=headers, json=comment_data)
        if comment_resp.status_code == 201:
            posted_count += 1
            print(f"Posted inline comment on line {critical['line']}")
        else:
            print(f"Failed to post inline comment on line {critical['line']}: {comment_resp.status_code}")
    
    print(f"Posted {posted_count}/{len(criticals)} inline comments")

# ---------------------
# Main
# ---------------------
if __name__ == "__main__":
    try:
        # Read the file
        if not os.path.exists(FILE_TO_REVIEW):
            print(f"File {FILE_TO_REVIEW} not found")
            exit(1)
        
        code_text = Path(FILE_TO_REVIEW).read_text()
        print(f"Reviewing {FILE_TO_REVIEW} ({len(code_text)} characters)")
        
        # Get LLM review
        print("Getting LLM review from Snowflake Cortex...")
        review = review_with_cortex(code_text)
        
        print("=== FULL REVIEW ===")
        print(review)
        print("=" * 50)
        
        # Extract critical findings
        criticals = extract_critical_findings(review)
        print(f"Found {len(criticals)} critical issues")
        
        # Save to JSON
        output_data = {
            "full_review": review,
            "criticals": criticals,
            "file": FILE_TO_REVIEW
        }
        
        with open("review_output.json", "w") as f:
            json.dump(output_data, f, indent=2)
        
        print("Saved review to review_output.json")
        
        # Post to GitHub if in CI environment
        if os.environ.get('GITHUB_ACTIONS'):
            print("Posting comments to GitHub...")
            post_github_comments(criticals, review)
        else:
            print("Not in GitHub Actions, skipping comment posting")
            
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
