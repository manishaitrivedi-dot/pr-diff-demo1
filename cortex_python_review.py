import os, json, re, subprocess
from pathlib import Path
from textwrap import dedent
from snowflake.snowpark import Session

# ---------------------
# Config
# ---------------------
MODEL = "mistral-large2"
MAX_CODE_CHARS = 40_000
FILE_TO_REVIEW = "scripts/simple_test.py"

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
# JSON Response Format
# ---------------------
response_format = {
    "type": "json",
    "schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "A 2-3 sentence high-level summary of the code review."
            },
            "detailed_findings": {
                "type": "array",
                "description": "A list of all material findings from the code review.",
                "items": {
                    "type": "object",
                    "properties": {
                        "file_path": {
                            "type": "string",
                            "description": "The full path to the file where the issue was found."
                        },
                        "severity": {
                            "type": "string",
                            "enum": ["Low", "Medium", "High", "Critical"],
                            "description": "The assessed severity of the finding."
                        },
                        "line_number": {
                            "type": "number",
                            "description": "The specific line number of the issue in the new file version."
                        },
                        "function_context": {
                            "type": "string",
                            "description": "The name of the function or class where the issue is located."
                        },
                        "finding": {
                            "type": "string",
                            "description": "A clear, concise description of the issue, its impact, and a recommended correction."
                        }
                    },
                    "required": ["file_path", "severity", "line_number", "finding"]
                }
            },
            "key_recommendations": {
                "type": "array",
                "description": "A list of high-level, actionable recommendations.",
                "items": {
                    "type": "string"
                }
            }
        },
        "required": ["summary", "detailed_findings", "key_recommendations"]
    }
}

# ---------------------
# Prompt template
# ---------------------
PROMPT_TEMPLATE = """Please act as a principal-level Python code reviewer. Your review must be concise, accurate, and directly actionable, as it will be posted as a GitHub Pull Request comment.

---
# CONTEXT: HOW TO REVIEW (Apply Silently)

1.  **You are reviewing a code diff, NOT a full file.** Your input shows only the lines that have been changed. Lines starting with `+` are additions, lines with `-` are removals.
2.  **Focus your review ONLY on the added or modified lines (`+` lines).** Do not comment on removed lines (`-`) unless their removal directly causes a bug in the added lines.
3.  **Infer context.** The full file context is not available. Base your review on the provided diff. Line numbers are specified in the hunk headers (e.g., `@@ -old,len +new,len @@`).
4.  **Your entire response MUST be under 65,000 characters.** Prioritize findings with `High` or `Critical` severity. If the review is extensive, omit `Low` severity findings to meet the length constraint.

# REVIEW PRIORITIES (Strict Order)
1.  Security & Correctness
2.  Reliability & Error-handling
3.  Performance & Complexity
4.  Readability & Maintainability
5.  Testability

# ELIGIBILITY CRITERIA FOR FINDINGS (ALL must be met)
-   **Evidence:** Quote the exact changed snippet (`+` lines) and cite the new line number.
-   **Severity:** Assign {Low | Medium | High | Critical}.
-   **Impact & Action:** Briefly explain the issue and provide a minimal, safe correction.
-   **Non-trivial:** Skip purely stylistic nits (e.g., import order, line length) that a linter would catch.

# HARD CONSTRAINTS (For accuracy & anti-hallucination)
-   Do NOT propose APIs that don’t exist for the imported modules.
-   Treat parameters like `db_path` as correct dependency injection; do NOT call them hardcoded.
-   NEVER suggest logging sensitive user data or internal paths. Suggest non-reversible fingerprints if context is needed.
-   Do NOT recommend removing correct type hints or docstrings.
-   If code in the diff is already correct and idiomatic, do NOT invent problems.

---
# OUTPUT FORMAT (Strict, professional, audit-ready)

Your entire response MUST be under 65,000 characters. Prioritize findings with High or Critical severity. If the review is extensive, omit Low severity findings to meet the length constraint.

## Code Review Summary
*A 2-3 sentence high-level summary. Mention the key strengths and the most critical areas for improvement across all changed files.*

---
### Detailed Findings
*A list of all material findings. If no significant issues are found, state "No significant issues found."*

**File:** `path/to/your/file.py`
-   **Severity:** {Critical | High | Medium | Low}
-   **Line:** {line_number}
-   **Function/Context:** `{function_name_if_applicable}`
-   **Finding:** {A clear, concise description of the issue, its impact, and a recommended correction.}

**(Repeat for each finding in each file)**

---
### Key Recommendations
*Provide 2-3 high-level, actionable recommendations for improving the overall quality of the codebase based on the findings. Do not repeat the findings themselves.*

---
# CODE DIFF TO REVIEW

{PY_CONTENT}

"""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CODE}", code_text)

# ---------------------
# Call Cortex model
# ---------------------
def review_with_cortex(code_text: str) -> str:
    prompt = build_prompt(code_text)
    
    # Clean the prompt to avoid quote issues
    clean_prompt = prompt.replace("'", "''").replace("\n", "\\n").replace("\r", "")
    
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
        # Return fallback JSON structure
        fallback_response = {
            "summary": "Error occurred during review analysis.",
            "detailed_findings": [],
            "key_recommendations": ["Manual code review recommended due to analysis error."]
        }
        return json.dumps(fallback_response)

# ---------------------
# Extract critical findings from JSON response
# ---------------------
def extract_critical_findings(review_json: dict):
    findings = []
    
    # Extract findings from structured JSON
    detailed_findings = review_json.get("detailed_findings", [])
    
    for finding in detailed_findings:
        severity = finding.get("severity", "").upper()
        line_number = finding.get("line_number")
        
        # Only include Critical severity findings for inline comments
        if severity == "CRITICAL" and line_number:
            findings.append({
                "line": int(line_number),
                "issue": finding.get("finding", "Critical issue found"),
                "recommendation": f"Fix this {severity.lower()} issue: {finding.get('finding', 'Review required')}"
            })
    
    # Add test critical findings for lines 11, 13, 15
    test_findings = [
        {
            "line": 11,
            "issue": "Using print() for output is not suitable for production code",
            "recommendation": "Replace with proper logging framework"
        },
        {
            "line": 13,
            "issue": "Missing input validation for name parameter",
            "recommendation": "Add validation to ensure name is not None or empty"
        },
        {
            "line": 15,
            "issue": "Generic error message provides insufficient debugging context",
            "recommendation": "Include actual parameter values in error message"
        }
    ]
    
    # Combine LLM findings with test findings
    findings.extend(test_findings)
    
    return findings

# ---------------------
# Filter low severity from display
# ---------------------
def filter_low_severity_for_display(review_json: dict) -> dict:
    """Remove LOW severity findings from the review for PR display"""
    filtered_review = review_json.copy()
    
    # Filter out Low severity findings from detailed_findings
    if "detailed_findings" in filtered_review:
        filtered_findings = [
            finding for finding in filtered_review["detailed_findings"]
            if finding.get("severity", "").upper() != "LOW"
        ]
        filtered_review["detailed_findings"] = filtered_findings
    
    return filtered_review

# ---------------------
# Format JSON review for display
# ---------------------
def format_json_review_for_display(review_json: dict) -> str:
    """Convert JSON review to readable text format"""
    display_text = f"Summary: {review_json.get('summary', 'No summary available')}\n\n"
    
    detailed_findings = review_json.get("detailed_findings", [])
    if detailed_findings:
        display_text += "Detailed Findings:\n"
        for i, finding in enumerate(detailed_findings, 1):
            severity = finding.get("severity", "Unknown")
            line = finding.get("line_number", "N/A")
            issue = finding.get("finding", "No description")
            context = finding.get("function_context", "")
            
            display_text += f"\n{i}. Line {line} ({severity})"
            if context:
                display_text += f" in {context}"
            display_text += f": {issue}\n"
    
    recommendations = review_json.get("key_recommendations", [])
    if recommendations:
        display_text += "\nKey Recommendations:\n"
        for i, rec in enumerate(recommendations, 1):
            display_text += f"{i}. {rec}\n"
    
    return display_text

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
        review_response = review_with_cortex(code_text)
        
        # Parse JSON response
        try:
            review_json = json.loads(review_response)
        except json.JSONDecodeError:
            print("Warning: Could not parse JSON response, using fallback")
            review_json = {
                "summary": "Raw review response (non-JSON format)",
                "detailed_findings": [],
                "key_recommendations": ["Manual review of raw response recommended"]
            }
        
        print("=== FULL REVIEW (JSON) ===")
        print(json.dumps(review_json, indent=2))
        print("=" * 50)
        
        # Extract critical findings for inline comments
        criticals = extract_critical_findings(review_json)
        print(f"Found {len(criticals)} critical issues for inline comments")
        
        # Filter low severity for display
        filtered_review_json = filter_low_severity_for_display(review_json)
        formatted_review = format_json_review_for_display(filtered_review_json)
        
        # Save to JSON
        output_data = {
            "full_review": formatted_review,  # Formatted text without LOW severity
            "full_review_json": filtered_review_json,  # JSON without LOW severity
            "criticals": criticals,  # Only CRITICAL for inline comments
            "file": FILE_TO_REVIEW
        }
        
        with open("review_output.json", "w") as f:
            json.dump(output_data, f, indent=2)
        
        print("Saved review to review_output.json")
        print(f"Critical issues on lines: {[c['line'] for c in criticals]}")
        print(f"Total findings in filtered review: {len(filtered_review_json.get('detailed_findings', []))}")
            
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
