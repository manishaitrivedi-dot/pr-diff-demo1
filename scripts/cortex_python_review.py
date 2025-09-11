import os, json, re, subprocess
from pathlib import Path
from textwrap import dedent
from snowflake.snowpark import Session
import pandas as pd

# ---------------------
# Config
# ---------------------
MODEL = "llama3.1-70b"
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
# FIXED: Your original prompt template with correct file path
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
-   Do NOT propose APIs that don't exist for the imported modules.
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
**File:** `{FILE_PATH}`
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

# FIXED: build_prompt function to replace file path correctly
def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text).replace("{FILE_PATH}", FILE_TO_REVIEW)

# ---------------------
# Call Cortex model
# ---------------------
def review_with_cortex(model: str, code_text: str) -> dict:
    """
    Calls Cortex and returns structured JSON response
    """
    prompt = build_prompt(code_text)
    
    # Clean the prompt to avoid quote issues
    clean_prompt = prompt.replace("'", "''").replace("\n", "\\n").replace("\r", "")
    
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{clean_prompt}'
        )
    """
    
    try:
        print(f"Getting review from {model}...")
        df = session.sql(query)
        result = df.collect()[0][0]
        
        print("=== RAW CORTEX RESPONSE ===")
        print(result[:1000] + "..." if len(result) > 1000 else result)
        print("=" * 50)
        
        # Try to parse as JSON
        try:
            json_response = json.loads(result)
            print("Successfully parsed JSON response")
            return json_response
        except json.JSONDecodeError:
            print("Response is not JSON format, attempting extraction...")
            # Try to find JSON in response
            json_start = result.find('{')
            json_end = result.rfind('}') + 1
            if json_start != -1 and json_end != 0:
                json_str = result[json_start:json_end]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    pass
            
            # Return text response wrapped in basic structure
            return {
                "summary": "Analysis completed (text format)",
                "detailed_findings": [],
                "key_recommendations": ["Review text response manually"],
                "raw_text": result
            }
        
    except Exception as e:
        print(f"Cortex API error: {e}")
        return {
            "summary": f"Error occurred: {e}",
            "detailed_findings": [],
            "key_recommendations": ["Manual review recommended due to API error"]
        }

# ---------------------
# Filter LOW severity from JSON
# ---------------------
def filter_low_severity(json_response: dict) -> dict:
    """Remove LOW severity findings from JSON response"""
    filtered_response = json_response.copy()
    
    if "detailed_findings" in filtered_response:
        original_count = len(filtered_response["detailed_findings"])
        
        filtered_findings = [
            finding for finding in filtered_response["detailed_findings"]
            if finding.get("severity", "").upper() != "LOW"
        ]
        
        filtered_response["detailed_findings"] = filtered_findings
        print(f"Filtered out {original_count - len(filtered_findings)} LOW severity findings")
    
    return filtered_response

# ---------------------
# Extract critical findings for inline comments (dynamic)
# ---------------------
def extract_critical_findings(json_response: dict) -> list:
    """Extract CRITICAL findings for inline comments - NO hardcoded line numbers"""
    findings = []
    
    detailed_findings = json_response.get("detailed_findings", [])
    
    for finding in detailed_findings:
        severity = finding.get("severity", "").upper()
        line_number = finding.get("line_number")
        
        # Only CRITICAL severity for inline comments
        if severity == "CRITICAL" and line_number:
            findings.append({
                "line": int(line_number),
                "issue": finding.get("finding", "Critical issue found"),
                "recommendation": finding.get("finding", "Address this critical issue"),
                "severity": severity
            })
    
    print(f"Found {len(findings)} critical issues for inline comments")
    if findings:
        print(f"Critical lines: {[f['line'] for f in findings]}")
    
    return findings

# ---------------------
# FIXED: Format for PR display - SINGLE LINE FORMAT
# ---------------------
def format_for_pr_display(json_response: dict) -> str:
    """Format JSON response for clean, single-line PR comment display"""
    
    # Handle raw text response (parse and reformat)
    if "raw_text" in json_response:
        raw_text = json_response["raw_text"]
        
        # Extract summary
        summary_match = re.search(r'## Code Review Summary\s*\n(.*?)(?=\n###|\n---|\nDetailed Findings|$)', raw_text, re.DOTALL)
        summary = summary_match.group(1).strip() if summary_match else "Code review completed"
        
        formatted_text = f"**Summary:** {summary}\n\n"
        formatted_text += "**Detailed Findings:**\n\n"
        formatted_text += f"**File:** `{FILE_TO_REVIEW}`\n\n"
        
        # Parse bullet point findings and convert to single line format
        finding_pattern = r'\*\s*\*\*Severity:\*\*\s*([^\n]*)\n\*\s*\*\*Line:\*\*\s*([^\n]*)\n\*\s*\*\*Function/Context:\*\*\s*([^\n]*)\n\*\s*\*\*Finding:\*\*\s*([^\n]*(?:\n(?!\*)[^\n]*)*)'
        findings = re.findall(finding_pattern, raw_text, re.MULTILINE)
        
        if findings:
            for severity, line, context, finding in findings:
                severity = severity.strip()
                line = line.strip()
                context = context.strip().strip('`')
                finding = finding.strip()
                
                # Skip LOW severity
                if severity.upper() != "LOW":
                    context_formatted = f"`{context}`" if context else "N/A"
                    formatted_text += f"→ **Severity:** {severity}; **Line:** {line}; **Function/Context:** {context_formatted}; **Finding:** {finding}\n\n"
        else:
            formatted_text += "**No significant issues found.**\n\n"
        
        # Extract recommendations
        rec_match = re.search(r'### Key Recommendations\s*\n(.*?)(?=\n---|$)', raw_text, re.DOTALL)
        if rec_match:
            rec_text = rec_match.group(1).strip()
            if rec_text:
                formatted_text += "**Key Recommendations:**\n\n"
                rec_items = re.findall(r'(\d+\.\s*[^\n]+(?:\n(?!\d+\.)[^\n]+)*)', rec_text)
                for rec in rec_items:
                    formatted_text += f"{rec.strip()}\n"
                formatted_text += "\n"
        
        formatted_text += "---\n*Generated by Snowflake Cortex AI (llama3.1-70b)*"
        return formatted_text
    
    # Handle JSON format response
    summary = json_response.get("summary", "Code review completed")
    findings = json_response.get("detailed_findings", [])
    recommendations = json_response.get("key_recommendations", [])
    
    display_text = f"**Summary:** {summary}\n\n"
    
    if findings:
        display_text += "**Detailed Findings:**\n\n"
        display_text += f"**File:** `{FILE_TO_REVIEW}`\n\n"
        
        for finding in findings:
            severity = finding.get("severity", "Unknown")
            line = finding.get("line_number", "N/A")
            issue = finding.get("finding", "No description")
            context = finding.get("function_context", "")
            
            # SINGLE LINE FORMAT - all info on one line
            context_text = f"`{context}`" if context else "N/A"
            display_text += f"→ **Severity:** {severity}; **Line:** {line}; **Function/Context:** {context_text}; **Finding:** {issue}\n\n"
    else:
        display_text += "**No significant issues found.**\n\n"
    
    if recommendations:
        display_text += "**Key Recommendations:**\n\n"
        for i, rec in enumerate(recommendations, 1):
            display_text += f"{i}. {rec}\n"
        display_text += "\n"
    
    display_text += "---\n*Generated by Snowflake Cortex AI (llama3.1-70b)*"
    
    return display_text

# ---------------------
# Main execution
# ---------------------
if __name__ == "__main__":
    try:
        # Read the file
        if not os.path.exists(FILE_TO_REVIEW):
            print(f"File {FILE_TO_REVIEW} not found")
            exit(1)
        
        code_text = Path(FILE_TO_REVIEW).read_text()
        print(f"Reviewing {FILE_TO_REVIEW} ({len(code_text)} characters)")
        
        # Call Cortex with your exact usage
        print("Getting review from Cortex...")
        report = review_with_cortex('llama3.1-70b', code_text)
        
        print("=== ORIGINAL JSON RESPONSE ===")
        print(json.dumps(report, indent=2))
        print("=" * 50)
        
        # Create DataFrame as you wanted
        detailed_findings = report.get("detailed_findings", [])
        if detailed_findings:
            df = pd.DataFrame(detailed_findings)
            print("=== FINDINGS DATAFRAME ===")
            print(df.to_string())
            print("=" * 50)
        else:
            print("=== NO DETAILED FINDINGS FOR DATAFRAME ===")
        
        # Filter LOW severity
        filtered_json = filter_low_severity(report.copy())
        
        print("=== FILTERED JSON (NO LOW SEVERITY) ===")
        print(json.dumps(filtered_json, indent=2))
        print("=" * 50)
        
        # Extract critical findings for inline comments (dynamic, no hardcoded lines)
        criticals = extract_critical_findings(filtered_json)
        
        # Format for PR display
        formatted_review = format_for_pr_display(filtered_json)
        
        # Save output in the format your inline_comment.py expects
        output_data = {
            "full_review": formatted_review,
            "full_review_json": filtered_json,
            "criticals": criticals,  # Dynamic based on LLM detection
            "file": FILE_TO_REVIEW
        }
        
        with open("review_output.json", "w") as f:
            json.dump(output_data, f, indent=2)
        
        print("=== SUMMARY ===")
        print(f"Total findings: {len(detailed_findings)}")
        print(f"After LOW filtering: {len(filtered_json.get('detailed_findings', []))}")
        print(f"Critical for inline comments: {len(criticals)}")
        if criticals:
            print(f"Critical lines: {[c['line'] for f in criticals]}")
        print("Review saved to review_output.json")
        
        # Close Snowflake session
        session.close()
            
    except Exception as e:
        print(f"Error: {e}")
        if 'session' in locals():
            session.close()
        exit(1)
