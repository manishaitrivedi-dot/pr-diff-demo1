import os, json, re
from pathlib import Path
from snowflake.snowpark import Session

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
# Prompt template
# ---------------------
PROMPT_TEMPLATE = """Please act as a principal-level Python code reviewer.
# CODE TO REVIEW
{PY_CONTENT}
"""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text).replace("{FILE_PATH}", FILE_TO_REVIEW)

# ---------------------
# Cortex call with safe parsing
# ---------------------
def review_with_cortex(model: str, code_text: str) -> dict:
    prompt = build_prompt(code_text)
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

        # Try strict JSON first
        try:
            return json.loads(result)
        except Exception:
            print("⚠️ Response not valid JSON, using text fallback...")

            # Extract summary
            summary_match = re.search(r"Summary:\s*(.*)", result, re.IGNORECASE)
            summary = summary_match.group(1).strip() if summary_match else "No significant issues found"

            # Extract recommendations
            rec_match = re.search(r"Key Recommendations:([\s\S]*)", result, re.IGNORECASE)
            recommendations = []
            if rec_match:
                recommendations = [line.strip(" -0123456789.") 
                                   for line in rec_match.group(1).splitlines() if line.strip()]

            return {
                "summary": summary,
                "detailed_findings": [],   # fallback empty
                "key_recommendations": recommendations,
                "raw_text": result
            }
    except Exception as e:
        return {"summary": f"Error: {e}", "detailed_findings": []}

# ---------------------
# Filter low severity
# ---------------------
def filter_low_severity(json_response: dict) -> dict:
    filtered = json_response.copy()
    if "detailed_findings" in filtered:
        filtered["detailed_findings"] = [
            f for f in filtered["detailed_findings"] if f.get("severity", "").upper() != "LOW"
        ]
    return filtered

# ---------------------
# Extract criticals
# ---------------------
def extract_critical_findings(json_response: dict) -> list:
    findings = []
    for f in json_response.get("detailed_findings", []):
        if f.get("severity", "").upper() == "CRITICAL" and f.get("line_number"):
            findings.append({
                "line": int(f["line_number"]),
                "issue": f.get("finding", ""),
                "recommendation": f.get("finding", ""),
                "severity": "CRITICAL"
            })
    return findings

# ---------------------
# Markdown formatter (GitHub PR)
# ---------------------
def format_for_pr_display(json_response: dict) -> str:
    findings = json_response.get("detailed_findings", [])
    recommendations = json_response.get("key_recommendations", [])

    # Handle summary fallback
    summary = json_response.get("summary", "").strip()
    if not summary or summary.lower() in ["analysis text", "no significant issues found", ""]:
        if findings:
            summary = "Issues were detected during the review."
        else:
            summary = "This file passed automated review — no major problems detected."

    display_text = f"## 🤖 Automated LLM Code Review\n\n"
    display_text += f"**File Reviewed:** `{FILE_TO_REVIEW}`\n\n"
    display_text += f"**Summary:** {summary}\n\n"

    if findings:
        display_text += "### Detailed Findings\n\n"
        for f in findings:
            severity = f.get("severity", "Unknown").upper()
            line = f.get("line_number", "N/A")
            issue = f.get("finding", "No description")
            context = f.get("function_context", "")
            context_text = f"`{context}`" if context else "N/A"

            sev_icon = {
                "CRITICAL": "🔴",
                "HIGH": "🟠",
                "MEDIUM": "🟡",
                "LOW": "🟢"
            }.get(severity, "⚪")

            display_text += f"<details>\n<summary>{sev_icon} **{severity}** at line {line}</summary>\n\n"
            display_text += f"- **Context:** {context_text}\n"
            display_text += f"- **Finding:** {issue}\n\n"
            display_text += "</details>\n\n"
    else:
        display_text += "✅ **No significant issues were detected in this file.**\n\n"

    if recommendations:
        display_text += "### Key Recommendations\n\n"
        for i, rec in enumerate(recommendations, 1):
            display_text += f"{i}. {rec}\n"
        display_text += "\n"

    display_text += "---\n*Generated by Snowflake Cortex AI (llama3.1-70b)*"
    return display_text

# ---------------------
# Interactive HTML
# ---------------------
def generate_interactive_html_report(json_response: dict, original_findings: list) -> str:
    findings = json_response.get("detailed_findings", [])
    file_findings = {FILE_TO_REVIEW: original_findings}

    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Code Review Report</title>
        <style>
            body { font-family: Arial, sans-serif; background: #f8f9fa; color: #343a40; }
            .container { max-width: 1000px; margin: 20px auto; background: white; border:1px solid #ddd; border-radius:8px; }
            .header { padding:20px; border-bottom:1px solid #ddd; font-weight:bold; }
            .file-header { padding:12px 20px; cursor:pointer; display:flex; align-items:center; border-bottom:1px solid #eee; }
            .file-header:hover { background:#f1f3f5; }
            .expand-icon { margin-right:8px; }
            .file-details { display:none; padding:15px 25px; background:#f8f9fa; }
            .file-details.expanded { display:block; }
            .priority-critical { background:#f8d7da; color:#721c24; padding:2px 6px; border-radius:6px; }
            .priority-high { background:#fff3cd; color:#856404; padding:2px 6px; border-radius:6px; }
            .priority-medium { background:#e2e3e5; color:#383d41; padding:2px 6px; border-radius:6px; }
            .issue-item { border:1px solid #ddd; border-radius:6px; margin-bottom:12px; padding:12px; background:white; }
            .issue-title { font-weight:bold; margin-bottom:6px; }
            .issue-location { font-size:0.85em; color:#555; margin-bottom:8px; }
        </style>
    </head>
    <body>
    <div class="container">
        <div class="header">📊 Interactive Code Review Report</div>
    """

    count = 0
    for file, issues in file_findings.items():
        count += 1
        html_content += f"""
        <div class="file-header" id="header-file{count}" onclick="toggleFile('file{count}')">
            <span class="expand-icon" id="expand-file{count}">▶</span>
            <span>📁 {os.path.basename(file)}</span>
            <span class="priority-medium">{len(issues)} Issues</span>
        </div>
        <div class="file-details" id="file{count}">
        """

        for idx, issue in enumerate(issues,1):
            sev = issue.get("severity","")
            finding = issue.get("finding","")
            line = issue.get("line_number","N/A")
            html_content += f"""
            <div class="issue-item">
                <div class="issue-title">[{sev}] {finding[:80]}</div>
                <div class="issue-location">Line {line}</div>
            </div>
            """

        html_content += "</div>"

    html_content += """
    </div>
    <script>
        function toggleFile(fileId) {
            const details=document.getElementById(fileId);
            const icon=document.getElementById('expand-'+fileId);
            if(details.classList.contains('expanded')) {
                details.classList.remove('expanded');
                icon.textContent='▶';
            } else {
                details.classList.add('expanded');
                icon.textContent='▼';
            }
        }
        document.addEventListener('DOMContentLoaded',()=>toggleFile('file1'));
    </script>
    </body>
    </html>
    """
    return html_content

# ---------------------
# Main
# ---------------------
if __name__ == "__main__":
    try:
        if not os.path.exists(FILE_TO_REVIEW):
            print(f"File {FILE_TO_REVIEW} not found"); exit(1)
        code_text = Path(FILE_TO_REVIEW).read_text()
        report = review_with_cortex(MODEL, code_text)
        original_findings = report.get("detailed_findings", [])
        filtered = filter_low_severity(report)
        criticals = extract_critical_findings(filtered)

        # Markdown for PR
        formatted_review = format_for_pr_display(filtered)

        # Interactive HTML
        html_report = generate_interactive_html_report(filtered, original_findings)
        with open("dbt_code_review_report.html","w") as f: f.write(html_report)

        # JSON output
        output_data = {
            "full_review": formatted_review,              # for inline_comment.py
            "full_review_markdown": formatted_review,
            "full_review_json": filtered,
            "criticals": criticals,
            "file": FILE_TO_REVIEW,
            "interactive_report_path": "dbt_code_review_report.html"
        }
        with open("review_output.json","w") as f: json.dump(output_data, f, indent=2)

        print("✅ Markdown review saved for PR")
        print("✅ Interactive HTML report saved to dbt_code_review_report.html")
    finally:
        session.close()
