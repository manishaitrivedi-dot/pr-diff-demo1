import os, json, re
from pathlib import Path
from snowflake.snowpark import Session
import pandas as pd
from datetime import datetime

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
# CODE DIFF TO REVIEW
{PY_CONTENT}
"""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text).replace("{FILE_PATH}", FILE_TO_REVIEW)

# ---------------------
# Cortex call
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

        try:
            return json.loads(result)
        except Exception:
            return {"summary": "Analysis text", "detailed_findings": [], "raw_text": result}
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
# Enhanced Markdown formatter with colors and expandable sections
# ---------------------
def format_for_pr_display(json_response: dict) -> str:
    summary = json_response.get("summary", "Code review completed")
    findings = json_response.get("detailed_findings", [])
    recommendations = json_response.get("key_recommendations", [])
    
    # Count findings by severity
    critical_count = sum(1 for f in findings if f.get("severity", "").upper() == "CRITICAL")
    high_count = sum(1 for f in findings if f.get("severity", "").upper() == "HIGH")
    medium_count = sum(1 for f in findings if f.get("severity", "").upper() == "MEDIUM")

    display_text = f"## 🤖 Automated LLM Code Review\n\n"
    display_text += f"**📁 File Reviewed:** `{FILE_TO_REVIEW}`\n\n"
    
    # Add severity summary with emoji indicators
    if findings:
        display_text += f"### 📊 Issues Summary\n"
        if critical_count > 0:
            display_text += f"🔴 **Critical:** {critical_count} issues\n"
        if high_count > 0:
            display_text += f"🟠 **High:** {high_count} issues\n"
        if medium_count > 0:
            display_text += f"🟡 **Medium:** {medium_count} issues\n"
        display_text += "\n"
    
    display_text += f"**📝 Summary:** {summary}\n\n"

    if findings:
        display_text += "<details>\n<summary><strong>🔍 Detailed Findings</strong> (Click to expand)</summary>\n\n"
        
        # Group findings by severity
        critical_findings = [f for f in findings if f.get("severity", "").upper() == "CRITICAL"]
        high_findings = [f for f in findings if f.get("severity", "").upper() == "HIGH"]
        medium_findings = [f for f in findings if f.get("severity", "").upper() == "MEDIUM"]
        
        # Display critical findings first
        if critical_findings:
            display_text += "### 🔴 Critical Issues\n\n"
            for f in critical_findings:
                line = f.get("line_number", "N/A")
                issue = f.get("finding", "No description")
                context = f.get("function_context", "")
                context_text = f"`{context}`" if context else "N/A"
                display_text += f"- **Line {line}** | **Context:** {context_text}\n"
                display_text += f"  > {issue}\n\n"
        
        # Display high findings
        if high_findings:
            display_text += "### 🟠 High Priority Issues\n\n"
            for f in high_findings:
                line = f.get("line_number", "N/A")
                issue = f.get("finding", "No description")
                context = f.get("function_context", "")
                context_text = f"`{context}`" if context else "N/A"
                display_text += f"- **Line {line}** | **Context:** {context_text}\n"
                display_text += f"  > {issue}\n\n"
        
        # Display medium findings
        if medium_findings:
            display_text += "### 🟡 Medium Priority Issues\n\n"
            for f in medium_findings:
                line = f.get("line_number", "N/A")
                issue = f.get("finding", "No description")
                context = f.get("function_context", "")
                context_text = f"`{context}`" if context else "N/A"
                display_text += f"- **Line {line}** | **Context:** {context_text}\n"
                display_text += f"  > {issue}\n\n"
        
        display_text += "</details>\n\n"
    else:
        display_text += "✅ **No significant issues found.**\n\n"

    if recommendations:
        display_text += "<details>\n<summary><strong>💡 Key Recommendations</strong> (Click to expand)</summary>\n\n"
        for i, rec in enumerate(recommendations, 1):
            display_text += f"{i}. {rec}\n"
        display_text += "\n</details>\n\n"

    display_text += "---\n*🔬 Generated by Snowflake Cortex AI (llama3.1-70b)*"
    return display_text

# ---------------------
# Generate Enhanced Interactive HTML with better styling
# ---------------------
def generate_interactive_html_report(json_response: dict, original_findings: list) -> str:
    findings = json_response.get("detailed_findings", [])
    file_findings = {FILE_TO_REVIEW: original_findings}
    
    # Count findings by severity for better priority display
    critical_count = sum(1 for f in findings if f.get("severity", "").upper() == "CRITICAL")
    high_count = sum(1 for f in findings if f.get("severity", "").upper() == "HIGH")
    medium_count = sum(1 for f in findings if f.get("severity", "").upper() == "MEDIUM")
    
    priority_class = "priority-critical" if critical_count > 0 else ("priority-high" if high_count > 0 else "priority-medium")
    priority_text = f"Critical: {critical_count}, High: {high_count}, Medium: {medium_count}"

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Code Review Report</title>
        <style>
            body {{ 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                color: #343a40; 
                margin: 0; 
                padding: 20px; 
                min-height: 100vh;
            }}
            .container {{ 
                max-width: 1200px; 
                margin: 0 auto; 
                background: white; 
                border-radius: 12px; 
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                overflow: hidden;
            }}
            .header {{ 
                padding: 30px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                color: white; 
                font-size: 1.5em; 
                font-weight: bold; 
                text-align: center;
            }}
            .stats-bar {{
                display: flex;
                justify-content: space-around;
                padding: 20px;
                background: #f8f9fa;
                border-bottom: 1px solid #dee2e6;
            }}
            .stat-item {{
                text-align: center;
                flex: 1;
            }}
            .stat-number {{
                font-size: 2em;
                font-weight: bold;
                margin-bottom: 5px;
            }}
            .stat-critical {{ color: #dc3545; }}
            .stat-high {{ color: #fd7e14; }}
            .stat-medium {{ color: #ffc107; }}
            .file-header {{ 
                padding: 20px; 
                cursor: pointer; 
                display: flex; 
                align-items: center; 
                border-bottom: 1px solid #dee2e6; 
                background: white;
                transition: background-color 0.2s;
            }}
            .file-header:hover {{ 
                background: #f8f9fa; 
            }}
            .expand-icon {{ 
                margin-right: 15px; 
                font-size: 1.2em;
                transition: transform 0.2s;
            }}
            .expand-icon.expanded {{
                transform: rotate(90deg);
            }}
            .file-details {{ 
                display: none; 
                padding: 0; 
                background: #f8f9fa; 
            }}
            .file-details.expanded {{ 
                display: block; 
                animation: slideDown 0.3s ease-out;
            }}
            @keyframes slideDown {{
                from {{ opacity: 0; max-height: 0; }}
                to {{ opacity: 1; max-height: 1000px; }}
            }}
            .priority-badge {{ 
                margin-left: auto; 
                padding: 8px 16px; 
                border-radius: 20px; 
                font-size: 0.85em; 
                font-weight: bold;
            }}
            .priority-critical {{ 
                background: linear-gradient(135deg, #dc3545, #c82333); 
                color: white; 
            }}
            .priority-high {{ 
                background: linear-gradient(135deg, #fd7e14, #e55a00); 
                color: white; 
            }}
            .priority-medium {{ 
                background: linear-gradient(135deg, #ffc107, #e0a800); 
                color: #212529; 
            }}
            .severity-section {{
                margin: 20px;
            }}
            .severity-title {{
                padding: 15px 20px;
                margin: 0 0 10px 0;
                border-radius: 8px;
                font-weight: bold;
                font-size: 1.1em;
            }}
            .severity-critical {{ 
                background: linear-gradient(135deg, #dc3545, #c82333); 
                color: white; 
            }}
            .severity-high {{ 
                background: linear-gradient(135deg, #fd7e14, #e55a00); 
                color: white; 
            }}
            .severity-medium {{ 
                background: linear-gradient(135deg, #ffc107, #e0a800); 
                color: #212529; 
            }}
            .issue-item {{ 
                border: 1px solid #dee2e6; 
                border-radius: 8px; 
                margin-bottom: 15px; 
                padding: 20px; 
                background: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                transition: transform 0.2s, box-shadow 0.2s;
            }}
            .issue-item:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 8px rgba(0,0,0,0.15);
            }}
            .issue-title {{ 
                font-weight: bold; 
                margin-bottom: 10px; 
                font-size: 1.05em;
                color: #495057;
            }}
            .issue-location {{ 
                font-size: 0.9em; 
                color: #6c757d; 
                margin-bottom: 12px;
                display: flex;
                align-items: center;
            }}
            .line-badge {{
                background: #007bff;
                color: white;
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 0.8em;
                margin-right: 10px;
            }}
            .code-diff {{ 
                border: 1px solid #dee2e6; 
                margin-top: 15px; 
                border-radius: 8px; 
                overflow: hidden;
            }}
            .diff-header {{ 
                background: #e9ecef; 
                padding: 10px 15px; 
                cursor: pointer; 
                font-size: 0.9em; 
                font-weight: bold;
                transition: background-color 0.2s;
            }}
            .diff-header:hover {{
                background: #dee2e6;
            }}
            .diff-body {{ 
                display: none; 
            }}
            .diff-body.expanded {{
                display: block;
                animation: fadeIn 0.3s ease-out;
            }}
            @keyframes fadeIn {{
                from {{ opacity: 0; }}
                to {{ opacity: 1; }}
            }}
            .current-code {{ 
                background: #f8d7da; 
                padding: 15px; 
                font-family: 'Courier New', monospace; 
                font-size: 0.85em; 
                border-bottom: 1px solid #f5c6cb;
            }}
            .optimized-code {{ 
                background: #d4edda; 
                padding: 15px; 
                font-family: 'Courier New', monospace; 
                font-size: 0.85em; 
            }}
            .no-issues {{
                text-align: center;
                padding: 60px 20px;
                color: #28a745;
                font-size: 1.2em;
            }}
            .no-issues .checkmark {{
                font-size: 3em;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
    <div class="container">
        <div class="header">📊 Interactive Code Review Report</div>
        
        <div class="stats-bar">
            <div class="stat-item">
                <div class="stat-number stat-critical">{critical_count}</div>
                <div>Critical</div>
            </div>
            <div class="stat-item">
                <div class="stat-number stat-high">{high_count}</div>
                <div>High</div>
            </div>
            <div class="stat-item">
                <div class="stat-number stat-medium">{medium_count}</div>
                <div>Medium</div>
            </div>
        </div>
    """

    count = 0
    for file, issues in file_findings.items():
        count += 1
        html_content += f"""
        <div class="file-header" onclick="toggleFile('file{count}')">
            <span class="expand-icon" id="expand-file{count}">▶</span>
            <span>📁 {os.path.basename(file)}</span>
            <div class="priority-badge {priority_class}">{priority_text}</div>
        </div>
        <div class="file-details" id="file{count}">
        """

        if not issues:
            html_content += """
            <div class="no-issues">
                <div class="checkmark">✅</div>
                <div>No issues found in this file!</div>
            </div>
            """
        else:
            # Group issues by severity
            critical_issues = [f for f in issues if f.get("severity", "").upper() == "CRITICAL"]
            high_issues = [f for f in issues if f.get("severity", "").upper() == "HIGH"]
            medium_issues = [f for f in issues if f.get("severity", "").upper() == "MEDIUM"]
            
            # Display issues by severity
            for severity, issues_list, title in [
                ("CRITICAL", critical_issues, "🔴 Critical Issues"),
                ("HIGH", high_issues, "🟠 High Priority Issues"),
                ("MEDIUM", medium_issues, "🟡 Medium Priority Issues")
            ]:
                if issues_list:
                    html_content += f"""
                    <div class="severity-section">
                        <h3 class="severity-title severity-{severity.lower()}">{title}</h3>
                    """
                    
                    for idx, issue in enumerate(issues_list, 1):
                        finding = issue.get("finding", "No description")
                        line = issue.get("line_number", "N/A")
                        context = issue.get("function_context", "")
                        
                        html_content += f"""
                        <div class="issue-item">
                            <div class="issue-title">{finding[:100]}{'...' if len(finding) > 100 else ''}</div>
                            <div class="issue-location">
                                <span class="line-badge">Line {line}</span>
                                {f'Context: {context}' if context else 'No context available'}
                            </div>
                            <div class="code-diff">
                                <div class="diff-header" onclick="toggleDiff('diff-{count}-{severity}-{idx}')">
                                    ▶ View Code Context & Suggestions
                                </div>
                                <div class="diff-body" id="diff-{count}-{severity}-{idx}">
                                    <div class="current-code"># Current code at line {line}:<br/># {finding}</div>
                                    <div class="optimized-code"># Suggested improvement:<br/># Apply the recommended changes above</div>
                                </div>
                            </div>
                        </div>
                        """
                    
                    html_content += "</div>"

        html_content += "</div>"

    html_content += """
    </div>
    <script>
        function toggleFile(fileId) {
            const details = document.getElementById(fileId);
            const icon = document.getElementById('expand-' + fileId);
            if (details.classList.contains('expanded')) {
                details.classList.remove('expanded');
                icon.classList.remove('expanded');
                icon.textContent = '▶';
            } else {
                details.classList.add('expanded');
                icon.classList.add('expanded');
                icon.textContent = '▼';
            }
        }
        
        function toggleDiff(diffId) {
            const diffBody = document.getElementById(diffId);
            if (diffBody.classList.contains('expanded')) {
                diffBody.classList.remove('expanded');
            } else {
                diffBody.classList.add('expanded');
            }
        }
        
        // Auto-expand first file on page load
        document.addEventListener('DOMContentLoaded', function() {
            toggleFile('file1');
        });
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

        # Enhanced Markdown for PR with colors and expandable sections
        formatted_review = format_for_pr_display(filtered)

        # Enhanced Interactive HTML with better styling
        html_report = generate_interactive_html_report(filtered, original_findings)
        with open("dbt_code_review_report.html","w") as f: f.write(html_report)

        # JSON output
        output_data = {
            "full_review": formatted_review,              
            "full_review_markdown": formatted_review,     
            "full_review_json": filtered,                 
            "criticals": criticals,
            "file": FILE_TO_REVIEW,
            "interactive_report_path": "dbt_code_review_report.html"
        }
        with open("review_output.json","w") as f: json.dump(output_data, f, indent=2)

        print("✅ Enhanced Markdown review saved for PR (with colors and expandable sections)")
        print("✅ Enhanced Interactive HTML report saved to dbt_code_review_report.html")
        print(f"📊 Found {len(criticals)} critical issues")
    finally:
        session.close()
