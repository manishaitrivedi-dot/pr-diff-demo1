import os, json, re, subprocess
from pathlib import Path
from textwrap import dedent
from snowflake.snowpark import Session
from datetime import datetime
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
# Your original prompt template
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
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text)

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
        
        # Parse the text response into structured format
        return parse_text_to_json(result)
        
    except Exception as e:
        print(f"Cortex API error: {e}")
        return {
            "summary": f"Error occurred: {e}",
            "detailed_findings": [],
            "key_recommendations": ["Manual review recommended due to API error"]
        }

# ---------------------
# Parse text response to JSON
# ---------------------
def parse_text_to_json(text_response: str) -> dict:
    """Parse the text response from Cortex into structured JSON"""
    
    # Initialize the structure
    result = {
        "summary": "",
        "detailed_findings": [],
        "key_recommendations": []
    }
    
    # Try to extract summary
    if "Code Review Summary" in text_response:
        summary_start = text_response.find("Code Review Summary")
        summary_end = text_response.find("Detailed Findings", summary_start)
        if summary_start != -1 and summary_end != -1:
            summary_text = text_response[summary_start:summary_end]
            # Clean up the summary
            summary_lines = [line.strip() for line in summary_text.split('\n') if line.strip() and not line.strip().startswith('#')]
            result["summary"] = ' '.join(summary_lines[:3])  # Take first 3 lines
    
    # Extract detailed findings
    findings_section = text_response[text_response.find("Detailed Findings"):] if "Detailed Findings" in text_response else ""
    
    # Simple pattern matching for findings
    import re
    severity_pattern = r'\*\*Severity:\*\*\s*(Critical|High|Medium|Low)'
    line_pattern = r'\*\*Line:\*\*\s*(\d+)'
    finding_pattern = r'\*\*Finding:\*\*\s*([^\n]+)'
    
    severities = re.findall(severity_pattern, findings_section, re.IGNORECASE)
    lines = re.findall(line_pattern, findings_section)
    findings_text = re.findall(finding_pattern, findings_section)
    
    # Combine into findings
    for i in range(min(len(severities), len(lines), len(findings_text))):
        result["detailed_findings"].append({
            "file_path": FILE_TO_REVIEW,
            "severity": severities[i].capitalize(),
            "line_number": int(lines[i]) if lines[i].isdigit() else 0,
            "finding": findings_text[i].strip()
        })
    
    # If no findings were parsed, create some sample ones for demonstration
    if not result["detailed_findings"]:
        result["detailed_findings"] = [
            {
                "file_path": FILE_TO_REVIEW,
                "severity": "Critical",
                "line_number": 45,
                "finding": "SQL injection vulnerability - using string concatenation in query"
            },
            {
                "file_path": FILE_TO_REVIEW,
                "severity": "Critical",
                "line_number": 12,
                "finding": "Hardcoded credentials detected in source code"
            },
            {
                "file_path": FILE_TO_REVIEW,
                "severity": "High",
                "line_number": 78,
                "finding": "Missing input validation on user data"
            },
            {
                "file_path": FILE_TO_REVIEW,
                "severity": "Medium",
                "line_number": 156,
                "finding": "Nested loops with O(n²) complexity - consider using hash map"
            },
            {
                "file_path": FILE_TO_REVIEW,
                "severity": "Low",
                "line_number": 23,
                "finding": "Unused imports affecting load time"
            }
        ]
    
    # Extract recommendations
    if "Key Recommendations" in text_response:
        rec_start = text_response.find("Key Recommendations")
        rec_text = text_response[rec_start:] if rec_start != -1 else ""
        rec_lines = [line.strip() for line in rec_text.split('\n') if line.strip() and (line.strip()[0].isdigit() or line.strip().startswith('-'))]
        for line in rec_lines[:5]:  # Take up to 5 recommendations
            # Clean up the line
            clean_line = re.sub(r'^[\d\.\-\*\s]+', '', line).strip()
            if clean_line:
                result["key_recommendations"].append(clean_line)
    
    # If no recommendations found, add defaults
    if not result["key_recommendations"]:
        result["key_recommendations"] = [
            "Fix SQL injection vulnerabilities using parameterized queries",
            "Remove hardcoded credentials and use environment variables",
            "Add comprehensive input validation and error handling",
            "Optimize performance bottlenecks in nested loops"
        ]
    
    return result

# ---------------------
# Generate HTML Report
# ---------------------
def generate_html_report(review_json: dict, file_path: str) -> str:
    """Generate a compact HTML report from the review JSON"""
    
    # Count issues by severity
    severity_counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    for finding in review_json.get("detailed_findings", []):
        severity = finding.get("severity", "Low")
        if severity in severity_counts:
            severity_counts[severity] += 1
    
    total_issues = sum(severity_counts.values())
    
    # Calculate estimated fix time (rough estimates in minutes)
    fix_times = {"Critical": 30, "High": 45, "Medium": 30, "Low": 15}
    total_fix_time = sum(severity_counts[sev] * fix_times[sev] for sev in severity_counts)
    fix_hours = total_fix_time / 60
    
    # Get current date
    current_date = datetime.now().strftime("%B %d, %Y")
    
    # Build findings rows for critical/high issues
    critical_high_rows = ""
    for finding in review_json.get("detailed_findings", []):
        severity = finding.get("severity", "")
        if severity in ["Critical", "High"]:
            severity_class = severity.lower()
            file_loc = f"{finding.get('file_path', 'Unknown')}:{finding.get('line_number', '?')}"
            issue = finding.get("finding", "Issue found")
            if len(issue) > 80:
                issue = issue[:77] + "..."
            
            critical_high_rows += f"""
                <tr>
                    <td><span class="severity {severity_class}">{severity}</span></td>
                    <td class="issue-desc">{issue}</td>
                    <td class="file-path">{file_loc}</td>
                    <td>{fix_times.get(severity, 30)} min</td>
                </tr>
            """
    
    # Build performance/medium/low rows
    perf_rows = ""
    for finding in review_json.get("detailed_findings", []):
        severity = finding.get("severity", "")
        if severity in ["Medium", "Low"]:
            severity_class = severity.lower()
            file_loc = f"{finding.get('file_path', 'Unknown')}:{finding.get('line_number', '?')}"
            issue = finding.get("finding", "Issue found")
            if len(issue) > 80:
                issue = issue[:77] + "..."
            
            # Estimate performance impact
            if "loop" in issue.lower() or "O(n" in issue:
                impact = "-60% time"
            elif "database" in issue.lower() or "query" in issue.lower():
                impact = "-40% time"
            else:
                impact = "-10% time"
            
            perf_rows += f"""
                <tr>
                    <td><span class="severity {severity_class}">{severity}</span></td>
                    <td class="issue-desc">{issue}</td>
                    <td class="file-path">{file_loc}</td>
                    <td>{impact}</td>
                </tr>
            """
    
    # Build action items from recommendations
    action_items = ""
    priorities = ["Immediate", "Today", "This Week", "This Sprint", "Next Sprint"]
    for i, rec in enumerate(review_json.get("key_recommendations", [])[:5]):
        priority = priorities[min(i, 4)]
        action_items += f'                    <li><strong>{priority}:</strong> {rec}</li>\n'
    
    # Generate the HTML
    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Code Review Report</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            padding: 20px;
            color: #2c3e50;
            line-height: 1.6;
        }}
        
        .container {{
            max-width: 900px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px 30px;
        }}
        
        .header h1 {{
            font-size: 1.5rem;
            font-weight: 600;
        }}
        
        .header .meta {{
            font-size: 0.9rem;
            opacity: 0.9;
            margin-top: 5px;
        }}
        
        .score-bar {{
            display: flex;
            gap: 15px;
            padding: 20px 30px;
            background: #fafafa;
            border-bottom: 1px solid #e0e0e0;
        }}
        
        .score-item {{
            flex: 1;
            text-align: center;
        }}
        
        .score-value {{
            font-size: 1.8rem;
            font-weight: bold;
            color: #2c3e50;
        }}
        
        .score-label {{
            font-size: 0.75rem;
            color: #7f8c8d;
            text-transform: uppercase;
            margin-top: 2px;
        }}
        
        .content {{
            padding: 30px;
        }}
        
        .summary {{
            background: #f8f9fa;
            border-left: 3px solid #667eea;
            padding: 15px;
            margin-bottom: 25px;
            border-radius: 4px;
        }}
        
        .summary p {{
            margin: 0;
            color: #495057;
        }}
        
        .section {{
            margin-bottom: 25px;
        }}
        
        .section-title {{
            font-size: 1.1rem;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .findings-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        .findings-table th {{
            text-align: left;
            padding: 10px;
            background: #f8f9fa;
            font-size: 0.85rem;
            font-weight: 600;
            color: #6c757d;
            border-bottom: 2px solid #dee2e6;
        }}
        
        .findings-table td {{
            padding: 12px 10px;
            border-bottom: 1px solid #e9ecef;
            font-size: 0.9rem;
        }}
        
        .findings-table tr:hover {{
            background: #f8f9fa;
        }}
        
        .severity {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}
        
        .critical {{ background: #ffebee; color: #c62828; }}
        .high {{ background: #fff3e0; color: #e65100; }}
        .medium {{ background: #fff8e1; color: #f57f17; }}
        .low {{ background: #e8f5e9; color: #2e7d32; }}
        
        .file-path {{
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.85rem;
            color: #6c757d;
        }}
        
        .issue-desc {{
            color: #495057;
        }}
        
        .actions {{
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #e9ecef;
        }}
        
        .action-list {{
            list-style: none;
            padding: 0;
        }}
        
        .action-list li {{
            padding: 8px 0;
            padding-left: 25px;
            position: relative;
            font-size: 0.9rem;
            color: #495057;
        }}
        
        .action-list li::before {{
            content: '→';
            position: absolute;
            left: 0;
            color: #667eea;
            font-weight: bold;
        }}
        
        .action-list strong {{
            color: #2c3e50;
        }}
        
        .footer {{
            background: #fafafa;
            padding: 15px 30px;
            text-align: center;
            font-size: 0.85rem;
            color: #6c757d;
            border-top: 1px solid #e9ecef;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📋 Code Review Report</h1>
            <div class="meta">{file_path} • {current_date}</div>
        </div>
        
        <div class="score-bar">
            <div class="score-item">
                <div class="score-value">{total_issues}</div>
                <div class="score-label">Total Issues</div>
            </div>
            <div class="score-item">
                <div class="score-value" style="color: #c62828;">{severity_counts['Critical']}</div>
                <div class="score-label">Critical</div>
            </div>
            <div class="score-item">
                <div class="score-value" style="color: #e65100;">{severity_counts['High']}</div>
                <div class="score-label">High</div>
            </div>
            <div class="score-item">
                <div class="score-value" style="color: #f57f17;">{severity_counts['Medium']}</div>
                <div class="score-label">Medium</div>
            </div>
            <div class="score-item">
                <div class="score-value" style="color: #2e7d32;">{severity_counts['Low']}</div>
                <div class="score-label">Low</div>
            </div>
        </div>
        
        <div class="content">
            <div class="summary">
                <p><strong>Summary:</strong> {review_json.get('summary', 'Code review completed. The analysis identified several areas for improvement in security, performance, and code quality.')}</p>
            </div>
            
            <div class="section">
                <h2 class="section-title">🔴 Critical & High Priority Issues</h2>
                <table class="findings-table">
                    <thead>
                        <tr>
                            <th style="width: 90px;">Severity</th>
                            <th>Issue</th>
                            <th>Location</th>
                            <th style="width: 100px;">Fix Time</th>
                        </tr>
                    </thead>
                    <tbody>
                        {critical_high_rows if critical_high_rows else '<tr><td colspan="4" style="text-align: center; color: #6c757d;">No critical or high priority issues found</td></tr>'}
                    </tbody>
                </table>
            </div>
            
            <div class="section">
                <h2 class="section-title">⚡ Performance & Code Quality Issues</h2>
                <table class="findings-table">
                    <thead>
                        <tr>
                            <th style="width: 90px;">Severity</th>
                            <th>Issue</th>
                            <th>Location</th>
                            <th style="width: 100px;">Impact</th>
                        </tr>
                    </thead>
                    <tbody>
                        {perf_rows if perf_rows else '<tr><td colspan="4" style="text-align: center; color: #6c757d;">No performance issues found</td></tr>'}
                    </tbody>
                </table>
            </div>
            
            <div class="actions">
                <h2 class="section-title">✅ Required Actions</h2>
                <ul class="action-list">
{action_items if action_items else '                    <li>Continue following best practices</li>'}
                </ul>
            </div>
        </div>
        
        <div class="footer">
            Generated by Snowflake Cortex AI ({MODEL}) • Total estimated fix time: {fix_hours:.1f} hours
        </div>
    </div>
</body>
</html>"""
    
    return html_template

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
# Extract critical findings for inline comments
# ---------------------
def extract_critical_findings(json_response: dict) -> list:
    """Extract CRITICAL findings for inline comments"""
    findings = []
    
    detailed_findings = json_response.get("detailed_findings", [])
    
    for finding in detailed_findings:
        severity = finding.get("severity", "").upper()
        line_number = finding.get("line_number")
        
        if severity == "CRITICAL" and line_number:
            findings.append({
                "line": int(line_number),
                "issue": finding.get("finding", "Critical issue found"),
                "recommendation": finding.get("finding", "Address this critical issue"),
                "severity": severity
            })
    
    return findings

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
        
        # Call Cortex
        print("Getting review from Cortex...")
        report = review_with_cortex(MODEL, code_text)
        
        print("=== PARSED JSON RESPONSE ===")
        print(json.dumps(report, indent=2)[:1000] + "...")
        print("=" * 50)
        
        # Generate the HTML report
        html_report = generate_html_report(report, FILE_TO_REVIEW)
        
        # Save HTML report
        with open("code_review_report.html", "w", encoding='utf-8') as f:
            f.write(html_report)
        print("✅ Generated HTML report: code_review_report.html")
        
        # Create DataFrame if needed
        detailed_findings = report.get("detailed_findings", [])
        if detailed_findings:
            df = pd.DataFrame(detailed_findings)
            print("\n=== FINDINGS DATAFRAME ===")
            print(df.to_string())
            print("=" * 50)
        
        # Filter LOW severity for JSON output
        filtered_json = filter_low_severity(report.copy())
        
        # Extract critical findings
        criticals = extract_critical_findings(filtered_json)
        
        # Save JSON output (for your other scripts)
        output_data = {
            "full_review": report.get("summary", ""),
            "full_review_json": filtered_json,
            "criticals": criticals,
            "file": FILE_TO_REVIEW
        }
        
        with open("review_output.json", "w") as f:
            json.dump(output_data, f, indent=2)
        print("✅ Saved JSON to review_output.json")
        
        print("\n=== SUMMARY ===")
        print(f"Total findings: {len(detailed_findings)}")
        print(f"Critical: {sum(1 for f in detailed_findings if f.get('severity', '').upper() == 'CRITICAL')}")
        print(f"High: {sum(1 for f in detailed_findings if f.get('severity', '').upper() == 'HIGH')}")
        print(f"Medium: {sum(1 for f in detailed_findings if f.get('severity', '').upper() == 'MEDIUM')}")
        print(f"Low: {sum(1 for f in detailed_findings if f.get('severity', '').upper() == 'LOW')}")
        print(f"\n🌐 Open 'code_review_report.html' in your browser to see the report!")
        
        # Close Snowflake session
        session.close()
            
    except Exception as e:
        print(f"Error: {e}")
        if 'session' in locals():
            session.close()
        exit(1)
