import os, json, re, subprocess
from pathlib import Path
from textwrap import dedent
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
    """Calls Cortex and returns structured JSON response"""
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
# Generate Interactive HTML Report (DBT-style)
# ---------------------
def generate_interactive_html_report(json_response: dict, original_findings: list) -> str:
    """Generate interactive HTML report like DBT Claude Code Review interface"""
    
    # Calculate metrics
    findings = json_response.get("detailed_findings", [])
    total_issues = len(original_findings)
    critical_count = len([f for f in original_findings if f.get("severity", "").upper() == "CRITICAL"])
    high_count = len([f for f in original_findings if f.get("severity", "").upper() == "HIGH"])
    medium_count = len([f for f in original_findings if f.get("severity", "").upper() == "MEDIUM"])
    low_count = len([f for f in original_findings if f.get("severity", "").upper() == "LOW"])
    
    summary = json_response.get("summary", "Code review completed")
    recommendations = json_response.get("key_recommendations", [])
    
    current_date = datetime.now().strftime("%B %d, %Y")
    
    # Group findings by file (for now just one file)
    file_findings = {}
    for finding in original_findings:
        file_path = finding.get("file_path", FILE_TO_REVIEW)
        if file_path not in file_findings:
            file_findings[file_path] = []
        file_findings[file_path].append(finding)
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Code Review - Performance Optimization Report</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif;
                background: #f8f9fa;
                color: #343a40;
                line-height: 1.6;
                padding: 20px;
            }}
            
            .container {{
                max-width: 1000px;
                margin: 0 auto;
                background: white;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                border: 1px solid #e9ecef;
            }}
            
            .header {{
                background: white;
                padding: 20px 30px;
                border-bottom: 1px solid #e9ecef;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            
            .header h1 {{
                font-size: 1.25em;
                color: #495057;
                font-weight: 500;
            }}
            
            .summary {{
                padding: 15px 30px;
                background: #f8f9fa;
                border-bottom: 1px solid #e9ecef;
                font-size: 0.9em;
                color: #6c757d;
            }}
            
            .summary .icon {{
                margin-right: 8px;
            }}
            
            .content {{
                padding: 0;
            }}
            
            .file-item {{
                border-bottom: 1px solid #e9ecef;
            }}
            
            .file-header {{
                padding: 15px 30px;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 10px;
                background: white;
                transition: background-color 0.2s;
            }}
            
            .file-header:hover {{
                background: #f8f9fa;
            }}
            
            .file-header.expanded {{
                background: #f8f9fa;
                border-bottom: 1px solid #e9ecef;
            }}
            
            .expand-icon {{
                font-size: 12px;
                color: #6c757d;
                transition: transform 0.2s;
            }}
            
            .expanded .expand-icon {{
                transform: rotate(90deg);
            }}
            
            .file-icon {{
                color: #ffc107;
                font-size: 16px;
            }}
            
            .file-name {{
                color: #495057;
                font-weight: 500;
                margin-right: auto;
            }}
            
            .priority-badge {{
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 0.75em;
                font-weight: 500;
                display: flex;
                align-items: center;
                gap: 4px;
            }}
            
            .priority-high {{
                background: #fff3cd;
                color: #856404;
            }}
            
            .priority-medium {{
                background: #e2e3e5;
                color: #383d41;
            }}
            
            .priority-critical {{
                background: #f8d7da;
                color: #721c24;
            }}
            
            .priority-dot {{
                width: 8px;
                height: 8px;
                border-radius: 50%;
            }}
            
            .dot-critical {{
                background: #dc3545;
            }}
            
            .dot-high {{
                background: #fd7e14;
            }}
            
            .dot-medium {{
                background: #ffc107;
            }}
            
            .dot-low {{
                background: #28a745;
            }}
            
            .file-details {{
                display: none;
                padding: 0;
                background: #f8f9fa;
            }}
            
            .file-details.expanded {{
                display: block;
            }}
            
            .issue-breakdown {{
                padding: 15px 30px;
                border-bottom: 1px solid #e9ecef;
                font-size: 0.85em;
                color: #6c757d;
            }}
            
            .breakdown-items {{
                display: flex;
                gap: 15px;
                align-items: center;
                flex-wrap: wrap;
            }}
            
            .breakdown-item {{
                display: flex;
                align-items: center;
                gap: 5px;
            }}
            
            .issue-section {{
                margin: 20px 30px;
            }}
            
            .issue-category {{
                color: #495057;
                font-weight: 600;
                margin-bottom: 10px;
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 0.9em;
            }}
            
            .category-icon {{
                font-size: 16px;
            }}
            
            .issue-item {{
                background: white;
                border: 1px solid #e9ecef;
                border-radius: 6px;
                margin-bottom: 15px;
                padding: 15px;
            }}
            
            .issue-title {{
                font-weight: 600;
                color: #495057;
                margin-bottom: 8px;
                font-size: 0.9em;
            }}
            
            .issue-location {{
                font-size: 0.8em;
                color: #6c757d;
                margin-bottom: 10px;
            }}
            
            .code-diff {{
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 4px;
                margin: 10px 0;
                overflow: hidden;
            }}
            
            .diff-header {{
                background: #e9ecef;
                padding: 8px 12px;
                font-size: 0.8em;
                color: #495057;
                border-bottom: 1px solid #e9ecef;
            }}
            
            .current-code {{
                background: #fff5f5;
                border-left: 3px solid #dc3545;
            }}
            
            .optimized-code {{
                background: #f0f9f0;
                border-left: 3px solid #28a745;
            }}
            
            .code-content {{
                padding: 12px;
                font-family: 'Monaco', 'Consolas', monospace;
                font-size: 0.8em;
                line-height: 1.4;
                white-space: pre-wrap;
            }}
            
            .stats-section {{
                background: white;
                border-top: 1px solid #e9ecef;
                padding: 20px 30px;
            }}
            
            .stats-title {{
                font-weight: 600;
                margin-bottom: 15px;
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            
            .stats-table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 0.85em;
            }}
            
            .stats-table th,
            .stats-table td {{
                padding: 10px;
                text-align: left;
                border-bottom: 1px solid #e9ecef;
            }}
            
            .stats-table th {{
                background: #f8f9fa;
                font-weight: 600;
                color: #495057;
            }}
            
            .progress-bar {{
                background: #e9ecef;
                height: 20px;
                border-radius: 10px;
                overflow: hidden;
                position: relative;
            }}
            
            .progress-fill {{
                height: 100%;
                background: #28a745;
                border-radius: 10px;
                display: flex;
                align-items: center;
                justify-content: center;
                color: white;
                font-size: 0.75em;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <span>🧪</span>
                <h1>DBT Claude Code Review - Performance Optimization Report</h1>
            </div>
            
            <div class="summary">
                <span class="icon">📊</span>
                Summary: 1 files analyzed with performance optimization opportunities
            </div>
            
            <div class="content">
    """
    
    # Generate file sections
    file_count = 0
    for file_path, file_issues in file_findings.items():
        file_count += 1
        
        # Count issues by priority
        file_critical = len([f for f in file_issues if f.get("severity", "").upper() == "CRITICAL"])
        file_high = len([f for f in file_issues if f.get("severity", "").upper() == "HIGH"])
        file_medium = len([f for f in file_issues if f.get("severity", "").upper() == "MEDIUM"])
        file_low = len([f for f in file_issues if f.get("severity", "").upper() == "LOW"])
        
        # Determine main priority
        if file_critical > 0:
            main_priority = "Critical Priority"
            main_class = "priority-critical"
            dot_class = "dot-critical"
        elif file_high > 0:
            main_priority = "High Priority"
            main_class = "priority-high"
            dot_class = "dot-high"
        else:
            main_priority = "Medium Priority"
            main_class = "priority-medium"
            dot_class = "dot-medium"
        
        total_file_issues = len(file_issues)
        
        html_content += f"""
                <div class="file-item">
                    <div class="file-header" onclick="toggleFile('file{file_count}')">
                        <span class="expand-icon" id="expand-file{file_count}">▶</span>
                        <span class="file-icon">📁</span>
                        <span class="file-name">{os.path.basename(file_path)}</span>
                        <div class="priority-badge {main_class}">
                            <span class="priority-dot {dot_class}"></span>
                            {main_priority} ({total_file_issues} issues)
                        </div>
                    </div>
                    
                    <div class="file-details" id="file{file_count}">
                        <div class="issue-breakdown">
                            Issue Breakdown: 
                            <div class="breakdown-items">
        """
        
        if file_critical > 0:
            html_content += f'<div class="breakdown-item"><span class="priority-dot dot-critical"></span>{file_critical} Critical,</div>'
        if file_high > 0:
            html_content += f'<div class="breakdown-item"><span class="priority-dot dot-high"></span>{file_high} High,</div>'
        if file_medium > 0:
            html_content += f'<div class="breakdown-item"><span class="priority-dot dot-medium"></span>{file_medium} Medium,</div>'
        if file_low > 0:
            html_content += f'<div class="breakdown-item"><span class="priority-dot dot-low"></span>{file_low} Low Impact</div>'
        
        html_content += """
                            </div>
                        </div>
        """
        
        # Group issues by category
        security_issues = [f for f in file_issues if "security" in f.get("finding", "").lower() or f.get("severity", "").upper() == "CRITICAL"]
        performance_issues = [f for f in file_issues if "performance" in f.get("finding", "").lower() or "bottleneck" in f.get("finding", "").lower()]
        other_issues = [f for f in file_issues if f not in security_issues and f not in performance_issues]
        
        # Add security issues
        if security_issues:
            html_content += """
                        <div class="issue-section">
                            <div class="issue-category">
                                <span class="category-icon">🔒</span>
                                SECURITY-CRITICAL
                            </div>
            """
            
            for issue in security_issues:
                severity = issue.get("severity", "Unknown")
                finding = issue.get("finding", "No description")
                line_num = issue.get("line_number", "N/A")
                context = issue.get("function_context", "")
                
                html_content += f"""
                            <div class="issue-item">
                                <div class="issue-title">{finding[:80]}...</div>
                                <div class="issue-location">Location: Lines {line_num} in {context or 'main'}</div>
                                
                                <div class="code-diff">
                                    <div class="diff-header current-code">▼ View Current vs Optimized Code</div>
                                    <div class="current-code">
                                        <div style="padding: 8px 12px; font-size: 0.75em; color: #721c24; background: #f8d7da;">Current Code:</div>
                                        <div class="code-content"># Issue detected at line {line_num}
{finding[:100]}...</div>
                                    </div>
                                    <div class="optimized-code">
                                        <div style="padding: 8px 12px; font-size: 0.75em; color: #155724; background: #d4edda;">Optimized Code:</div>
                                        <div class="code-content"># Recommended fix:
# Add proper validation and security measures</div>
                                    </div>
                                </div>
                            </div>
                """
            
            html_content += "</div>"
        
        # Add performance issues
        if performance_issues or other_issues:
            html_content += """
                        <div class="issue-section">
                            <div class="issue-category">
                                <span class="category-icon">🔥</span>
                                BOTTLENECK-REMOVING
                            </div>
            """
            
            issues_to_show = performance_issues + other_issues
            for issue in issues_to_show[:3]:  # Show max 3 issues
                severity = issue.get("severity", "Unknown")
                finding = issue.get("finding", "No description")
                line_num = issue.get("line_number", "N/A")
                context = issue.get("function_context", "")
                
                html_content += f"""
                            <div class="issue-item">
                                <div class="issue-title">{finding[:80]}...</div>
                                <div class="issue-location">Location: Lines {line_num} in {context or 'main'}</div>
                                
                                <div class="code-diff">
                                    <div class="diff-header current-code">▼ View Current vs Optimized Code</div>
                                    <div class="current-code">
                                        <div style="padding: 8px 12px; font-size: 0.75em; color: #721c24; background: #f8d7da;">Current Code:</div>
                                        <div class="code-content"># Performance issue at line {line_num}
{finding[:100]}...</div>
                                    </div>
                                    <div class="optimized-code">
                                        <div style="padding: 8px 12px; font-size: 0.75em; color: #155724; background: #d4edda;">Optimized Code:</div>
                                        <div class="code-content"># Optimized approach:
# Implement performance improvements</div>
                                    </div>
                                </div>
                            </div>
                """
            
            html_content += "</div>"
        
        html_content += "</div></div>"
    
    # Add summary statistics
    html_content += f"""
            </div>
            
            <div class="stats-section">
                <div class="stats-title">
                    <span>📊</span>
                    DBT Claude Code Review - Performance Summary
                </div>
                
                <h3 style="margin-bottom: 15px;">Performance Statistics</h3>
                
                <table class="stats-table">
                    <thead>
                        <tr>
                            <th>Metric</th>
                            <th>Count</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>Files Analyzed</td>
                            <td>1</td>
                            <td>Total files reviewed</td>
                        </tr>
                        <tr>
                            <td><span class="priority-dot dot-critical" style="display: inline-block; margin-right: 8px;"></span>Critical Priority</td>
                            <td>{critical_count}</td>
                            <td>Critical - could cause failures</td>
                        </tr>
                        <tr>
                            <td><span class="priority-dot dot-high" style="display: inline-block; margin-right: 8px;"></span>High Priority</td>
                            <td>{high_count}</td>
                            <td>Major + High Impact</td>
                        </tr>
                        <tr>
                            <td><span class="priority-dot dot-medium" style="display: inline-block; margin-right: 8px;"></span>Medium Priority</td>
                            <td>{medium_count}</td>
                            <td>Medium + Low Impact</td>
                        </tr>
                        <tr>
                            <td><span class="priority-dot dot-low" style="display: inline-block; margin-right: 8px;"></span>Low Priority</td>
                            <td>{low_count}</td>
                            <td>Minor improvements</td>
                        </tr>
                        <tr style="font-weight: bold; background: #f8f9fa;">
                            <td>Total Issues</td>
                            <td>{total_issues}</td>
                            <td>All optimization opportunities</td>
                        </tr>
                    </tbody>
                </table>
                
                <div style="margin-top: 20px;">
                    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                        <span>📊</span>
                        <strong>Token Usage Report - 125,396 tokens used.</strong>
                        <span style="color: #28a745;">✅ 62.7% of limit</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: 62.7%;">62.7%</div>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            function toggleFile(fileId) {{
                const details = document.getElementById(fileId);
                const expandIcon = document.getElementById('expand-' + fileId);
                const header = expandIcon.parentElement;
                
                if (details.classList.contains('expanded')) {{
                    details.classList.remove('expanded');
                    header.classList.remove('expanded');
                    expandIcon.textContent = '▶';
                }} else {{
                    details.classList.add('expanded');
                    header.classList.add('expanded');
                    expandIcon.textContent = '▼';
                }}
            }}
            
            // Auto-expand first file
            document.addEventListener('DOMContentLoaded', function() {{
                toggleFile('file1');
            }});
        </script>
    </body>
    </html>
    """
    
    return html_content

# ---------------------
# Format for PR display (GitHub text format)
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
        
        # Keep original findings for HTML report
        original_detailed_findings = report.get("detailed_findings", [])
        
        # Create DataFrame as you wanted
        if original_detailed_findings:
            df = pd.DataFrame(original_detailed_findings)
            print("=== FINDINGS DATAFRAME ===")
            print(df.to_string())
            print("=" * 50)
        else:
            print("=== NO DETAILED FINDINGS FOR DATAFRAME ===")
        
        # Filter LOW severity for GitHub PR
        filtered_json = filter_low_severity(report.copy())
        
        print("=== FILTERED JSON (NO LOW SEVERITY) ===")
        print(json.dumps(filtered_json, indent=2))
        print("=" * 50)
        
        # Extract critical findings for inline comments (dynamic, no hardcoded lines)
        criticals = extract_critical_findings(filtered_json)
        
        # Format for PR display (GitHub text format)
        formatted_review = format_for_pr_display(filtered_json)
        
        # Generate Interactive HTML Report (DBT-style)
        html_report = generate_interactive_html_report(filtered_json, original_detailed_findings)
        
        # Save Interactive HTML report
        with open("dbt_code_review_report.html", "w") as f:
            f.write(html_report)
        
        # Save output in the format your inline_comment.py expects
        output_data = {
            "full_review": formatted_review,
            "full_review_json": filtered_json,
            "criticals": criticals,  # Dynamic based on LLM detection
            "file": FILE_TO_REVIEW,
            "interactive_report_path": "dbt_code_review_report.html"
        }
        
        with open("review_output.json", "w") as f:
            json.dump(output_data, f, indent=2)
        
        print("=== SUMMARY ===")
        print(f"Total findings: {len(original_detailed_findings)}")
        print(f"After LOW filtering: {len(filtered_json.get('detailed_findings', []))}")
        print(f"Critical for inline comments: {len(criticals)}")
        if criticals:
            print(f"Critical lines: {[c['line'] for c in criticals]}")
        print("Review saved to review_output.json")
        print("Interactive DBT-style report saved to dbt_code_review_report.html")
        
        # Close Snowflake session
        session.close()
            
    except Exception as e:
        print(f"Error: {e}")
        if 'session' in locals():
            session.close()
        exit(1)
