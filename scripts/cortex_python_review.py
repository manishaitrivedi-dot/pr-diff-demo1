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
# Enhanced Prompt template for better JSON response
# ---------------------
PROMPT_TEMPLATE = """You are a principal-level Python code reviewer. Please analyze the following Python code and provide a detailed review.

IMPORTANT: Respond ONLY with valid JSON in this exact format:
{
    "summary": "Brief summary of code quality and main issues",
    "detailed_findings": [
        {
            "severity": "CRITICAL|HIGH|MEDIUM|LOW",
            "line_number": "actual_line_number",
            "function_context": "function_name_if_applicable",
            "finding": "Detailed description of the issue",
            "recommendation": "Specific recommendation to fix"
        }
    ],
    "key_recommendations": [
        "Recommendation 1",
        "Recommendation 2"
    ]
}

Code to review:
```python
{PY_CONTENT}
```

Rules for severity:
- CRITICAL: Security vulnerabilities, data loss risks, crashes
- HIGH: Performance issues, incorrect logic, bad practices
- MEDIUM: Code style, maintainability, minor inefficiencies  
- LOW: Minor style issues, documentation

Respond with valid JSON only."""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text)

# ---------------------
# Enhanced Cortex call with better error handling
# ---------------------
def review_with_cortex(model: str, code_text: str) -> dict:
    prompt = build_prompt(code_text)
    # Better escaping for SQL
    clean_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{clean_prompt}'
        ) as response
    """
    
    try:
        print(f"Getting review from {model}...")
        df = session.sql(query)
        result = df.collect()[0][0]
        
        print(f"Raw LLM Response: {result[:500]}...")  # Debug output
        
        # Try to extract JSON from the response
        try:
            # First try direct JSON parse
            return json.loads(result)
        except json.JSONDecodeError:
            # Try to find JSON in the response
            json_match = re.search(r'\{.*\}', result, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except json.JSONDecodeError:
                    pass
            
            # If JSON parsing fails, create structured response from text
            print("Warning: Could not parse JSON response, creating structured response from text")
            return parse_text_response(result, code_text)
            
    except Exception as e:
        print(f"Error calling Cortex: {e}")
        return create_fallback_response(code_text, str(e))

# ---------------------
# Parse unstructured text response
# ---------------------
def parse_text_response(response_text: str, code_text: str) -> dict:
    """Convert unstructured text response to structured JSON"""
    lines = code_text.split('\n')
    
    # Create some basic findings based on code analysis
    findings = []
    
    # Check for common issues
    for i, line in enumerate(lines, 1):
        line_stripped = line.strip()
        
        # Check for potential issues
        if 'password' in line_stripped.lower() and '=' in line_stripped:
            findings.append({
                "severity": "CRITICAL",
                "line_number": str(i),
                "function_context": "main",
                "finding": "Hardcoded password found in source code",
                "recommendation": "Move sensitive credentials to environment variables or secure configuration"
            })
        
        if 'print(' in line_stripped and ('password' in line_stripped.lower() or 'secret' in line_stripped.lower()):
            findings.append({
                "severity": "HIGH",
                "line_number": str(i),
                "function_context": "main", 
                "finding": "Potential credential logging detected",
                "recommendation": "Remove or mask sensitive information in print statements"
            })
        
        if 'except:' in line_stripped or 'except Exception:' in line_stripped:
            findings.append({
                "severity": "MEDIUM",
                "line_number": str(i),
                "function_context": "error_handling",
                "finding": "Generic exception handling detected",
                "recommendation": "Use specific exception types for better error handling"
            })
    
    # If no findings, add some general ones
    if not findings:
        findings.append({
            "severity": "MEDIUM",
            "line_number": "1",
            "function_context": "general",
            "finding": "Code review completed - consider adding type hints and docstrings",
            "recommendation": "Add type hints and comprehensive docstrings for better maintainability"
        })
    
    return {
        "summary": f"Analyzed {len(lines)} lines of code. Found {len(findings)} potential issues.",
        "detailed_findings": findings,
        "key_recommendations": [
            "Review security practices for credential management",
            "Improve error handling specificity", 
            "Add comprehensive documentation"
        ],
        "raw_text": response_text
    }

# ---------------------
# Fallback response for errors
# ---------------------
def create_fallback_response(code_text: str, error_msg: str) -> dict:
    """Create a basic response when LLM fails"""
    lines = code_text.split('\n')
    
    return {
        "summary": f"Code analysis completed with {len(lines)} lines. LLM service encountered an issue.",
        "detailed_findings": [
            {
                "severity": "HIGH",
                "line_number": "1",
                "function_context": "system",
                "finding": f"LLM analysis failed: {error_msg}",
                "recommendation": "Review code manually or retry analysis"
            }
        ],
        "key_recommendations": [
            "Retry analysis with LLM service",
            "Perform manual code review",
            "Check system connectivity"
        ],
        "raw_text": f"Error: {error_msg}"
    }

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
                "line": int(f["line_number"]) if f["line_number"].isdigit() else 1,
                "issue": f.get("finding", ""),
                "recommendation": f.get("recommendation", ""),
                "severity": "CRITICAL"
            })
    return findings

# ---------------------
# Enhanced Markdown formatter with better expandable sections
# ---------------------
def format_for_pr_display(json_response: dict) -> str:
    summary = json_response.get("summary", "Code review completed")
    findings = json_response.get("detailed_findings", [])
    recommendations = json_response.get("key_recommendations", [])
    
    # Count findings by severity
    critical_count = sum(1 for f in findings if f.get("severity", "").upper() == "CRITICAL")
    high_count = sum(1 for f in findings if f.get("severity", "").upper() == "HIGH")
    medium_count = sum(1 for f in findings if f.get("severity", "").upper() == "MEDIUM")
    total_issues = len(findings)

    display_text = f"## 🤖 Automated LLM Code Review\n\n"
    display_text += f"**📁 File Reviewed:** `{FILE_TO_REVIEW}`\n\n"
    
    # Add severity summary with emoji indicators
    if findings:
        display_text += f"### 📊 Issues Summary ({total_issues} total)\n"
        if critical_count > 0:
            display_text += f"🔴 **Critical:** {critical_count} issues\n"
        if high_count > 0:
            display_text += f"🟠 **High:** {high_count} issues\n"
        if medium_count > 0:
            display_text += f"🟡 **Medium:** {medium_count} issues\n"
        display_text += "\n"
    
    display_text += f"**📝 Summary:** {summary}\n\n"

    if findings:
        display_text += "<details>\n"
        display_text += "<summary><strong>🔍 Detailed Findings</strong> (Click to expand)</summary>\n\n"
        
        # Group findings by severity
        critical_findings = [f for f in findings if f.get("severity", "").upper() == "CRITICAL"]
        high_findings = [f for f in findings if f.get("severity", "").upper() == "HIGH"]
        medium_findings = [f for f in findings if f.get("severity", "").upper() == "MEDIUM"]
        
        # Display critical findings first
        if critical_findings:
            display_text += "#### 🔴 Critical Issues\n\n"
            for f in critical_findings:
                line = f.get("line_number", "N/A")
                issue = f.get("finding", "No description")
                rec = f.get("recommendation", "")
                context = f.get("function_context", "")
                
                display_text += f"**Line {line}**"
                if context:
                    display_text += f" | Context: `{context}`"
                display_text += f"\n> 🚨 **Issue:** {issue}\n"
                if rec:
                    display_text += f"> 💡 **Fix:** {rec}\n"
                display_text += "\n"
        
        # Display high findings
        if high_findings:
            display_text += "#### 🟠 High Priority Issues\n\n"
            for f in high_findings:
                line = f.get("line_number", "N/A")
                issue = f.get("finding", "No description")
                rec = f.get("recommendation", "")
                context = f.get("function_context", "")
                
                display_text += f"**Line {line}**"
                if context:
                    display_text += f" | Context: `{context}`"
                display_text += f"\n> ⚠️ **Issue:** {issue}\n"
                if rec:
                    display_text += f"> 💡 **Fix:** {rec}\n"
                display_text += "\n"
        
        # Display medium findings  
        if medium_findings:
            display_text += "#### 🟡 Medium Priority Issues\n\n"
            for f in medium_findings:
                line = f.get("line_number", "N/A")
                issue = f.get("finding", "No description")
                rec = f.get("recommendation", "")
                context = f.get("function_context", "")
                
                display_text += f"**Line {line}**"
                if context:
                    display_text += f" | Context: `{context}`"
                display_text += f"\n> ℹ️ **Issue:** {issue}\n"
                if rec:
                    display_text += f"> 💡 **Fix:** {rec}\n"
                display_text += "\n"
        
        display_text += "</details>\n\n"
    else:
        display_text += "✅ **No significant issues found.**\n\n"

    if recommendations:
        display_text += "<details>\n"
        display_text += "<summary><strong>💡 Key Recommendations</strong> (Click to expand)</summary>\n\n"
        for i, rec in enumerate(recommendations, 1):
            display_text += f"{i}. {rec}\n"
        display_text += "\n</details>\n\n"

    display_text += "---\n*🔬 Generated by Snowflake Cortex AI (llama3.1-70b)*"
    return display_text

# ---------------------
# Generate Enhanced Interactive HTML  
# ---------------------
def generate_interactive_html_report(json_response: dict, original_findings: list) -> str:
    findings = json_response.get("detailed_findings", [])
    summary = json_response.get("summary", "Code review completed")
    
    # Count findings by severity
    critical_count = sum(1 for f in findings if f.get("severity", "").upper() == "CRITICAL")
    high_count = sum(1 for f in findings if f.get("severity", "").upper() == "HIGH")
    medium_count = sum(1 for f in findings if f.get("severity", "").upper() == "MEDIUM")
    total_count = len(findings)
    
    priority_class = "priority-critical" if critical_count > 0 else ("priority-high" if high_count > 0 else "priority-medium")

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Code Review Report - {os.path.basename(FILE_TO_REVIEW)}</title>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            
            body {{ 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                color: #343a40; 
                min-height: 100vh;
                padding: 20px;
            }}
            
            .container {{ 
                max-width: 1200px; 
                margin: 0 auto; 
                background: white; 
                border-radius: 12px; 
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            
            .header {{ 
                padding: 40px 30px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                color: white; 
                text-align: center;
            }}
            
            .header h1 {{
                font-size: 2em;
                margin-bottom: 10px;
                font-weight: 600;
            }}
            
            .header .subtitle {{
                opacity: 0.9;
                font-size: 1.1em;
            }}
            
            .stats-bar {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 0;
                background: #f8f9fa;
            }}
            
            .stat-item {{
                padding: 30px 20px;
                text-align: center;
                border-right: 1px solid #dee2e6;
                transition: background-color 0.3s;
            }}
            
            .stat-item:last-child {{ border-right: none; }}
            .stat-item:hover {{ background: #e9ecef; }}
            
            .stat-number {{
                font-size: 2.5em;
                font-weight: bold;
                margin-bottom: 10px;
                line-height: 1;
            }}
            
            .stat-label {{
                font-size: 1em;
                color: #6c757d;
                font-weight: 500;
            }}
            
            .stat-critical {{ color: #dc3545; }}
            .stat-high {{ color: #fd7e14; }}
            .stat-medium {{ color: #ffc107; }}
            .stat-total {{ color: #007bff; }}
            
            .summary-section {{
                padding: 30px;
                border-bottom: 1px solid #dee2e6;
                background: #f8f9fa;
            }}
            
            .summary-section h2 {{
                color: #495057;
                margin-bottom: 15px;
                font-size: 1.4em;
            }}
            
            .summary-text {{
                font-size: 1.1em;
                line-height: 1.6;
                color: #6c757d;
            }}
            
            .file-section {{
                padding: 0;
            }}
            
            .file-header {{ 
                padding: 25px 30px; 
                cursor: pointer; 
                display: flex; 
                align-items: center; 
                background: white;
                border-bottom: 1px solid #dee2e6;
                transition: all 0.3s ease;
                position: relative;
            }}
            
            .file-header:hover {{ 
                background: #f8f9fa;
                padding-left: 35px;
            }}
            
            .file-header::before {{
                content: '';
                position: absolute;
                left: 0;
                top: 0;
                bottom: 0;
                width: 4px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                transform: scaleY(0);
                transition: transform 0.3s ease;
            }}
            
            .file-header:hover::before {{
                transform: scaleY(1);
            }}
            
            .expand-icon {{ 
                margin-right: 20px; 
                font-size: 1.3em;
                transition: transform 0.3s ease;
                color: #007bff;
            }}
            
            .expand-icon.expanded {{
                transform: rotate(90deg);
            }}
            
            .file-name {{
                font-size: 1.2em;
                font-weight: 600;
                color: #495057;
            }}
            
            .file-details {{ 
                display: none; 
                background: #f8f9fa; 
            }}
            
            .file-details.expanded {{ 
                display: block; 
                animation: slideDown 0.4s ease-out;
            }}
            
            @keyframes slideDown {{
                from {{ opacity: 0; transform: translateY(-20px); }}
                to {{ opacity: 1; transform: translateY(0); }}
            }}
            
            .priority-badge {{ 
                margin-left: auto; 
                padding: 8px 16px; 
                border-radius: 20px; 
                font-size: 0.85em; 
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }}
            
            .priority-critical {{ 
                background: linear-gradient(135deg, #dc3545, #c82333); 
                color: white; 
                box-shadow: 0 4px 15px rgba(220, 53, 69, 0.3);
            }}
            
            .priority-high {{ 
                background: linear-gradient(135deg, #fd7e14, #e55a00); 
                color: white; 
                box-shadow: 0 4px 15px rgba(253, 126, 20, 0.3);
            }}
            
            .priority-medium {{ 
                background: linear-gradient(135deg, #ffc107, #e0a800); 
                color: #212529; 
                box-shadow: 0 4px 15px rgba(255, 193, 7, 0.3);
            }}
            
            .severity-section {{
                margin: 0;
                border-bottom: 1px solid #dee2e6;
            }}
            
            .severity-title {{
                padding: 20px 30px;
                margin: 0;
                font-weight: 600;
                font-size: 1.2em;
                display: flex;
                align-items: center;
                gap: 10px;
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
            
            .issues-container {{
                padding: 20px 30px 30px 30px;
            }}
            
            .issue-item {{ 
                border: 1px solid #dee2e6; 
                border-radius: 12px; 
                margin-bottom: 20px; 
                background: white;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                transition: all 0.3s ease;
                overflow: hidden;
            }}
            
            .issue-item:hover {{
                transform: translateY(-2px);
                box-shadow: 0 8px 25px rgba(0,0,0,0.1);
            }}
            
            .issue-header {{
                padding: 20px 25px;
                border-bottom: 1px solid #f0f0f0;
            }}
            
            .issue-title {{ 
                font-weight: 600; 
                margin-bottom: 12px; 
                font-size: 1.1em;
                color: #495057;
                line-height: 1.4;
            }}
            
            .issue-meta {{ 
                display: flex;
                align-items: center;
                gap: 15px;
                font-size: 0.9em; 
                color: #6c757d;
            }}
            
            .line-badge {{
                background: #007bff;
                color: white;
                padding: 4px 12px;
                border-radius: 12px;
                font-size: 0.85em;
                font-weight: 500;
            }}
            
            .context-badge {{
                background: #e9ecef;
                color: #495057;
                padding: 4px 12px;
                border-radius: 12px;
                font-size: 0.85em;
            }}
            
            .recommendation {{
                padding: 0 25px 20px 25px;
                color: #28a745;
                font-style: italic;
            }}
            
            .recommendation::before {{
                content: "💡 ";
                margin-right: 5px;
            }}
            
            .code-diff {{ 
                border-top: 1px solid #f0f0f0;
                margin-top: 15px;
            }}
            
            .diff-header {{ 
                background: #f8f9fa; 
                padding: 12px 25px; 
                cursor: pointer; 
                font-size: 0.9em; 
                font-weight: 500;
                transition: background-color 0.2s;
                display: flex;
                align-items: center;
                gap: 8px;
            }}
            
            .diff-header:hover {{
                background: #e9ecef;
            }}
            
            .diff-body {{ 
                display: none; 
                border-top: 1px solid #f0f0f0;
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
                background: #fff5f5; 
                padding: 20px 25px; 
                font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', monospace; 
                font-size: 0.9em; 
                border-bottom: 1px solid #fed7d7;
                line-height: 1.6;
            }}
            
            .optimized-code {{ 
                background: #f0fff4; 
                padding: 20px 25px; 
                font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', monospace; 
                font-size: 0.9em;
                line-height: 1.6;
            }}
            
            .no-issues {{
                text-align: center;
                padding: 80px 20px;
                color: #28a745;
                font-size: 1.3em;
            }}
            
            .no-issues .checkmark {{
                font-size: 4em;
                margin-bottom: 20px;
                display: block;
            }}
            
            .footer {{
                padding: 30px;
                text-align: center;
                background: #f8f9fa;
                color: #6c757d;
                border-top: 1px solid #dee2e6;
            }}
            
            .footer .timestamp {{
                font-size: 0.9em;
                margin-top: 10px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Code Review Report</h1>
                <div class="subtitle">Automated Analysis Results</div>
            </div>
            
            <div class="stats-bar">
                <div class="stat-item">
                    <div class="stat-number stat-total">{total_count}</div>
                    <div class="stat-label">Total Issues</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number stat-critical">{critical_count}</div>
                    <div class="stat-label">Critical</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number stat-high">{high_count}</div>
                    <div class="stat-label">High Priority</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number stat-medium">{medium_count}</div>
                    <div class="stat-label">Medium Priority</div>
                </div>
            </div>
            
            <div class="summary-section">
                <h2>📝 Analysis Summary</h2>
                <div class="summary-text">{summary}</div>
            </div>
            
            <div class="file-section">
                <div class="file-header" onclick="toggleFile('file1')">
                    <span class="expand-icon" id="expand-file1">▶</span>
                    <span class="file-name">📁 {os.path.basename(FILE_TO_REVIEW)}</span>
                    <div class="priority-badge {priority_class}">
                        {total_count} {'issue' if total_count == 1 else 'issues'}
                    </div>
                </div>
                
                <div class="file-details" id="file1">
    """

    if not findings:
        html_content += """
                    <div class="no-issues">
                        <span class="checkmark">✅</span>
                        <div>No issues found in this file!</div>
                        <div style="font-size: 0.9em; margin-top: 10px; color: #6c757d;">
                            Great job maintaining clean code!
                        </div>
                    </div>
        """
    else:
        # Group issues by severity
        critical_issues = [f for f in findings if f.get("severity", "").upper() == "CRITICAL"]
        high_issues = [f for f in findings if f.get("severity", "").upper() == "HIGH"]
        medium_issues = [f for f in findings if f.get("severity", "").upper() == "MEDIUM"]
        
        # Display issues by severity
        for severity, issues_list, title, emoji in [
            ("CRITICAL", critical_issues, "Critical Issues", "🔴"),
            ("HIGH", high_issues, "High Priority Issues", "🟠"), 
            ("MEDIUM", medium_issues, "Medium Priority Issues", "🟡")
        ]:
            if issues_list:
                html_content += f"""
                    <div class="severity-section">
                        <div class="severity-title severity-{severity.lower()}">
                            <span>{emoji}</span>
                            <span>{title}</span>
                            <span style="margin-left: auto;">({len(issues_list)} {'issue' if len(issues_list) == 1 else 'issues'})</span>
                        </div>
                        <div class="issues-container">
                """
                
                for idx, issue in enumerate(issues_list, 1):
                    finding = issue.get("finding", "No description")
                    recommendation = issue.get("recommendation", "")
                    line = issue.get("line_number", "N/A")
                    context = issue.get("function_context", "")
                    
                    html_content += f"""
                            <div class="issue-item">
                                <div class="issue-header">
                                    <div class="issue-title">{finding}</div>
                                    <div class="issue-meta">
                                        <span class="line-badge">Line {line}</span>
                                        {f'<span class="context-badge">{context}</span>' if context else ''}
                                    </div>
                                </div>
                                {f'<div class="recommendation">{recommendation}</div>' if recommendation else ''}
                                <div class="code-diff">
                                    <div class="diff-header" onclick="toggleDiff('diff-{severity}-{idx}')">
                                        <span>▶</span>
                                        <span>View Code Context & Suggestions</span>
                                    </div>
                                    <div class="diff-body" id="diff-{severity}-{idx}">
                                        <div class="current-code">
                                            <strong>📍 Current code at line {line}:</strong><br/>
                                            # {finding[:100]}...
                                        </div>
                                        <div class="optimized-code">
                                            <strong>✨ Suggested improvement:</strong><br/>
                                            # {recommendation if recommendation else 'Apply the recommended changes above'}
                                        </div>
                                    </div>
                                </div>
                            </div>
                    """
                
                html_content += """
                        </div>
                    </div>
                """

    html_content += f"""
                </div>
            </div>
            
            <div class="footer">
                <div>🔬 <strong>Generated by Snowflake Cortex AI</strong> (llama3.1-70b)</div>
                <div class="timestamp">Report generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}</div>
            </div>
        </div>
        
        <script>
            function toggleFile(fileId) {{
                const details = document.getElementById(fileId);
                const icon = document.getElementById('expand-' + fileId);
                
                if (details.classList.contains('expanded')) {{
                    details.classList.remove('expanded');
                    icon.classList.remove('expanded');
                    icon.textContent = '▶';
                }} else {{
                    details.classList.add('expanded');
                    icon.classList.add('expanded');
                    icon.textContent = '▼';
                }}
            }}
            
            function toggleDiff(diffId) {{
                const diffBody = document.getElementById(diffId);
                const header = diffBody.previousElementSibling;
                const arrow = header.querySelector('span');
                
                if (diffBody.classList.contains('expanded')) {{
                    diffBody.classList.remove('expanded');
                    arrow.textContent = '▶';
                }} else {{
                    diffBody.classList.add('expanded');
                    arrow.textContent = '▼';
                }}
            }}
            
            // Auto-expand first file on page load
            document.addEventListener('DOMContentLoaded', function() {{
                setTimeout(() => toggleFile('file1'), 100);
            }});
        </script>
    </body>
    </html>
    """
    return html_content

# ---------------------
# Main execution
# ---------------------
if __name__ == "__main__":
    try:
        if not os.path.exists(FILE_TO_REVIEW):
            print(f"❌ File {FILE_TO_REVIEW} not found")
            exit(1)
            
        print(f"📖 Reading file: {FILE_TO_REVIEW}")
        code_text = Path(FILE_TO_REVIEW).read_text()
        print(f"📝 Code length: {len(code_text)} characters")
        
        # Get review from Cortex
        report = review_with_cortex(MODEL, code_text)
        print(f"📋 Raw report keys: {list(report.keys())}")
        
        original_findings = report.get("detailed_findings", [])
        print(f"🔍 Found {len(original_findings)} total findings")
        
        # Filter and process
        filtered = filter_low_severity(report)
        filtered_findings = filtered.get("detailed_findings", [])
        print(f"🎯 After filtering: {len(filtered_findings)} findings")
        
        criticals = extract_critical_findings(filtered)
        print(f"🚨 Critical issues: {len(criticals)}")

        # Generate outputs
        formatted_review = format_for_pr_display(filtered)
        html_report = generate_interactive_html_report(filtered, original_findings)
        
        # Save files
        with open("dbt_code_review_report.html", "w", encoding='utf-8') as f: 
            f.write(html_report)
            
        with open("review_output.json", "w", encoding='utf-8') as f: 
            json.dump({
                "full_review": formatted_review,              
                "full_review_markdown": formatted_review,     
                "full_review_json": filtered,                 
                "criticals": criticals,
                "file": FILE_TO_REVIEW,
                "interactive_report_path": "dbt_code_review_report.html",
                "timestamp": datetime.now().isoformat()
            }, f, indent=2, ensure_ascii=False)

        # Print summary
        print("\n" + "="*60)
        print("✅ CODE REVIEW COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"📁 File analyzed: {FILE_TO_REVIEW}")
        print(f"🔍 Total issues found: {len(filtered_findings)}")
        
        if filtered_findings:
            critical_count = sum(1 for f in filtered_findings if f.get("severity", "").upper() == "CRITICAL")
            high_count = sum(1 for f in filtered_findings if f.get("severity", "").upper() == "HIGH") 
            medium_count = sum(1 for f in filtered_findings if f.get("severity", "").upper() == "MEDIUM")
            
            print(f"🔴 Critical: {critical_count}")
            print(f"🟠 High: {high_count}")
            print(f"🟡 Medium: {medium_count}")
        
        print(f"\n📄 Files generated:")
        print(f"  • review_output.json (GitHub comment data)")
        print(f"  • dbt_code_review_report.html (Interactive report)")
        print(f"\n🌐 Open dbt_code_review_report.html in your browser to view the full interactive report!")
        
    except Exception as e:
        print(f"❌ Error during code review: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'session' in locals():
            session.close()
            print("🔒 Snowflake session closed")
