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
# Enhanced Prompt template for executive-level analysis
# ---------------------
PROMPT_TEMPLATE = """You are a senior software architect performing an executive-level code review. Analyze the Python code for business impact, technical debt, security risks, and maintainability.

IMPORTANT: Respond ONLY with valid JSON in this exact format:
{
    "executive_summary": "High-level assessment of code quality, business risks, and overall technical debt",
    "quality_score": 85,
    "business_impact": "LOW|MEDIUM|HIGH - overall business risk assessment",
    "technical_debt_score": "LOW|MEDIUM|HIGH",
    "security_risk_level": "LOW|MEDIUM|HIGH|CRITICAL",
    "maintainability_rating": "POOR|FAIR|GOOD|EXCELLENT",
    "detailed_findings": [
        {
            "severity": "CRITICAL|HIGH|MEDIUM|LOW",
            "category": "Security|Performance|Maintainability|Best Practices|Documentation|Error Handling",
            "line_number": "actual_line_number",
            "function_context": "function_name_if_applicable",
            "finding": "Detailed technical issue description",
            "business_impact": "How this affects business operations or risk",
            "recommendation": "Specific technical solution",
            "effort_estimate": "LOW|MEDIUM|HIGH - effort to fix",
            "priority_ranking": 1
        }
    ],
    "metrics": {
        "lines_of_code": 150,
        "complexity_score": "LOW|MEDIUM|HIGH",
        "code_coverage_gaps": ["area1", "area2"],
        "dependency_risks": ["risk1", "risk2"]
    },
    "strategic_recommendations": [
        "High-level recommendation for technical leadership",
        "Process improvement suggestion"
    ],
    "immediate_actions": [
        "Critical item requiring immediate attention",
        "Quick win opportunity"
    ]
}

Code to review:
```python
{PY_CONTENT}
```

Focus on:
- CRITICAL: Security vulnerabilities, data breaches, system failures
- HIGH: Performance bottlenecks, architectural issues, compliance risks  
- MEDIUM: Code quality, maintainability concerns, technical debt
- LOW: Style issues, minor optimizations

Provide executive-level insights focusing on business impact and strategic technical decisions."""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text)

# ---------------------
# Enhanced Cortex call with better error handling
# ---------------------
def review_with_cortex(model: str, code_text: str) -> dict:
    prompt = build_prompt(code_text)
    clean_prompt = prompt.replace("'", "''").replace("\\", "\\\\")
    
    query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{clean_prompt}'
        ) as response
    """
    
    try:
        print(f"🔍 Analyzing code with {model}...")
        df = session.sql(query)
        result = df.collect()[0][0]
        
        print(f"📊 Processing LLM response...")
        
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            json_match = re.search(r'\{.*\}', result, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except json.JSONDecodeError:
                    pass
            
            print("⚠️ Creating structured response from analysis...")
            return parse_executive_response(result, code_text)
            
    except Exception as e:
        print(f"❌ Analysis error: {e}")
        return create_executive_fallback(code_text, str(e))

# ---------------------
# Parse response for executive format
# ---------------------
def parse_executive_response(response_text: str, code_text: str) -> dict:
    lines = code_text.split('\n')
    code_length = len(lines)
    
    findings = []
    security_issues = []
    performance_issues = []
    maintainability_issues = []
    
    for i, line in enumerate(lines, 1):
        line_stripped = line.strip().lower()
        
        if any(keyword in line_stripped for keyword in ['password', 'secret', 'key', 'token']) and '=' in line_stripped:
            security_issues.append({
                "severity": "CRITICAL",
                "category": "Security", 
                "line_number": str(i),
                "function_context": "authentication",
                "finding": "Hardcoded credentials detected in source code",
                "business_impact": "HIGH - Risk of credential exposure and unauthorized access",
                "recommendation": "Implement secure credential management using environment variables or secure vault",
                "effort_estimate": "MEDIUM",
                "priority_ranking": 1
            })
        
        if any(keyword in line_stripped for keyword in ['session.sql(', 'df.collect()']):
            performance_issues.append({
                "severity": "MEDIUM",
                "category": "Performance",
                "line_number": str(i), 
                "function_context": "database_operations",
                "finding": "Direct database query execution without optimization",
                "business_impact": "MEDIUM - Potential performance bottlenecks affecting user experience",
                "recommendation": "Implement query optimization, connection pooling, and caching strategies",
                "effort_estimate": "HIGH",
                "priority_ranking": 2
            })
        
        if 'except:' in line_stripped or 'except exception:' in line_stripped:
            maintainability_issues.append({
                "severity": "HIGH",
                "category": "Error Handling",
                "line_number": str(i),
                "function_context": "exception_handling",
                "finding": "Generic exception handling reduces debugging capability",
                "business_impact": "MEDIUM - Increased troubleshooting time and operational costs",
                "recommendation": "Implement specific exception handling with proper logging and monitoring",
                "effort_estimate": "LOW",
                "priority_ranking": 3
            })
    
    all_findings = security_issues + performance_issues + maintainability_issues
    critical_count = len(security_issues)
    high_count = len([f for f in all_findings if f["severity"] == "HIGH"])
    
    quality_score = max(30, 100 - (critical_count * 25) - (high_count * 10))
    business_impact = "HIGH" if critical_count > 0 else ("MEDIUM" if high_count > 0 else "LOW")
    security_risk = "CRITICAL" if critical_count > 2 else ("HIGH" if critical_count > 0 else "MEDIUM")
    
    return {
        "executive_summary": f"Analysis of {code_length} lines reveals {len(all_findings)} technical concerns requiring attention. Primary risks identified in credential management and error handling patterns.",
        "quality_score": quality_score,
        "business_impact": business_impact,
        "technical_debt_score": "MEDIUM" if len(all_findings) > 3 else "LOW",
        "security_risk_level": security_risk,
        "maintainability_rating": "FAIR" if len(all_findings) < 5 else "POOR",
        "detailed_findings": all_findings[:10],
        "metrics": {
            "lines_of_code": code_length,
            "complexity_score": "MEDIUM" if code_length > 100 else "LOW",
            "code_coverage_gaps": ["error_handling", "input_validation"],
            "dependency_risks": ["database_connection", "credential_management"]
        },
        "strategic_recommendations": [
            "Implement comprehensive security review process for credential management",
            "Establish code quality gates with automated security scanning",
            "Create standardized error handling and logging framework"
        ],
        "immediate_actions": [
            "Secure hardcoded credentials within 24 hours",
            "Implement proper exception handling patterns",
            "Add input validation for database operations"
        ],
        "raw_analysis": response_text
    }

def create_executive_fallback(code_text: str, error_msg: str) -> dict:
    lines = len(code_text.split('\n'))
    
    return {
        "executive_summary": f"Technical analysis of {lines} lines completed with system limitations. Manual review recommended for comprehensive assessment.",
        "quality_score": 75,
        "business_impact": "MEDIUM",
        "technical_debt_score": "MEDIUM", 
        "security_risk_level": "MEDIUM",
        "maintainability_rating": "FAIR",
        "detailed_findings": [{
            "severity": "HIGH",
            "category": "System",
            "line_number": "1",
            "function_context": "analysis_system",
            "finding": f"Automated analysis system limitation: {error_msg}",
            "business_impact": "MEDIUM - Requires manual technical review for complete assessment",
            "recommendation": "Conduct manual code review by senior technical staff",
            "effort_estimate": "HIGH",
            "priority_ranking": 1
        }],
        "metrics": {
            "lines_of_code": lines,
            "complexity_score": "UNKNOWN",
            "code_coverage_gaps": ["automated_analysis"],
            "dependency_risks": ["system_connectivity"]
        },
        "strategic_recommendations": [
            "Establish backup manual review processes",
            "Investigate automated analysis system reliability"
        ],
        "immediate_actions": [
            "Schedule manual code review session",
            "Document analysis system limitations"
        ]
    }

def format_executive_pr_display(json_response: dict) -> str:
    summary = json_response.get("executive_summary", "Technical analysis completed")
    findings = json_response.get("detailed_findings", [])
    quality_score = json_response.get("quality_score", 75)
    business_impact = json_response.get("business_impact", "MEDIUM")
    security_risk = json_response.get("security_risk_level", "MEDIUM")
    tech_debt = json_response.get("technical_debt_score", "MEDIUM")
    maintainability = json_response.get("maintainability_rating", "FAIR")
    metrics = json_response.get("metrics", {})
    strategic_recs = json_response.get("strategic_recommendations", [])
    immediate_actions = json_response.get("immediate_actions", [])
    
    critical_count = sum(1 for f in findings if f.get("severity", "").upper() == "CRITICAL")
    high_count = sum(1 for f in findings if f.get("severity", "").upper() == "HIGH")
    medium_count = sum(1 for f in findings if f.get("severity", "").upper() == "MEDIUM")
    
    security_findings = [f for f in findings if f.get("category", "") == "Security"]
    performance_findings = [f for f in findings if f.get("category", "") == "Performance"]
    
    risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
    quality_emoji = "🟢" if quality_score >= 80 else ("🟡" if quality_score >= 60 else "🔴")
    
    display_text = f"""# 📊 Executive Code Review Report

**File:** `{FILE_TO_REVIEW}` | **Analysis Date:** {datetime.now().strftime('%Y-%m-%d')}

## 🎯 Executive Summary
{summary}

## 📈 Quality Dashboard

| Metric | Score | Status | Business Impact |
|--------|-------|--------|-----------------|
| **Overall Quality** | {quality_score}/100 | {quality_emoji} | {business_impact} Risk |
| **Security Risk** | {security_risk} | {risk_emoji.get(security_risk, "🟡")} | {len(security_findings)} vulnerabilities |
| **Technical Debt** | {tech_debt} | {risk_emoji.get(tech_debt, "🟡")} | {len(findings)} items |
| **Maintainability** | {maintainability} | {risk_emoji.get(maintainability, "🟡")} | Long-term sustainability |

## 🔍 Issue Distribution

| Severity | Count | Category Breakdown | Priority Actions |
|----------|-------|-------------------|------------------|
| 🔴 Critical | {critical_count} | Security: {len([f for f in findings if f.get("severity") == "CRITICAL" and f.get("category") == "Security"])} | Immediate fix required |
| 🟠 High | {high_count} | Performance: {len([f for f in findings if f.get("severity") == "HIGH" and f.get("category") == "Performance"])} | Fix within sprint |
| 🟡 Medium | {medium_count} | Quality: {len([f for f in findings if f.get("severity") == "MEDIUM"])} | Plan for next release |

"""

    if metrics:
        loc = metrics.get("lines_of_code", "N/A")
        complexity = metrics.get("complexity_score", "N/A")
        coverage_gaps = len(metrics.get("code_coverage_gaps", []))
        dep_risks = len(metrics.get("dependency_risks", []))
        
        display_text += f"""## 📊 Technical Metrics

| Metric | Value | Assessment | Recommendation |
|--------|-------|------------|----------------|
| **Lines of Code** | {loc} | {'🟢 Manageable' if isinstance(loc, int) and loc < 200 else '🟡 Monitor'} | {'Good size' if isinstance(loc, int) and loc < 200 else 'Consider refactoring'} |
| **Complexity** | {complexity} | {risk_emoji.get(complexity, "🟡")} | {'Acceptable' if complexity == 'LOW' else 'Review architecture'} |
| **Coverage Gaps** | {coverage_gaps} areas | {'🟢 Good' if coverage_gaps < 3 else '🟡 Needs attention'} | {'Maintain current' if coverage_gaps < 3 else 'Increase test coverage'} |
| **Dependency Risks** | {dep_risks} items | {'🟢 Low risk' if dep_risks < 3 else '🟡 Monitor'} | {'Current approach OK' if dep_risks < 3 else 'Review dependencies'} |

"""

    if findings:
        display_text += """<details>
<summary><strong>🔍 Detailed Technical Findings</strong> (Click to expand)</summary>

| Priority | Category | Line | Issue | Business Impact | Effort |
|----------|----------|------|-------|-----------------|--------|
"""
        
        sorted_findings = sorted(findings, key=lambda x: (
            x.get("priority_ranking", 999),
            {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}.get(x.get("severity", "LOW"), 4)
        ))
        
        for finding in sorted_findings[:15]:
            severity = finding.get("severity", "MEDIUM")
            category = finding.get("category", "General")
            line = finding.get("line_number", "N/A")
            issue = finding.get("finding", "")[:80] + ("..." if len(finding.get("finding", "")) > 80 else "")
            business_impact_text = finding.get("business_impact", "")[:60] + ("..." if len(finding.get("business_impact", "")) > 60 else "")
            effort = finding.get("effort_estimate", "MEDIUM")
            
            priority_emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(severity, "🟡")
            effort_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(effort, "🟡")
            
            display_text += f"| {priority_emoji} {severity} | {category} | {line} | {issue} | {business_impact_text} | {effort_emoji} {effort} |\n"
        
        display_text += "\n</details>\n\n"

    if strategic_recs:
        display_text += """## 🎯 Strategic Recommendations

<details>
<summary><strong>💡 Leadership Actions</strong> (Click to expand)</summary>

| Priority | Recommendation | Expected Outcome | Timeline |
|----------|----------------|------------------|----------|
"""
        for i, rec in enumerate(strategic_recs, 1):
            priority = "🔴 High" if i <= 2 else "🟡 Medium"
            timeline = "2-4 weeks" if i <= 2 else "1-2 months"
            outcome = "Risk reduction" if "security" in rec.lower() or "risk" in rec.lower() else "Quality improvement"
            
            display_text += f"| {priority} | {rec} | {outcome} | {timeline} |\n"
        
        display_text += "\n</details>\n\n"

    if immediate_actions:
        display_text += """## ⚡ Immediate Actions Required

<details>
<summary><strong>🚨 Critical Tasks</strong> (Click to expand)</summary>

| Urgency | Action | Owner | Due Date |
|---------|--------|-------|----------|
"""
        for i, action in enumerate(immediate_actions, 1):
            urgency = "🔴 Critical" if "24 hours" in action or "immediate" in action.lower() else "🟠 High"
            owner = "Security Team" if "credential" in action.lower() or "security" in action.lower() else "Dev Team"
            due_date = "24 hours" if "24 hours" in action else "End of sprint"
            
            display_text += f"| {urgency} | {action} | {owner} | {due_date} |\n"
        
        display_text += "\n</details>\n\n"

    display_text += f"""---

**📋 Review Summary:** {len(findings)} findings identified | **🎯 Quality Score:** {quality_score}/100 | **⚡ Critical Issues:** {critical_count}

*🔬 Powered by Snowflake Cortex AI • Executive Technical Analysis*"""

    return display_text

def generate_executive_html_report(json_response: dict) -> str:
    findings = json_response.get("detailed_findings", [])
    quality_score = json_response.get("quality_score", 75)
    business_impact = json_response.get("business_impact", "MEDIUM")
    security_risk = json_response.get("security_risk_level", "MEDIUM")
    tech_debt = json_response.get("technical_debt_score", "MEDIUM") 
    maintainability = json_response.get("maintainability_rating", "FAIR")
    summary = json_response.get("executive_summary", "Analysis completed")
    
    critical_count = sum(1 for f in findings if f.get("severity", "").upper() == "CRITICAL")
    high_count = sum(1 for f in findings if f.get("severity", "").upper() == "HIGH")
    medium_count = sum(1 for f in findings if f.get("severity", "").upper() == "MEDIUM")
    total_count = len(findings)
    
    quality_color = "#28a745" if quality_score >= 80 else ("#ffc107" if quality_score >= 60 else "#dc3545")
    risk_colors = {"LOW": "#28a745", "MEDIUM": "#ffc107", "HIGH": "#fd7e14", "CRITICAL": "#dc3545"}
    
    if findings:
        sorted_findings = sorted(findings, key=lambda x: (
            x.get("priority_ranking", 999),
            {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}.get(x.get("severity", "LOW"), 4)
        ))[:15]
        
        findings_html = """
            <table class="findings-table">
                <thead>
                    <tr>
                        <th>Priority</th>
                        <th>Category</th>
                        <th>Line</th>
                        <th>Technical Issue</th>
                        <th>Business Impact</th>
                        <th>Effort</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for f in sorted_findings:
            severity = f.get("severity", "MEDIUM").upper()
            category = f.get("category", "General")
            line_num = f.get("line_number", "N/A")
            finding = f.get("finding", "No description")
            business_impact_text = f.get("business_impact", "")
            effort = f.get("effort_estimate", "MEDIUM").upper()
            
            finding_text = finding[:100] + ("..." if len(finding) > 100 else "")
            impact_text = business_impact_text[:80] + ("..." if len(business_impact_text) > 80 else "")
            
            effort_icon = "clock" if effort == "LOW" else ("hourglass-half" if effort == "MEDIUM" else "hourglass-end")
            
            findings_html += f"""
                    <tr>
                        <td><span class="severity-badge severity-{severity.lower()}">{severity}</span></td>
                        <td><span class="category-tag">{category}</span></td>
                        <td><strong>{line_num}</strong></td>
                        <td>{finding_text}</td>
                        <td>{impact_text}</td>
                        <td><span class="effort-indicator effort-{effort.lower()}"><i class="fas fa-{effort_icon}"></i> {effort}</span></td>
                    </tr>
            """
        
        findings_html += """
                </tbody>
            </table>
        """
    else:
        findings_html = """
            <div class="no-issues">
                <i class="fas fa-check-circle checkmark"></i>
                No technical issues identified.<br/>
                <small style="color: #6c757d; margin-top: 10px;">Excellent code quality maintained!</small>
            </div>
        """
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Executive Code Review - {os.path.basename(FILE_TO_REVIEW)}</title>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif; 
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); 
            color: #343a40; 
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{ 
            max-width: 1400px; 
            margin: 0 auto; 
            background: white; 
            border-radius: 16px; 
            box-shadow: 0 25px 50px rgba(0,0,0,0.15);
            overflow: hidden;
        }}
        
        .executive-header {{ 
            padding: 40px 50px; 
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); 
            color: white; 
        }}
        
        .header-title {{
            font-size: 2.5em;
            font-weight: 700;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        
        .header-subtitle {{
            font-size: 1.2em;
            opacity: 0.9;
            margin-bottom: 20px;
        }}
        
        .header-meta {{
            display: flex;
            gap: 30px;
            font-size: 0.95em;
            opacity: 0.8;
        }}
        
        .kpi-dashboard {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 0;
            background: #f8f9fa;
        }}
        
        .kpi-card {{
            padding: 40px 30px;
            text-align: center;
            border-right: 1px solid #dee2e6;
            transition: all 0.3s ease;
        }}
        
        .kpi-card:last-child {{ border-right: none; }}
        
        .kpi-card:hover {{
            background: white;
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(0,0,0,0.1);
        }}
        
        .kpi-icon {{
            font-size: 2.5em;
            margin-bottom: 15px;
            opacity: 0.8;
        }}
        
        .kpi-value {{
            font-size: 3em;
            font-weight: bold;
            margin-bottom: 10px;
            line-height: 1;
        }}
        
        .kpi-label {{
            font-size: 1.1em;
            color: #6c757d;
            font-weight: 600;
            margin-bottom: 5px;
        }}
        
        .kpi-status {{
            font-size: 0.9em;
            font-weight: 500;
            padding: 4px 12px;
            border-radius: 20px;
            display: inline-block;
        }}
        
        .status-excellent {{ background: #d4edda; color: #155724; }}
        .status-good {{ background: #d1ecf1; color: #0c5460; }}
        .status-fair {{ background: #fff3cd; color: #856404; }}
        .status-poor {{ background: #f8d7da; color: #721c24; }}
        .status-critical {{ background: #f8d7da; color: #721c24; }}
        .status-high {{ background: #fff3cd; color: #856404; }}
        .status-medium {{ background: #d1ecf1; color: #0c5460; }}
        .status-low {{ background: #d4edda; color: #155724; }}
        
        .executive-summary {{
            padding: 40px 50px;
            background: white;
            border-bottom: 1px solid #dee2e6;
        }}
        
        .summary-title {{
            font-size: 1.6em;
            font-weight: 600;
            margin-bottom: 20px;
            color: #495057;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .summary-text {{
            font-size: 1.15em;
            line-height: 1.7;
            color: #6c757d;
            background: #f8f9fa;
            padding: 25px;
            border-radius: 8px;
            border-left: 4px solid #007bff;
        }}
        
        .findings-section {{
            padding: 40px 50px;
        }}
        
        .section-title {{
            font-size: 1.8em;
            font-weight: 600;
            margin-bottom: 30px;
            color: #495057;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        
        .findings-table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }}
        
        .findings-table th {{
            background: linear-gradient(135deg, #495057, #6c757d);
            color: white;
            padding: 20px 15px;
            text-align: left;
            font-weight: 600;
            font-size: 0.95em;
        }}
        
        .findings-table td {{
            padding: 18px 15px;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }}
        
        .findings-table tr:hover {{
            background: #f8f9fa;
        }}
        
        .severity-badge {{
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
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
        
        .severity-low {{
            background: linear-gradient(135deg, #28a745, #1e7e34);
            color: white;
        }}
        
        .category-tag {{
            background: #e9ecef;
            color: #495057;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: 500;
        }}
        
        .effort-indicator {{
            display: flex;
            align-items: center;
            gap: 5px;
            font-size: 0.9em;
            font-weight: 500;
        }}
        
        .effort-low {{ color: #28a745; }}
        .effort-medium {{ color: #ffc107; }}
        .effort-high {{ color: #dc3545; }}
        
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
            padding: 30px 50px;
            text-align: center;
            background: linear-gradient(135deg, #495057, #6c757d);
            color: white;
        }}
        
        .footer-content {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 20px;
        }}
        
        .footer-left {{
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        
        .footer-right {{
            font-size: 0.9em;
            opacity: 0.8;
        }}
        
        @media (max-width: 768px) {{
            .container {{ margin: 10px; }}
            .executive-header, .executive-summary, .findings-section {{
                padding: 20px 25px;
            }}
            .kpi-dashboard {{ grid-template-columns: repeat(2, 1fr); }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="executive-header">
            <div class="header-content">
                <h1 class="header-title">
                    <i class="fas fa-chart-line"></i>
                    Executive Code Review
                </h1>
                <div class="header-subtitle">Strategic Technical Assessment & Risk Analysis</div>
                <div class="header-meta">
                    <div><i class="fas fa-file-code"></i> {os.path.basename(FILE_TO_REVIEW)}</div>
                    <div><i class="fas fa-calendar"></i> {datetime.now().strftime('%B %d, %Y')}</div>
                    <div><i class="fas fa-clock"></i> {datetime.now().strftime('%H:%M UTC')}</div>
                </div>
            </div>
        </div>
        
        <div class="kpi-dashboard">
            <div class="kpi-card">
                <div class="kpi-icon" style="color: {quality_color};">
                    <i class="fas fa-gauge-high"></i>
                </div>
                <div class="kpi-value" style="color: {quality_color};">{quality_score}</div>
                <div class="kpi-label">Quality Score</div>
                <div class="kpi-status status-{'excellent' if quality_score >= 80 else ('good' if quality_score >= 60 else 'fair')}">
                    {'Excellent' if quality_score >= 80 else ('Good' if quality_score >= 60 else 'Needs Improvement')}
                </div>
            </div>
            
            <div class="kpi-card">
                <div class="kpi-icon" style="color: {risk_colors.get(business_impact, '#ffc107')};">
                    <i class="fas fa-exclamation-triangle"></i>
                </div>
                <div class="kpi-value" style="color: {risk_colors.get(business_impact, '#ffc107')};">{business_impact}</div>
                <div class="kpi-label">Business Risk</div>
                <div class="kpi-status status-{business_impact.lower()}">{business_impact} Impact</div>
            </div>
            
            <div class="kpi-card">
                <div class="kpi-icon" style="color: {risk_colors.get(security_risk, '#ffc107')};">
                    <i class="fas fa-shield-alt"></i>
                </div>
                <div class="kpi-value" style="color: {risk_colors.get(security_risk, '#ffc107')};">{security_risk}</div>
                <div class="kpi-label">Security Risk</div>
                <div class="kpi-status status-{security_risk.lower()}">{security_risk} Risk</div>
            </div>
            
            <div class="kpi-card">
                <div class="kpi-icon" style="color: {risk_colors.get(tech_debt, '#ffc107')};">
                    <i class="fas fa-tools"></i>
                </div>
                <div class="kpi-value" style="color: {risk_colors.get(tech_debt, '#ffc107')};">{maintainability}</div>
                <div class="kpi-label">Maintainability</div>
                <div class="kpi-status status-{maintainability.lower()}">{maintainability}</div>
            </div>
        </div>
        
        <div class="executive-summary">
            <h2 class="summary-title">
                <i class="fas fa-clipboard-list"></i>
                Executive Summary
            </h2>
            <div class="summary-text">{summary}</div>
        </div>
        
        <div class="findings-section">
            <h2 class="section-title">
                <i class="fas fa-search"></i>
                Detailed Technical Findings
            </h2>
            {findings_html}
        </div>
        
        <div class="footer">
            <div class="footer-content">
                <div class="footer-left">
                    <i class="fas fa-brain"></i>
                    <strong>Powered by Snowflake Cortex AI</strong>
                    <span style="opacity: 0.8;">| Executive Technical Analysis</span>
                </div>
                <div class="footer-right">
                    Report ID: {datetime.now().strftime('%Y%m%d_%H%M%S')} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
                </div>
            </div>
        </div>
    </div>
</body>
</html>"""
    
    return html_content

def filter_low_severity(json_response: dict) -> dict:
    filtered = json_response.copy()
    if "detailed_findings" in filtered:
        all_findings = filtered["detailed_findings"]
        sorted_findings = sorted(all_findings, key=lambda x: (
            x.get("priority_ranking", 999),
            {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}.get(x.get("severity", "LOW"), 4)
        ))
        filtered["detailed_findings"] = sorted_findings[:20]
    return filtered

def extract_critical_findings(json_response: dict) -> list:
    findings = []
    for f in json_response.get("detailed_findings", []):
        if f.get("severity", "").upper() == "CRITICAL" and f.get("line_number"):
            findings.append({
                "line": int(f["line_number"]) if str(f["line_number"]).isdigit() else 1,
                "issue": f.get("finding", ""),
                "recommendation": f.get("recommendation", ""),
                "severity": "CRITICAL",
                "business_impact": f.get("business_impact", ""),
                "category": f.get("category", "General")
            })
    return findings

if __name__ == "__main__":
    try:
        print("🚀 Starting Executive Code Review Analysis...")
        print("="*60)
        
        if not os.path.exists(FILE_TO_REVIEW):
            print(f"❌ File {FILE_TO_REVIEW} not found")
            exit(1)
            
        print(f"📖 Reading file: {FILE_TO_REVIEW}")
        code_text = Path(FILE_TO_REVIEW).read_text()
        print(f"📝 Code length: {len(code_text)} characters ({len(code_text.split())} lines)")
        
        report = review_with_cortex(MODEL, code_text)
        print(f"📋 Analysis completed - Report sections: {list(report.keys())}")
        
        original_findings = report.get("detailed_findings", [])
        quality_score = report.get("quality_score", 75)
        print(f"🎯 Quality Score: {quality_score}/100")
        print(f"🔍 Found {len(original_findings)} total findings")
        
        filtered = filter_low_severity(report)
        filtered_findings = filtered.get("detailed_findings", [])
        print(f"📊 Executive summary: {len(filtered_findings)} key findings")
        
        criticals = extract_critical_findings(filtered)
        print(f"🚨 Critical issues requiring immediate attention: {len(criticals)}")

        print("\n📄 Generating executive reports...")
        formatted_review = format_executive_pr_display(filtered)
        html_report = generate_executive_html_report(filtered)
        
        print("💾 Saving report files...")
        with open("executive_code_review_report.html", "w", encoding='utf-8') as f: 
            f.write(html_report)
        
        output_data = {
            "full_review": formatted_review,              
            "full_review_markdown": formatted_review,     
            "full_review_json": filtered,                 
            "criticals": criticals,
            "executive_summary": {
                "quality_score": quality_score,
                "business_impact": report.get("business_impact", "MEDIUM"),
                "security_risk_level": report.get("security_risk_level", "MEDIUM"),
                "total_findings": len(filtered_findings),
                "critical_count": len(criticals)
            },
            "file": FILE_TO_REVIEW,
            "interactive_report_path": "executive_code_review_report.html",
            "timestamp": datetime.now().isoformat(),
            "report_version": "Executive_v2.0"
        }
        
        with open("executive_review_output.json", "w", encoding='utf-8') as f: 
            json.dump(output_data, f, indent=2, ensure_ascii=False)
            
        with open("review_output.json", "w", encoding='utf-8') as f: 
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print("\n" + "="*70)
        print("✅ EXECUTIVE CODE REVIEW COMPLETED")
        print("="*70)
        print(f"📁 File Analyzed: {FILE_TO_REVIEW}")
        print(f"🎯 Overall Quality Score: {quality_score}/100")
        print(f"📊 Business Risk Level: {report.get('business_impact', 'MEDIUM')}")
        print(f"🔒 Security Risk: {report.get('security_risk_level', 'MEDIUM')}")
        print(f"🔧 Maintainability: {report.get('maintainability_rating', 'FAIR')}")
        
        if filtered_findings:
            critical_count = sum(1 for f in filtered_findings if f.get("severity", "").upper() == "CRITICAL")
            high_count = sum(1 for f in filtered_findings if f.get("severity", "").upper() == "HIGH") 
            medium_count = sum(1 for f in filtered_findings if f.get("severity", "").upper() == "MEDIUM")
            
            print(f"\n📈 Issue Distribution:")
            print(f"  🔴 Critical: {critical_count} (Immediate action required)")
            print(f"  🟠 High: {high_count} (Fix within sprint)")
            print(f"  🟡 Medium: {medium_count} (Plan for next release)")
        
        print(f"\n📋 Reports Generated:")
        print(f"  • review_output.json (inline_comment.py compatibility)")
        print(f"  • executive_review_output.json (enhanced executive data)")
        print(f"  • executive_code_review_report.html (interactive dashboard)")
        
        print(f"\n🌐 Next Steps:")
        print(f"  1. Open executive_code_review_report.html for detailed analysis")
        print(f"  2. Review immediate actions for critical issues")
        print(f"  3. Share executive summary with technical leadership")
        
        if len(criticals) > 0:
            print(f"\n⚠️  ATTENTION: {len(criticals)} critical issues require immediate attention!")
        else:
            print(f"\n✅ No critical issues found - excellent code quality!")
        
    except Exception as e:
        print(f"❌ Executive analysis error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'session' in locals():
            session.close()
            print("\n🔒 Analysis session completed")
