import os, sys, json, re, uuid, glob
from pathlib import Path
from snowflake.snowpark import Session
import pandas as pd
from datetime import datetime

# ---------------------
# Config
# ---------------------
MODEL = "openai-gpt-4.1"
MAX_CHARS_FOR_FINAL_SUMMARY_FILE = 65000
MAX_TOKENS_FOR_SUMMARY_INPUT = 100000

# Dynamic file pattern - processes all Python files in scripts directory
SCRIPTS_DIRECTORY = "scripts"  # Base directory to scan
FILE_PATTERN = "*.py"  # Pattern to match Python files

# ---------------------
# Snowflake session
# ---------------------
cfg = {
    "account": "XKB93357.us-west-2",
    "user": "MANISHAT007", 
    "password": "Welcome@987654321",
    "role": "SYSADMIN",  # ONLY CHANGE: from ORGADMIN to SYSADMIN
    "warehouse": "COMPUTE_WH",
    "database": "MY_DB",
    "schema": "PUBLIC",
}
session = Session.builder.configs(cfg).create()

# FIX DATABASE PERMISSIONS: Try different approaches
database_available = False
try:
    session.sql("USE ROLE SYSADMIN").collect()
    session.sql("USE DATABASE MY_DB").collect()
    session.sql("USE SCHEMA PUBLIC").collect()
    print("✅ Database context set: MY_DB.PUBLIC with SYSADMIN")
    database_available = True
except Exception as e:
    print(f"⚠️ Warning: SYSADMIN failed: {e}")
    # Try creating our own schema with full permissions
    try:
        session.sql("USE ROLE SYSADMIN").collect()
        session.sql("CREATE DATABASE IF NOT EXISTS REVIEW_DB").collect()
        session.sql("USE DATABASE REVIEW_DB").collect()
        session.sql("CREATE SCHEMA IF NOT EXISTS REVIEWS").collect()
        session.sql("USE SCHEMA REVIEWS").collect()
        print("✅ Created and using REVIEW_DB.REVIEWS")
        database_available = True
    except Exception as e2:
        print(f"⚠️ Warning: Schema creation failed: {e2}")
        print("⚠️ Continuing without database logging - previous reviews won't work")
        database_available = False

# ---------------------
# PROMPT TEMPLATES
# ---------------------
PROMPT_TEMPLATE_INDIVIDUAL = """Please act as a principal-level Python code reviewer. Your review must be concise, accurate, and directly actionable, as it will be posted as a GitHub Pull Request comment.

---
# CONTEXT: HOW TO REVIEW (Apply Silently)

1.  **You are reviewing a code file for executive-level analysis.** Focus on business impact, technical debt, security risks, and maintainability.
2.  **Focus your review on the most critical aspects.** Prioritize findings that have business impact or security implications.
3.  **Infer context from the full code.** Base your review on the complete file provided.
4.  **Your entire response MUST be under 65,000 characters.** Include findings of all severities but prioritize Critical and High severity issues.

# REVIEW PRIORITIES (Strict Order)
1.  Security & Correctness
2.  Reliability & Error-handling
3.  Performance & Complexity
4.  Readability & Maintainability
5.  Testability

# SEVERITY GUIDELINES (Be Realistic and Balanced - MOST ISSUES SHOULD BE MEDIUM OR LOW)
-   **Critical:** ONLY for security vulnerabilities, data loss risks, system crashes, production outages
-   **High:** ONLY for significant error handling gaps, major performance bottlenecks, security concerns
-   **Medium:** Code quality improvements, minor performance issues, maintainability concerns, documentation gaps
-   **Low:** Style improvements, minor optimizations, non-critical suggestions, cosmetic issues

# REALISTIC SEVERITY DISTRIBUTION (MANDATORY):
- Critical: 0-5% of findings (very rare)
- High: 10-20% of findings 
- Medium: 40-50% of findings (most common)
- Low: 30-40% of findings (common)

# ELIGIBILITY CRITERIA FOR FINDINGS (ALL must be met)
-   **Evidence:** Quote the exact code snippet and cite the line number.
-   **Severity:** Assign {Low | Medium | High | Critical} - BE REALISTIC, most issues should be Medium or Low.
-   **Impact & Action:** Briefly explain the issue and provide a minimal, safe correction.
-   **Non-trivial:** Skip purely stylistic nits (e.g., import order, line length) that a linter would catch.

# HARD CONSTRAINTS (For accuracy & anti-hallucination)
-   Do NOT propose APIs that don't exist for the imported modules.
-   Treat parameters like `db_path` as correct dependency injection; do NOT call them hardcoded.
-   NEVER suggest logging sensitive user data or internal paths. Suggest non-reversible fingerprints if context is needed.
-   Do NOT recommend removing correct type hints or docstrings.
-   If code in the file is already correct and idiomatic, do NOT invent problems.
-   DO NOT inflate severity levels - be conservative and realistic.

---
# OUTPUT FORMAT (Strict, professional, audit-ready)

Your entire response MUST be under 65,000 characters. Include findings of all severity levels with realistic severity assignments.

## Code Review Summary
*A 2-3 sentence high-level summary. Mention the key strengths and the most critical areas for improvement.*

---
### Detailed Findings
*A list of all material findings. If no significant issues are found, state "No significant issues found."*

**File:** {filename}
-   **Severity:** {Critical | High | Medium | Low}
-   **Line:** {line_number}
-   **Function/Context:** `{function_name_if_applicable}`
-   **Finding:** {A clear, concise description of the issue, its impact, and a recommended correction.}

**(Repeat for each finding)**

---
### Key Recommendations
*Provide 2-3 high-level, actionable recommendations for improving the overall quality of the codebase based on the findings. Do not repeat the findings themselves.*

---
# CODE TO REVIEW

{PY_CONTENT}
"""

PROMPT_TEMPLATE_CONSOLIDATED = """
You are an expert code review summarization engine for executive-level reporting. Your task is to analyze individual code reviews and generate a single, consolidated executive summary with business impact focus.

You MUST respond ONLY with a valid JSON object that conforms to the executive schema. Do not include any other text, explanations, or markdown formatting outside of the JSON structure.

Follow these instructions to populate the JSON fields:

1.  **`executive_summary` (string):** Write a 2-3 sentence high-level summary of the entire code change, covering the most important findings across all files with business impact focus.
2.  **`quality_score` (number):** Assign an overall quality score (0-100) based on severity and number of findings.
3.  **`business_impact` (string):** Assess overall business risk as "LOW", "MEDIUM", or "HIGH".
4.  **`technical_debt_score` (string):** Evaluate technical debt as "LOW", "MEDIUM", or "HIGH".
5.  **`security_risk_level` (string):** Determine security risk as "LOW", "MEDIUM", "HIGH", or "CRITICAL".
6.  **`maintainability_rating` (string):** Rate maintainability as "POOR", "FAIR", "GOOD", or "EXCELLENT".
7.  **`detailed_findings` (array of objects):** Create an array of objects, where each object represents a single, distinct issue found in the code:
         -   **`severity`**: Assign severity realistically: "Low", "Medium", "High", or "Critical". MOST ISSUES SHOULD BE Medium or Low. Only use Critical for security vulnerabilities or data loss risks. Only use High for significant errors or performance issues.
         -   **`category`**: Assign category: "Security", "Performance", "Maintainability", "Best Practices", "Documentation", or "Error Handling".
         -   **`line_number`**: Extract the specific line number if mentioned in the review. If no line number is available, use "N/A".
         -   **`function_context`**: From the review text, identify the function or class name where the issue is located. If not applicable, use "global scope".
         -   **`finding`**: Write a clear, concise description of the issue, its potential impact, and a concrete recommendation.
         -   **`business_impact`**: Explain how this affects business operations or risk.
         -   **`recommendation`**: Provide specific technical solution.
         -   **`effort_estimate`**: Estimate effort as "LOW", "MEDIUM", or "HIGH".
         -   **`priority_ranking`**: Assign priority ranking (1 = highest priority).
         -   **`filename`**: The name of the file where the issue was found.
8.  **`metrics` (object):** Include technical metrics:
         -   **`lines_of_code`**: Total number of lines analyzed across all files.
         -   **`complexity_score`**: "LOW", "MEDIUM", or "HIGH".
         -   **`code_coverage_gaps`**: Array of areas needing test coverage.
         -   **`dependency_risks`**: Array of dependency-related risks.
9.  **`strategic_recommendations` (array of strings):** Provide 2-3 high-level, actionable recommendations for technical leadership.
10. **`immediate_actions` (array of strings):** List critical items requiring immediate attention.
11. **`previous_issues_resolved` (array of objects):** For each issue from previous review, indicate status:
         -   **`original_issue`**: Brief description of the previous issue
         -   **`status`**: "RESOLVED", "PARTIALLY_RESOLVED", "NOT_ADDRESSED", or "WORSENED"
         -   **`details`**: Explanation of current status

**CRITICAL INSTRUCTION FOR BALANCED REVIEWS:**
Your entire response MUST be under {MAX_CHARS_FOR_FINAL_SUMMARY_FILE} characters. Include findings of all severity levels with realistic severity assignments:
-   Use "Critical" only for security vulnerabilities, data loss risks, or system crashes
-   Use "High" only for significant error handling gaps or major performance issues  
-   Use "Medium" for code quality improvements and minor performance issues
-   Use "Low" for style improvements and non-critical suggestions
-   REALISTIC DISTRIBUTION: Expect mostly Medium (40-50%) and Low (30-40%) severity findings, with fewer High (10-20%) and very few Critical (0-5%)

Here are the individual code reviews to process:
{ALL_REVIEWS_CONTENT}
"""

PROMPT_TEMPLATE_WITH_CONTEXT = """
You are reviewing subsequent commits for Pull Request #{pr_number}. 

PREVIOUS REVIEW SUMMARY AND FINDINGS:
{previous_context}

CRITICAL INSTRUCTION: You must analyze the new code changes with full awareness of the previous feedback. Specifically:
1. Check if previous Critical/High severity issues were addressed in the new code
2. Identify if any previous recommendations were implemented
3. Note any new issues that may have been introduced
4. Maintain continuity with previous review comments
5. In the "previous_issues_resolved" section, provide specific status for each previous issue

{consolidated_template}
"""

def get_changed_python_files(folder_path=None):
    """
    Dynamically get all Python files from the specified folder or scripts directory.
    Uses wildcard pattern matching for flexibility.
    """
    # If no folder specified, use the scripts directory
    if not folder_path:
        folder_path = SCRIPTS_DIRECTORY
        
    if not os.path.exists(folder_path):
        print(f"❌ Directory {folder_path} not found")
        return []
    
    # Use glob pattern to find all Python files
    pattern = os.path.join(folder_path, FILE_PATTERN)
    py_files = glob.glob(pattern)
    
    # Also check subdirectories recursively
    recursive_pattern = os.path.join(folder_path, "**", FILE_PATTERN)
    py_files.extend(glob.glob(recursive_pattern, recursive=True))
    
    # Remove duplicates and sort
    py_files = sorted(list(set(py_files)))
    
    print(f"📁 Found {len(py_files)} Python files in {folder_path} using pattern '{FILE_PATTERN}':")
    for file in py_files:
        print(f"  - {file}")
    
    return py_files

def build_prompt_for_individual_review(code_text: str, filename: str = "code_file") -> str:
    prompt = PROMPT_TEMPLATE_INDIVIDUAL.replace("{PY_CONTENT}", code_text)
    prompt = prompt.replace("{filename}", filename)
    return prompt

def build_prompt_for_consolidated_summary(all_reviews_content: str, previous_context: str = None, pr_number: int = None) -> str:
    if previous_context and pr_number:
        prompt = PROMPT_TEMPLATE_WITH_CONTEXT.replace("{previous_context}", previous_context)
        prompt = prompt.replace("{pr_number}", str(pr_number))
        prompt = prompt.replace("{consolidated_template}", PROMPT_TEMPLATE_CONSOLIDATED)
        prompt = prompt.replace("{ALL_REVIEWS_CONTENT}", all_reviews_content)
    else:
        prompt = PROMPT_TEMPLATE_CONSOLIDATED.replace("{ALL_REVIEWS_CONTENT}", all_reviews_content)
    return prompt

def review_with_cortex(model, prompt_text: str, session) -> str:
    try:
        clean_prompt = prompt_text.replace("'", "''").replace("\\", "\\\\")
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{clean_prompt}') as response"
        df = session.sql(query)
        result = df.collect()[0][0]
        return result
    except Exception as e:
        print(f"Error calling Cortex complete for model '{model}': {e}", file=sys.stderr)
        return f"ERROR: Could not get response from Cortex. Reason: {e}"

def chunk_large_file(code_text: str, max_chunk_size: int = 50000) -> list:
    if len(code_text) <= max_chunk_size:
        return [code_text]
    
    lines = code_text.split('\n')
    chunks = []
    current_chunk = []
    current_size = 0
    
    for line in lines:
        line_size = len(line) + 1
        if current_size + line_size > max_chunk_size and current_chunk:
            chunks.append('\n'.join(current_chunk))
            current_chunk = [line]
            current_size = line_size
        else:
            current_chunk.append(line)
            current_size += line_size
    
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    
    return chunks

def calculate_executive_quality_score(findings: list, total_lines_of_code: int) -> int:
    """
    Executive-level rule-based quality scoring (0-100).
    MUCH MORE BALANCED - Fixed overly harsh scoring.
    
    Scoring Logic (REALISTIC):
    - Start with base score of 100
    - Reasonable deductions that won't hit zero easily
    - Focus on actionable scoring for executives
    """
    if not findings or len(findings) == 0:
        return 100
    
    base_score = 100
    total_deductions = 0
    
    # MUCH MORE BALANCED severity weightings
    severity_weights = {
        "Critical": 8,     # Each critical issue deducts 8 points (was 15)
        "High": 3,         # Each high issue deducts 3 points (was 6)
        "Medium": 1.5,     # Each medium issue deducts 1.5 points (was 3)
        "Low": 0.5         # Each low issue deducts 0.5 points (was 1)
    }
    
    # Count issues by severity - STRICT PRECISION (NO CONVERSION)
    severity_counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    total_affected_lines = 0
    
    print(f"  📊 Scoring {len(findings)} findings...")
    
    for finding in findings:
        severity = str(finding.get("severity", "")).strip()  # Keep original case
        
        # STRICT MATCHING - NO CONVERSION TO MEDIUM
        if severity == "Critical":
            severity_counts["Critical"] += 1
        elif severity == "High":
            severity_counts["High"] += 1
        elif severity == "Medium":
            severity_counts["Medium"] += 1
        elif severity == "Low":
            severity_counts["Low"] += 1
        else:
            # LOG UNRECOGNIZED SEVERITY BUT DON'T COUNT IT
            print(f"    ⚠️ UNRECOGNIZED SEVERITY: '{severity}' in finding: {finding.get('finding', 'Unknown')[:50]}... - SKIPPING")
            continue  # Skip this finding entirely instead of converting
            
        print(f"    - {severity}: {finding.get('finding', 'No description')[:50]}...")
        
        # Count affected lines (treat N/A as 1 line)
        line_num = finding.get("line_number", "N/A")
        total_affected_lines += 1
    
    print(f"  📈 Severity breakdown: Critical={severity_counts['Critical']}, High={severity_counts['High']}, Medium={severity_counts['Medium']}, Low={severity_counts['Low']}")
    
    # Calculate REALISTIC deductions from severity
    for severity, count in severity_counts.items():
        if count > 0:
            weight = severity_weights[severity]
            
            # MUCH MORE BALANCED progressive penalty
            if severity == "Critical":
                # Critical: 8, 12, 16, 20 for 1,2,3,4 issues (much more reasonable)
                if count <= 3:
                    deduction = weight * count
                else:
                    deduction = weight * 3 + (count - 3) * (weight + 2)
                # Cap critical deductions at 25 points max (was 50)
                deduction = min(25, deduction)
            elif severity == "High":
                # High: Linear scaling with small bonus after 8 issues
                if count <= 8:
                    deduction = weight * count
                else:
                    deduction = weight * 8 + (count - 8) * (weight + 1)
                # Cap high severity deductions at 20 points max (was 40)
                deduction = min(20, deduction)
            else:
                # Medium/Low: Pure linear scaling with caps
                deduction = weight * count
                # Much lower caps
                if severity == "Medium":
                    deduction = min(15, deduction)  # Was 20
                else:
                    deduction = min(8, deduction)   # Was 10
                
            total_deductions += deduction
            print(f"    {severity}: {count} issues = -{deduction:.1f} points (capped)")
    
    # MUCH REDUCED penalties
    if total_lines_of_code > 0:
        affected_ratio = total_affected_lines / total_lines_of_code
        if affected_ratio > 0.3:  # Only penalize if more than 30% affected (was 20%)
            coverage_penalty = min(5, int(affected_ratio * 25))  # Max 5 point penalty (was 10)
            total_deductions += coverage_penalty
            print(f"    Coverage penalty: -{coverage_penalty} points ({affected_ratio:.1%} affected)")
    
    # MUCH REDUCED critical threshold penalties
    if severity_counts["Critical"] >= 10:  # Raised threshold from 5 to 10
        total_deductions += 8  # Reduced from 15 to 8
        print(f"    Executive threshold penalty: -8 points (10+ critical issues)")
    
    if severity_counts["Critical"] + severity_counts["High"] >= 25:  # Raised from 15 to 25
        total_deductions += 5  # Reduced from 10 to 5
        print(f"    Production readiness penalty: -5 points (25+ critical/high issues)")
    
    # Calculate final score
    final_score = max(0, base_score - int(total_deductions))
    
    print(f"  🎯 Final calculation: {base_score} - {int(total_deductions)} = {final_score}")
    
    # ADJUSTED executive score bands for more realistic scoring
    if final_score >= 85:
        return min(100, final_score)  # Excellent
    elif final_score >= 70:  # Lowered from 65
        return final_score  # Good
    elif final_score >= 50:  # Lowered from 40
        return final_score  # Fair - needs attention
    else:
        return max(25, final_score)  # Poor - but never below 25 for functional code

def format_executive_pr_display(json_response: dict, processed_files: list) -> str:
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
    previous_issues = json_response.get("previous_issues_resolved", [])
    
    critical_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "CRITICAL")
    high_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "HIGH")
    medium_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "MEDIUM")
    
    risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
    quality_emoji = "🟢" if quality_score >= 80 else ("🟡" if quality_score >= 60 else "🔴")
    
    display_text = f"""# 📊 Executive Code Review Report

**Files Analyzed:** {len(processed_files)} files | **Analysis Date:** {datetime.now().strftime('%Y-%m-%d')}

## 🎯 Executive Summary
{summary}

## 📈 Quality Dashboard

| Metric | Score | Status | Business Impact |
|--------|-------|--------|-----------------|
| **Overall Quality** | {quality_score}/100 | {quality_emoji} | {business_impact} Risk |
| **Security Risk** | {security_risk} | {risk_emoji.get(security_risk, "🟡")} | Critical security concerns |
| **Technical Debt** | {tech_debt} | {risk_emoji.get(tech_debt, "🟡")} | {len(findings)} items |
| **Maintainability** | {maintainability} | {risk_emoji.get(maintainability, "🟡")} | Long-term sustainability |

## 🔍 Issue Distribution

| Severity | Count | Priority Actions |
|----------|-------|------------------|
| 🔴 Critical | {critical_count} | Immediate fix required |
| 🟠 High | {high_count} | Fix within sprint |
| 🟡 Medium | {medium_count} | Plan for next release |

"""

    # Add Critical Issues Summary section if there are critical issues
    critical_findings = [f for f in findings if str(f.get("severity", "")).upper() == "CRITICAL"]
    if critical_findings:
        display_text += """## 🚨 Critical Issues Summary

**⚠️ IMMEDIATE ACTION REQUIRED** - The following critical issues must be addressed before deployment:

"""
        for i, finding in enumerate(critical_findings, 1):
            line_num = finding.get("line_number", "N/A")
            filename = finding.get("filename", "N/A")
            issue_desc = finding.get("finding", "No description available")
            business_impact = finding.get("business_impact", "No business impact specified")
            recommendation = finding.get("recommendation", finding.get("finding", "No recommendation available"))
            
            display_text += f"""**{i}. Critical Issue - Line {line_num}**
- **File:** {filename}
- **Issue:** {issue_desc}
- **Business Impact:** {business_impact}
- **Required Action:** {recommendation}

"""
        display_text += """---

"""

    # NO TRUNCATION - show full text for previous issues
    if previous_issues:
        display_text += """<details>
<summary><strong>📈 Previous Issues Resolution Status</strong> (Click to expand)</summary>

| Previous Issue | Status | Details |
|----------------|--------|---------|
"""
        for issue in previous_issues:
            status = issue.get("status", "UNKNOWN")
            status_emoji = {"RESOLVED": "✅", "PARTIALLY_RESOLVED": "⚠️", "NOT_ADDRESSED": "❌", "WORSENED": "🔴"}.get(status, "❓")
            
            # NO TRUNCATION - show full text
            original_display = issue.get("original_issue", "")
            details_display = issue.get("details", "")
            
            display_text += f"| {original_display} | {status_emoji} {status} | {details_display} |\n"
        
        display_text += "\n</details>\n\n"

    if findings:
        display_text += """<details>
<summary><strong>🔍 Current Review Findings</strong> (Click to expand)</summary>

| Priority | File | Line | Issue | Business Impact |
|----------|------|------|-------|-----------------|
"""
        
        severity_order = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
        sorted_findings = sorted(findings, key=lambda x: severity_order.get(str(x.get("severity", "Low")), 4))
        
        for finding in sorted_findings[:15]:
            severity = str(finding.get("severity", "Medium"))
            filename = finding.get("filename", "N/A")
            line = finding.get("line_number", "N/A")
            
            # NO TRUNCATION - show full text
            issue_display = str(finding.get("finding", ""))
            business_impact_display = str(finding.get("business_impact", ""))
            
            priority_emoji = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}.get(severity, "🟡")
            
            display_text += f"| {priority_emoji} {severity} | {filename} | {line} | {issue_display} | {business_impact_display} |\n"
        
        display_text += "\n</details>\n\n"

    if strategic_recs:
        display_text += """<details>
<summary><strong>🎯 Strategic Recommendations</strong> (Click to expand)</summary>

"""
        for i, rec in enumerate(strategic_recs, 1):
            display_text += f"{i}. {rec}\n"
        display_text += "\n</details>\n\n"

    if immediate_actions:
        display_text += """<details>
<summary><strong>⚡ Immediate Actions Required</strong> (Click to expand)</summary>

"""
        for i, action in enumerate(immediate_actions, 1):
            display_text += f"{i}. {action}\n"
        display_text += "\n</details>\n\n"

    display_text += f"""---

**📋 Review Summary:** {len(findings)} findings identified | **🎯 Quality Score:** {quality_score}/100 | **⚡ Critical Issues:** {critical_count}

*🔬 Powered by Snowflake Cortex AI • Two-Stage Executive Analysis*"""

    return display_text

def main():
    if len(sys.argv) >= 5:
        output_folder_path = sys.argv[2]  # Keep output folder from args
        try:
            pull_request_number = int(sys.argv[3]) if sys.argv[3] and sys.argv[3].strip() else None
        except (ValueError, IndexError):
            print(f"⚠️  Warning: Invalid or empty PR number '{sys.argv[3] if len(sys.argv) > 3 else 'None'}', using None")
            pull_request_number = None
        commit_sha = sys.argv[4]
        directory_mode = True
        
        # ALWAYS use scripts directory regardless of first argument
        print(f"📁 Command line mode: Using {SCRIPTS_DIRECTORY} directory instead of '{sys.argv[1]}'")
        python_files = get_changed_python_files(SCRIPTS_DIRECTORY)
        if not python_files:
            print(f"❌ No Python files found in {SCRIPTS_DIRECTORY} directory using pattern {FILE_PATTERN}")
            return
            
        folder_path = SCRIPTS_DIRECTORY  # Always use scripts directory
            
    else:
        # Fallback for single file mode - use scripts directory with wildcard pattern
        python_files = get_changed_python_files(SCRIPTS_DIRECTORY)
        if not python_files:
            print(f"❌ No Python files found in {SCRIPTS_DIRECTORY} directory using pattern {FILE_PATTERN}")
            return
            
        folder_path = SCRIPTS_DIRECTORY
        output_folder_path = "output_reviews"
        pull_request_number = 0
        commit_sha = "test"
        directory_mode = False
        print(f"Running in dynamic pattern mode with {len(python_files)} Python files from {SCRIPTS_DIRECTORY}")

    if os.path.exists(output_folder_path):
        import shutil
        shutil.rmtree(output_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)

    all_individual_reviews = []
    processed_files = []

    print("\n🔍 STAGE 1: Individual File Analysis...")
    print("=" * 60)
    
    for file_path in python_files:
        filename = os.path.basename(file_path)
        print(f"\n--- Reviewing file: {filename} ---")
        processed_files.append(filename)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code_content = f.read()

            if not code_content.strip():
                review_text = "No code found in file, skipping review."
            else:
                chunks = chunk_large_file(code_content)
                print(f"  File split into {len(chunks)} chunk(s)")
                
                chunk_reviews = []
                for i, chunk in enumerate(chunks):
                    chunk_name = f"{filename}_chunk_{i+1}" if len(chunks) > 1 else filename
                    print(f"  Processing chunk: {chunk_name}")
                    
                    individual_prompt = build_prompt_for_individual_review(chunk, chunk_name)
                    review_text = review_with_cortex(MODEL, individual_prompt, session)
                    chunk_reviews.append(review_text)
                
                if len(chunk_reviews) > 1:
                    review_text = "\n\n".join([f"## Chunk {i+1}\n{review}" for i, review in enumerate(chunk_reviews)])
                else:
                    review_text = chunk_reviews[0]

            all_individual_reviews.append({
                "filename": filename,
                "review_feedback": review_text
            })

            output_filename = f"{Path(filename).stem}_individual_review.md"
            output_file_path = os.path.join(output_folder_path, output_filename)
            with open(output_file_path, 'w', encoding='utf-8') as outfile:
                outfile.write(review_text)
            print(f"  ✅ Individual review saved: {output_filename}")

        except Exception as e:
            print(f"  ❌ Error processing {filename}: {e}")
            all_individual_reviews.append({
                "filename": filename,
                "review_feedback": f"ERROR: Could not generate review. Reason: {e}"
            })

    print(f"\n🔄 STAGE 2: Executive Consolidation...")
    print("=" * 60)
    print(f"Consolidating {len(all_individual_reviews)} individual reviews...")

    if not all_individual_reviews:
        print("❌ No reviews to consolidate")
        return

    try:
        # CRITICAL: Retrieve previous review context BEFORE generating new review
        previous_review_context = None
        if pull_request_number and pull_request_number != 0 and database_available:
            try:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS CODE_REVIEW_LOG (
                    REVIEW_ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                    PULL_REQUEST_NUMBER INTEGER,
                    COMMIT_SHA VARCHAR(40),
                    REVIEW_SUMMARY TEXT,
                    DETAILED_FINDINGS TEXT,
                    REVIEW_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                """
                session.sql(create_table_query).collect()
                
                query = f"""
                    SELECT REVIEW_SUMMARY, DETAILED_FINDINGS FROM CODE_REVIEW_LOG 
                    WHERE PULL_REQUEST_NUMBER = {pull_request_number}
                    ORDER BY REVIEW_TIMESTAMP DESC 
                    LIMIT 1
                """
                result = session.sql(query).collect()
                
                if result:
                    previous_review_context = result[0]["REVIEW_SUMMARY"][:3000]  # Truncate for prompt
                    print("  📋 Retrieved previous review context - this is a subsequent commit review")
                else:
                    print("  📋 No previous review found - this is the initial commit review")
                    
            except Exception as e:
                print(f"  Warning: Could not retrieve previous review: {e}")
        elif not database_available:
            print("  ⚠️ Database not available - cannot retrieve previous reviews")

        combined_reviews_json = json.dumps(all_individual_reviews, indent=2)
        print(f"  Combined reviews: {len(combined_reviews_json)} characters")

        # Generate consolidation prompt with or without previous context
        consolidation_prompt = build_prompt_for_consolidated_summary(
            combined_reviews_json, 
            previous_review_context, 
            pull_request_number
        )
        consolidation_prompt = consolidation_prompt.replace("{MAX_CHARS_FOR_FINAL_SUMMARY_FILE}", str(MAX_CHARS_FOR_FINAL_SUMMARY_FILE))
        consolidated_raw = review_with_cortex(MODEL, consolidation_prompt, session)
        
        try:
            consolidated_json = json.loads(consolidated_raw)
            print("  ✅ Successfully parsed consolidated JSON response")
            
            # OVERRIDE: Calculate rule-based quality score (don't trust LLM for this)
            findings = consolidated_json.get("detailed_findings", [])
            total_lines = sum(len(review.get("review_feedback", "").split('\n')) for review in all_individual_reviews)
            
            rule_based_score = calculate_executive_quality_score(findings, total_lines)
            consolidated_json["quality_score"] = rule_based_score
            
            print(f"  🎯 Rule-based quality score calculated: {rule_based_score}/100 (overriding LLM score)")
            
        except json.JSONDecodeError as e:
            print(f"  ⚠️ JSON parsing failed: {e}")
            json_match = re.search(r'\{.*\}', consolidated_raw, re.DOTALL)
            if json_match:
                consolidated_json = json.loads(json_match.group())
            else:
                consolidated_json = {
                    "executive_summary": "Consolidation failed - using fallback",
                    "quality_score": 75,
                    "business_impact": "MEDIUM",
                    "detailed_findings": [],
                    "strategic_recommendations": [],
                    "immediate_actions": [],
                    "previous_issues_resolved": []
                }

        executive_summary = format_executive_pr_display(consolidated_json, processed_files)
        
        consolidated_path = os.path.join(output_folder_path, "consolidated_executive_summary.md")
        with open(consolidated_path, 'w', encoding='utf-8') as f:
            f.write(executive_summary)
        print(f"  ✅ Executive summary saved: consolidated_executive_summary.md")

        json_path = os.path.join(output_folder_path, "consolidated_data.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(consolidated_json, f, indent=2)

        # Generate review_output.json for inline_comment.py compatibility
        criticals = []
        critical_findings = [f for f in consolidated_json.get("detailed_findings", []) if str(f.get("severity", "")).upper() == "CRITICAL"]
        
        for f in critical_findings:
            critical = {
                "line": f.get("line_number", "N/A"),
                "issue": f.get("finding", "Critical issue found"),
                "recommendation": f.get("recommendation", f.get("finding", "")),
                "severity": f.get("severity", "Critical"),
                "filename": f.get("filename", "N/A"),
                "business_impact": f.get("business_impact", "No business impact specified"),
                "description": f.get("finding", "Critical issue found")  # Add explicit description field
            }
            criticals.append(critical)

        # Create a proper critical issues summary for inline_comment.py
        critical_summary = ""
        if critical_findings:
            critical_summary = "Critical Issues Summary:\n"
            for i, finding in enumerate(critical_findings, 1):
                line_num = finding.get("line_number", "N/A")
                issue_desc = finding.get("finding", "Critical issue found")
                critical_summary += f"* **Line {line_num}:** {issue_desc}\n"

        review_output_data = {
            "full_review": executive_summary,
            "full_review_markdown": executive_summary,
            "full_review_json": consolidated_json,
            "criticals": criticals,
            "critical_summary": critical_summary,  # Add explicit critical summary
            "critical_count": len(critical_findings),
            "file": processed_files[0] if processed_files else "unknown",
            "timestamp": datetime.now().isoformat()
        }

        with open("review_output.json", "w", encoding='utf-8') as f:
            json.dump(review_output_data, f, indent=2, ensure_ascii=False)
        print("  ✅ review_output.json saved for inline_comment.py compatibility")

        # Store current review for future comparisons - FIXED SQL
        if pull_request_number and pull_request_number != 0 and database_available:
            try:
                insert_sql = """
                    INSERT INTO CODE_REVIEW_LOG (PULL_REQUEST_NUMBER, COMMIT_SHA, REVIEW_SUMMARY, DETAILED_FINDINGS)
                    VALUES (?, ?, ?, ?)
                """
                params = [
                    pull_request_number, 
                    commit_sha, 
                    executive_summary[:8000],  # Truncate for storage
                    json.dumps(consolidated_json.get("detailed_findings", []))
                ]
                session.sql(insert_sql, params=params).collect()
                print(f"  ✅ Current review stored for future comparisons")
            except Exception as e:
                print(f"  Warning: Could not store review: {e}")

        if 'GITHUB_OUTPUT' in os.environ:
            delimiter = str(uuid.uuid4())
            with open(os.environ['GITHUB_OUTPUT'], 'a') as gh_out:
                gh_out.write(f'consolidated_summary_text<<{delimiter}\n')
                gh_out.write(f'{executive_summary}\n')
                gh_out.write(f'{delimiter}\n')
            print("  ✅ GitHub Actions output written")

        print(f"\n🎉 TWO-STAGE ANALYSIS COMPLETED!")
        print("=" * 60)
        print(f"📁 Files processed: {len(processed_files)}")
        print(f"🔍 Individual reviews: {len(all_individual_reviews)} (PROMPT 1)")
        print(f"📊 Executive summary: 1 (PROMPT 2)")
        print(f"🎯 Quality Score: {consolidated_json.get('quality_score', 'N/A')}/100")
        print(f"📈 Findings: {len(consolidated_json.get('detailed_findings', []))}")
        if previous_review_context:
            print(f"🔄 Previous context included: ✅ Subsequent commit review")
        else:
            print(f"🔄 Previous context: ❌ Initial commit review")
        
    except Exception as e:
        print(f"❌ Consolidation error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        main()
    finally:
        if 'session' in locals():
            session.close()
            print("\n🔒 Session closed")
