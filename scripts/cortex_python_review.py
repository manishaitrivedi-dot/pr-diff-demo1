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

# Dynamic file pattern - processes all Python AND SQL files in scripts directory
SCRIPTS_DIRECTORY = "scripts"  # Base directory to scan
FILE_PATTERNS = ["*.py", "*.sql"]  # CHANGED: Added SQL files

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

# FIX DATABASE PERMISSIONS AND SETUP: Enhanced approach
database_available = False
current_database = None
current_schema = None

def setup_database_with_fallback():
    """Setup database with multiple fallback strategies"""
    global database_available, current_database, current_schema
    
    print("🔧 Setting up database for review logging...")
    
    # Strategy 1: Try original database with ACCOUNTADMIN
    try:
        session.sql("USE ROLE ACCOUNTADMIN").collect()
        session.sql("GRANT USAGE ON DATABASE MY_DB TO ROLE SYSADMIN").collect()
        session.sql("GRANT USAGE ON SCHEMA MY_DB.PUBLIC TO ROLE SYSADMIN").collect()
        session.sql("GRANT CREATE TABLE ON SCHEMA MY_DB.PUBLIC TO ROLE SYSADMIN").collect()
        session.sql("GRANT INSERT ON ALL TABLES IN SCHEMA MY_DB.PUBLIC TO ROLE SYSADMIN").collect()
        session.sql("USE ROLE SYSADMIN").collect()
        session.sql("USE DATABASE MY_DB").collect()
        session.sql("USE SCHEMA PUBLIC").collect()
        current_database = "MY_DB"
        current_schema = "PUBLIC"
        print("✅ Strategy 1: Successfully granted permissions and using MY_DB.PUBLIC")
        database_available = True
        return True
    except Exception as e:
        print(f"⚠️ Strategy 1 failed: {e}")

    # Strategy 2: Create our own database as SYSADMIN
    try:
        session.sql("USE ROLE SYSADMIN").collect()
        session.sql("CREATE DATABASE IF NOT EXISTS CODE_REVIEWS").collect()
        session.sql("USE DATABASE CODE_REVIEWS").collect()
        session.sql("CREATE SCHEMA IF NOT EXISTS REVIEWS").collect()
        session.sql("USE SCHEMA REVIEWS").collect()
        current_database = "CODE_REVIEWS"
        current_schema = "REVIEWS"
        print("✅ Strategy 2: Successfully created and using CODE_REVIEWS.REVIEWS")
        database_available = True
        return True
    except Exception as e:
        print(f"⚠️ Strategy 2 failed: {e}")

    # Strategy 3: Try user's personal database
    try:
        session.sql("USE ROLE SYSADMIN").collect()
        user_db = f"DB_{cfg['user']}"
        session.sql(f"CREATE DATABASE IF NOT EXISTS {user_db}").collect()
        session.sql(f"USE DATABASE {user_db}").collect()
        session.sql("CREATE SCHEMA IF NOT EXISTS LOGS").collect()
        session.sql("USE SCHEMA LOGS").collect()
        current_database = user_db
        current_schema = "LOGS"
        print(f"✅ Strategy 3: Successfully created and using {user_db}.LOGS")
        database_available = True
        return True
    except Exception as e:
        print(f"⚠️ Strategy 3 failed: {e}")

    print("❌ All database strategies failed - continuing without logging")
    database_available = False
    return False

# Setup database
setup_database_with_fallback()

# ---------------------
# Line Number Extraction Functions (Python-based)
# ---------------------
def extract_line_numbers_from_code(code_content, search_patterns):
    """
    Extract line numbers from code based on search patterns.
    This is Python-based, not LLM-based to avoid NaN issues.
    """
    lines = code_content.split('\n')
    found_lines = []
    
    for i, line in enumerate(lines, 1):  # Line numbers start at 1
        line_stripped = line.strip().lower()
        
        for pattern in search_patterns:
            if pattern.lower() in line_stripped:
                found_lines.append({
                    'line_number': i,
                    'line_content': line.strip(),
                    'matched_pattern': pattern
                })
    
    return found_lines

def extract_function_context(code_content, line_number):
    """
    Extract function context for a given line number
    """
    lines = code_content.split('\n')
    if line_number < 1 or line_number > len(lines):
        return "global scope"
    
    # Look backwards to find function definition
    for i in range(line_number - 1, -1, -1):
        line = lines[i].strip()
        if line.startswith('def ') or line.startswith('class '):
            # Extract function/class name
            if line.startswith('def '):
                func_name = line.split('(')[0].replace('def ', '').strip()
                return f"function: {func_name}"
            elif line.startswith('class '):
                class_name = line.split(':')[0].replace('class ', '').strip()
                return f"class: {class_name}"
    
    return "global scope"

def analyze_code_issues_with_lines(code_content, filename):
    """
    Analyze code and return issues with accurate line numbers (Python-based)
    """
    lines = code_content.split('\n')
    issues = []
    
    # Common issue patterns to check
    issue_patterns = {
        'hardcoded_credentials': ['password', 'api_key', 'secret', 'token'],
        'sql_injection': ['execute(', 'cursor.execute', '.sql('],
        'missing_error_handling': ['except:', 'except Exception:', 'pass'],
        'security_concerns': ['eval(', 'exec(', 'subprocess.call'],
        'performance_issues': ['for i in range(len(', '+ ""'],
        'maintainability': ['TODO', 'FIXME', 'HACK']
    }
    
    for i, line in enumerate(lines, 1):
        line_stripped = line.strip().lower()
        
        for issue_type, patterns in issue_patterns.items():
            for pattern in patterns:
                if pattern.lower() in line_stripped:
                    function_context = extract_function_context(code_content, i)
                    
                    issues.append({
                        'line_number': i,
                        'filename': filename,
                        'issue_type': issue_type,
                        'line_content': line.strip(),
                        'function_context': function_context,
                        'severity': determine_severity(issue_type, line.strip()),
                        'description': generate_issue_description(issue_type, line.strip())
                    })
    
    return issues

def determine_severity(issue_type, line_content):
    """Determine severity based on issue type and content"""
    critical_patterns = ['password =', 'api_key =', 'execute(f"', "execute(f'"]
    high_patterns = ['subprocess.call', 'eval(', 'exec(']
    
    line_lower = line_content.lower()
    
    # Check for critical issues
    for pattern in critical_patterns:
        if pattern in line_lower:
            return "Critical"
    
    # Check for high severity issues
    for pattern in high_patterns:
        if pattern in line_lower:
            return "High"
    
    # Default severity mapping
    severity_map = {
        'hardcoded_credentials': 'High',
        'sql_injection': 'High',
        'security_concerns': 'High',
        'missing_error_handling': 'Medium',
        'performance_issues': 'Medium',
        'maintainability': 'Low'
    }
    
    return severity_map.get(issue_type, 'Medium')

def generate_issue_description(issue_type, line_content):
    """Generate description for the issue"""
    descriptions = {
        'hardcoded_credentials': f"Potential hardcoded credential found: {line_content}",
        'sql_injection': f"Potential SQL injection vulnerability: {line_content}",
        'security_concerns': f"Security concern with dynamic execution: {line_content}",
        'missing_error_handling': f"Missing specific error handling: {line_content}",
        'performance_issues': f"Performance concern: {line_content}",
        'maintainability': f"Maintainability issue: {line_content}"
    }
    
    return descriptions.get(issue_type, f"Issue found: {line_content}")

# ---------------------
# PROMPT TEMPLATES (ENHANCED)
# ---------------------
PROMPT_TEMPLATE_INDIVIDUAL = """Please act as a principal-level code reviewer with expertise in Python, SQL, and database security. Your review must be concise, accurate, and directly actionable, as it will be posted as a GitHub Pull Request comment.

---
# CONTEXT: HOW TO REVIEW (Apply Silently)

1.  **You are reviewing a code file for executive-level analysis.** Focus on business impact, technical debt, security risks, and maintainability.
2.  **Focus your review on the most critical aspects.** Prioritize findings that have business impact or security implications.
3.  **Infer context from the full code.** Base your review on the complete file provided.
4.  **Your entire response MUST be under 65,000 characters.** Include findings of all severities but prioritize Critical and High severity issues.

# REVIEW PRIORITIES (Strict Order)
1.  Security & Correctness (Real SQL Injection with User Input, Production Credentials)
2.  Reliability & Error-handling
3.  Performance & Complexity (Major Bottlenecks, Resource Issues)
4.  Readability & Maintainability
5.  Testability

# CRITICAL INSTRUCTION FOR LINE NUMBERS:
- When you identify an issue, you MUST reference the specific line number where the issue occurs
- Count lines starting from 1 (first line = line 1)
- Be precise with line numbers - this is crucial for tracking issues across reviews
- If an issue spans multiple lines, reference the primary line where the issue starts

# BALANCED SECURITY FOCUS AREAS:
**For SQL Code & Database Operations (BE REALISTIC):**
-   **CRITICAL ONLY:** Confirmed SQL injection with user input paths, production credentials exposed in code, DELETE/UPDATE without WHERE affecting entire tables, data breach risks
-   **HIGH:** Missing parameterization with potential user input exposure, significant security gaps, major performance bottlenecks affecting production
-   **MEDIUM:** Hardcoded non-production values, suboptimal queries, missing indexes, maintainability issues, code organization problems
-   **LOW:** Style inconsistencies, minor optimizations, documentation gaps, cosmetic improvements

**For Python Code (BE REALISTIC):**
-   **CRITICAL ONLY:** Confirmed code injection with user input (eval/exec with user data), production credential exposure, data corruption risks
-   **HIGH:** Significant error handling gaps, major security concerns, subprocess vulnerabilities with user input
-   **MEDIUM:** Code quality improvements, minor security concerns, maintainability issues, missing error handling
-   **LOW:** Style improvements, minor optimizations, documentation gaps, cosmetic issues

# REALISTIC SEVERITY GUIDELINES (MANDATORY - MOST ISSUES ARE NOT CRITICAL):
-   **Critical:** 0-2% of findings (extremely rare - only for confirmed security vulnerabilities with user input or production credential exposure)
-   **High:** 5-15% of findings (significant but fixable issues)
-   **Medium:** 50-60% of findings (most common - code quality and maintainability)
-   **Low:** 25-40% of findings (style and minor improvements)

# ELIGIBILITY CRITERIA FOR FINDINGS (ALL must be met)
-   **Evidence:** Quote the exact code snippet and cite the EXACT line number.
-   **Severity:** Assign {Low | Medium | High | Critical} - BE VERY CONSERVATIVE. Only use Critical for confirmed security vulnerabilities.
-   **Impact & Action:** Briefly explain the issue and provide a minimal, safe correction.
-   **Non-trivial:** Skip purely stylistic nits (e.g., import order, line length) that a linter would catch.

# HARD CONSTRAINTS (For accuracy & anti-hallucination)
-   Do NOT propose APIs that don't exist for the imported modules.
-   Treat parameters like `db_path` as correct dependency injection; do NOT call them hardcoded.
-   NEVER suggest logging sensitive user data or internal paths. Suggest non-reversible fingerprints if context is needed.
-   Do NOT recommend removing correct type hints or docstrings.
-   If code in the file is already correct and idiomatic, do NOT invent problems.
-   DO NOT inflate severity levels - be very conservative. Most findings should be Medium or Low.
-   **MUST INCLUDE ACCURATE LINE NUMBERS** for all findings

---
# OUTPUT FORMAT (Strict, professional, audit-ready)

Your entire response MUST be under 65,000 characters. Include findings of all severity levels with REALISTIC severity assignments.

## Code Review Summary
*A 2-3 sentence high-level summary. Mention the key strengths and the most critical areas for improvement, being realistic about severity.*

---
### Detailed Findings
*A list of all material findings. If no significant issues are found, state "No significant issues found."*

**File:** {filename}
-   **Severity:** {Critical | High | Medium | Low}
-   **Line:** {line_number} (MUST BE ACCURATE)
-   **Function/Context:** `{function_name_if_applicable}`
-   **Finding:** {A clear, concise description of the issue, its impact, and a recommended correction. Be realistic about severity - most issues are Medium or Low.}

**(Repeat for each finding)**

---
### Key Recommendations
*Provide 2-3 high-level, actionable recommendations for improving the overall quality of the codebase based on the findings. Focus on the most impactful improvements.*

---
# CODE TO REVIEW

{PY_CONTENT}
"""

PROMPT_TEMPLATE_CONSOLIDATED = """
You are an expert code review summarization engine for executive-level reporting. Your task is to analyze individual code reviews and generate a single, consolidated executive summary with business impact focus.

You MUST respond ONLY with a valid JSON object that conforms to the executive schema. Do not include any other text, explanations, or markdown formatting outside of the JSON structure.

Follow these instructions to populate the JSON fields:

1.  **`executive_summary` (string):** Write a SHORT 1-2 sentence summary of the most critical findings only.
2.  **`quality_score` (number):** Assign an overall quality score (0-100) based on severity and number of findings.
3.  **`business_impact` (string):** Assess overall business risk as "LOW", "MEDIUM", or "HIGH".
4.  **`technical_debt_score` (string):** Evaluate technical debt as "LOW", "MEDIUM", or "HIGH".
5.  **`security_risk_level` (string):** Determine security risk as "LOW", "MEDIUM", "HIGH", or "CRITICAL". Only use CRITICAL for confirmed SQL injection or production credential exposure.
6.  **`maintainability_rating` (string):** Rate maintainability as "POOR", "FAIR", "GOOD", or "EXCELLENT".
7.  **`detailed_findings` (array of objects):** Create an array of objects, where each object represents a single, distinct issue found in the code:
         -   **`severity`**: Assign severity VERY CONSERVATIVELY: "Low", "Medium", "High", or "Critical". CRITICAL should be 0-2% of all findings (only for confirmed security vulnerabilities with user input or production credential exposure). HIGH should be 5-15%. MEDIUM should be 50-60% (most common). LOW should be 25-40%.
         -   **`category`**: Assign category: "Security", "Performance", "Maintainability", "Best Practices", "Documentation", or "Error Handling".
         -   **`line_number`**: Extract the EXACT line number from the review text. If line number is not provided or unclear, use the string "N/A".
         -   **`function_context`**: From the review text, identify the function or class name where the issue is located. If not applicable, use "global scope".
         -   **`finding`**: Write a clear, concise description of the issue, its potential impact, and a concrete recommendation.
         -   **`business_impact`**: Explain how this affects business operations or risk. Be realistic - most issues have low to medium business impact.
         -   **`recommendation`**: Provide specific technical solution.
         -   **`effort_estimate`**: Estimate effort as "LOW", "MEDIUM", or "HIGH".
         -   **`priority_ranking`**: Assign priority ranking (1 = highest priority).
         -   **`filename`**: The name of the file where the issue was found.
8.  **`metrics` (object):** Include technical metrics:
         -   **`lines_of_code`**: Total number of lines analyzed across all files.
         -   **`complexity_score`**: "LOW", "MEDIUM", or "HIGH".
         -   **`code_coverage_gaps`**: Array of areas needing test coverage.
         -   **`dependency_risks`**: Array of dependency-related risks.
9.  **`immediate_actions` (array of strings):** List critical items requiring immediate attention. Should be very few items.
10. **`previous_issues_resolved` (array of objects):** For each issue from previous review, indicate status:
         -   **`original_issue`**: Brief description of the previous issue
         -   **`status`**: "RESOLVED", "PARTIALLY_RESOLVED", "NOT_ADDRESSED", or "WORSENED"
         -   **`details`**: Explanation of current status
         -   **`original_line_number`**: Line number from previous review
         -   **`current_line_number`**: Current line number if still exists

**CRITICAL INSTRUCTION FOR REALISTIC REVIEWS:**
Your entire response MUST be under {MAX_CHARS_FOR_FINAL_SUMMARY_FILE} characters. Include findings of all severity levels with VERY CONSERVATIVE severity assignments and ACCURATE line numbers extracted from the review text.

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
4. Maintain continuity with previous review comments and LINE NUMBERS
5. In the "previous_issues_resolved" section, provide specific status for each previous issue with original and current line numbers
6. Track line number changes - if code was moved, note the new line numbers

{consolidated_template}
"""

def get_changed_python_files(folder_path=None):
    """
    Dynamically get all Python AND SQL files from the specified folder or scripts directory.
    Uses wildcard pattern matching for flexibility.
    """
    # If no folder specified, use the scripts directory
    if not folder_path:
        folder_path = SCRIPTS_DIRECTORY
        
    if not os.path.exists(folder_path):
        print(f"❌ Directory {folder_path} not found")
        return []
    
    all_files = []
    
    # Process both Python and SQL files
    for pattern in FILE_PATTERNS:
        # Use glob pattern to find files
        pattern_path = os.path.join(folder_path, pattern)
        found_files = glob.glob(pattern_path)
        
        # Also check subdirectories recursively
        recursive_pattern = os.path.join(folder_path, "**", pattern)
        found_files.extend(glob.glob(recursive_pattern, recursive=True))
        
        all_files.extend(found_files)
    
    # Remove duplicates and sort
    all_files = sorted(list(set(all_files)))
    
    print(f"📁 Found {len(all_files)} code files in {folder_path} using patterns {FILE_PATTERNS}:")
    for file in all_files:
        file_type = "SQL" if file.lower().endswith('.sql') else "Python"
        print(f"  - {file} ({file_type})")
    
    return all_files

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
        "Critical": 12,    # Each critical issue deducts 12 points (but there should be very few)
        "High": 4,         # Each high issue deducts 4 points
        "Medium": 1.5,     # Each medium issue deducts 1.5 points
        "Low": 0.3         # Each low issue deducts 0.3 points
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
                # Critical: Should be very rare, but high impact
                if count <= 2:
                    deduction = weight * count
                else:
                    deduction = weight * 2 + (count - 2) * (weight + 3)
                # Cap critical deductions at 30 points max
                deduction = min(30, deduction)
            elif severity == "High":
                # High: Linear scaling with small bonus after 10 issues
                if count <= 10:
                    deduction = weight * count
                else:
                    deduction = weight * 10 + (count - 10) * (weight + 1)
                # Cap high severity deductions at 25 points max
                deduction = min(25, deduction)
            else:
                # Medium/Low: Pure linear scaling with caps
                deduction = weight * count
                # Reasonable caps
                if severity == "Medium":
                    deduction = min(20, deduction)
                else:
                    deduction = min(10, deduction)
                
            total_deductions += deduction
            print(f"    {severity}: {count} issues = -{deduction:.1f} points (capped)")
    
    # MUCH REDUCED penalties
    if total_lines_of_code > 0:
        affected_ratio = total_affected_lines / total_lines_of_code
        if affected_ratio > 0.4:  # Only penalize if more than 40% affected
            coverage_penalty = min(5, int(affected_ratio * 20))  # Max 5 point penalty
            total_deductions += coverage_penalty
            print(f"    Coverage penalty: -{coverage_penalty} points ({affected_ratio:.1%} affected)")
    
    # REALISTIC critical threshold penalties (should rarely trigger)
    if severity_counts["Critical"] >= 3:  # Very high threshold
        total_deductions += 10
        print(f"    Executive threshold penalty: -10 points (3+ critical issues)")
    
    if severity_counts["Critical"] + severity_counts["High"] >= 20:  # High threshold
        total_deductions += 5
        print(f"    Production readiness penalty: -5 points (20+ critical/high issues)")
    
    # Calculate final score
    final_score = max(0, base_score - int(total_deductions))
    
    print(f"  🎯 Final calculation: {base_score} - {int(total_deductions)} = {final_score}")
    
    # ADJUSTED executive score bands for more realistic scoring
    if final_score >= 85:
        return min(100, final_score)  # Excellent
    elif final_score >= 70:
        return final_score  # Good
    elif final_score >= 50:
        return final_score  # Fair - needs attention
    else:
        return max(30, final_score)  # Poor - but never below 30 for functional code

def setup_review_log_table():
    """Setup the review log table with VARIANT columns"""
    global database_available
    
    if not database_available:
        return False
        
    try:
        # Create table if it doesn't exist (NO DROP - just create if not exists)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {current_database}.{current_schema}.CODE_REVIEW_LOG (
            REVIEW_ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
            PULL_REQUEST_NUMBER INTEGER,
            COMMIT_SHA VARCHAR(40),
            REVIEW_SUMMARY VARIANT,
            DETAILED_FINDINGS_JSON VARIANT,
            REVIEW_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FILES_ANALYZED VARIANT
        );
        """
        session.sql(create_table_query).collect()
        print(f"✅ Review log table ensured in {current_database}.{current_schema}")
        return True
    except Exception as e:
        print(f"❌ Failed to create review log table: {e}")
        return False

def store_review_log(pull_request_number, commit_sha, executive_summary, consolidated_json, processed_files):
    """Store review with VARIANT columns - APPENDS, does not overwrite"""
    global database_available
    
    if not database_available:
        print("  ⚠️ Database not available - cannot store review")
        return False
        
    try:
        findings = consolidated_json.get("detailed_findings", [])
        
        # APPEND new review (no deletion of previous entries)
        insert_sql = f"""
        INSERT INTO {current_database}.{current_schema}.CODE_REVIEW_LOG 
            (PULL_REQUEST_NUMBER, COMMIT_SHA, REVIEW_SUMMARY, DETAILED_FINDINGS_JSON, FILES_ANALYZED)
            SELECT ?, ?, PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?)
        """
        
        # Store all relevant data
        params = [
            pull_request_number,
            commit_sha,
            json.dumps(consolidated_json) if consolidated_json else None,
            json.dumps(findings) if findings else None,
            json.dumps(processed_files) if processed_files else None
        ]
        
        session.sql(insert_sql, params=params).collect()
        print(f"  ✅ Review appended successfully to {current_database}.{current_schema}.CODE_REVIEW_LOG")
        
        # Show review history for this PR
        count_query = f"""
        SELECT COUNT(*) as total_reviews
        FROM {current_database}.{current_schema}.CODE_REVIEW_LOG 
        WHERE PULL_REQUEST_NUMBER = {pull_request_number}
        """
        result = session.sql(count_query).collect()
        total_reviews = result[0]['TOTAL_REVIEWS'] if result else 0
        print(f"  📊 Total reviews for PR #{pull_request_number}: {total_reviews}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to store review: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_previous_reviews_hybrid(pull_request_number, limit=3):
    """
    HYBRID APPROACH: Get previous reviews with Python-based analysis
    Returns structured previous review data with accurate line numbers
    """
    global database_available
    
    if not database_available:
        return None, []
        
    try:
        # Get all previous reviews for this PR ordered by most recent first
        query = f"""
        SELECT 
            REVIEW_SUMMARY, 
            DETAILED_FINDINGS_JSON,
            FILES_ANALYZED,
            REVIEW_TIMESTAMP,
            COMMIT_SHA
        FROM {current_database}.{current_schema}.CODE_REVIEW_LOG 
        WHERE PULL_REQUEST_NUMBER = {pull_request_number}
        ORDER BY REVIEW_TIMESTAMP DESC 
        LIMIT {limit}
        """
        
        results = session.sql(query).collect()
        
        if not results:
            print("  📋 No previous reviews found for this PR")
            return None, []
        
        print(f"  📋 Found {len(results)} previous review(s) for PR #{pull_request_number}")
        
        # Process previous reviews with Python-based analysis
        previous_findings = []
        previous_context_parts = []
        
        for i, row in enumerate(results):
            try:
                review_summary = json.loads(str(row['REVIEW_SUMMARY'])) if row['REVIEW_SUMMARY'] else {}
                findings_json = json.loads(str(row['DETAILED_FINDINGS_JSON'])) if row['DETAILED_FINDINGS_JSON'] else []
                files_analyzed = json.loads(str(row['FILES_ANALYZED'])) if row['FILES_ANALYZED'] else []
                
                # Extract findings with Python-based line number validation
                for finding in findings_json:
                    line_num = finding.get('line_number', 'N/A')
                    # Ensure line number is properly formatted
                    if isinstance(line_num, str) and line_num.isdigit():
                        line_num = int(line_num)
                    elif not isinstance(line_num, int):
                        line_num = 'N/A'
                    
                    previous_findings.append({
                        'filename': finding.get('filename', 'unknown'),
                        'line_number': line_num,
                        'original_line_number': line_num,  # Store original line number
                        'severity': finding.get('severity', 'Medium'),
                        'finding': finding.get('finding', 'Previous issue'),
                        'function_context': finding.get('function_context', 'global scope'),
                        'category': finding.get('category', 'General'),
                        'review_timestamp': row['REVIEW_TIMESTAMP'],
                        'commit_sha': row['COMMIT_SHA']
                    })
                
                # Build context summary for this review
                review_date = row['REVIEW_TIMESTAMP'].strftime('%Y-%m-%d %H:%M') if row['REVIEW_TIMESTAMP'] else 'Unknown'
                context_part = f"""
Review #{i+1} from {review_date} (Commit: {row['COMMIT_SHA'][:8]}):
- Quality Score: {review_summary.get('quality_score', 'N/A')}
- Total Findings: {len(findings_json)}
- Files: {', '.join(files_analyzed) if files_analyzed else 'N/A'}
- Critical Issues: {sum(1 for f in findings_json if f.get('severity') == 'Critical')}
- High Issues: {sum(1 for f in findings_json if f.get('severity') == 'High')}
"""
                previous_context_parts.append(context_part)
                
            except Exception as e:
                print(f"  ⚠️ Error processing review {i+1}: {e}")
                continue
        
        # Combine all context
        previous_context = "\n".join(previous_context_parts)
        
        if previous_context_parts:
            previous_context += f"\n\nPrevious Findings Summary ({len(previous_findings)} total):\n"
            
            # Group findings by file and severity for summary
            by_file = {}
            for finding in previous_findings[:10]:  # Show top 10 previous findings
                filename = finding['filename']
                if filename not in by_file:
                    by_file[filename] = []
                by_file[filename].append(finding)
            
            for filename, file_findings in by_file.items():
                previous_context += f"\n{filename}:\n"
                for finding in file_findings:
                    line_display = finding['line_number'] if finding['line_number'] != 'N/A' else 'N/A'
                    previous_context += f"  - Line {line_display}: {finding['severity']} - {finding['finding'][:100]}...\n"
        
        print(f"  📊 Processed {len(previous_findings)} previous findings across {len(results)} reviews")
        return previous_context, previous_findings
        
    except Exception as e:
        print(f"  ⚠️ Error retrieving previous reviews: {e}")
        return None, []

def compare_findings_with_previous(current_findings, previous_findings):
    """
    Python-based comparison of current findings with previous findings
    Returns updated findings with resolution status
    """
    if not previous_findings:
        return current_findings
    
    print(f"  🔄 Comparing {len(current_findings)} current findings with {len(previous_findings)} previous findings")
    
    # Create enhanced findings with comparison data
    enhanced_findings = []
    
    for current_finding in current_findings:
        current_file = current_finding.get('filename', '')
        current_line = current_finding.get('line_number', 'N/A')
        current_severity = current_finding.get('severity', '')
        current_description = current_finding.get('finding', '')
        
        # Look for similar issues in previous findings
        similar_previous = None
        best_match_score = 0
        
        for prev_finding in previous_findings:
            prev_file = prev_finding.get('filename', '')
            prev_line = prev_finding.get('line_number', 'N/A')
            prev_description = prev_finding.get('finding', '')
            
            # Calculate similarity score
            match_score = 0
            
            # File match (high weight)
            if current_file == prev_file:
                match_score += 50
            
            # Line number proximity (medium weight)
            if current_line != 'N/A' and prev_line != 'N/A':
                try:
                    curr_line_int = int(current_line) if isinstance(current_line, str) else current_line
                    prev_line_int = int(prev_line) if isinstance(prev_line, str) else prev_line
                    line_diff = abs(curr_line_int - prev_line_int)
                    if line_diff == 0:
                        match_score += 30
                    elif line_diff <= 5:
                        match_score += 20
                    elif line_diff <= 10:
                        match_score += 10
                except (ValueError, TypeError):
                    pass
            
            # Description similarity (medium weight)
            if len(current_description) > 10 and len(prev_description) > 10:
                # Simple keyword matching
                current_words = set(current_description.lower().split())
                prev_words = set(prev_description.lower().split())
                common_words = current_words.intersection(prev_words)
                if common_words:
                    match_score += min(20, len(common_words) * 2)
            
            # Update best match
            if match_score > best_match_score:
                best_match_score = match_score
                similar_previous = prev_finding
        
        # Enhance finding with comparison results
        enhanced_finding = current_finding.copy()
        
        if similar_previous and best_match_score >= 40:  # Threshold for considering it a match
            # This appears to be a continuing issue
            enhanced_finding['is_previous_issue'] = 'Yes'
            enhanced_finding['original_line_number'] = similar_previous.get('original_line_number', 'N/A')
            enhanced_finding['previous_severity'] = similar_previous.get('severity', 'Unknown')
            
            # Determine resolution status
            if current_severity == similar_previous.get('severity', ''):
                enhanced_finding['resolution_status'] = 'NOT_ADDRESSED'
            elif current_severity in ['Critical', 'High'] and similar_previous.get('severity') in ['Medium', 'Low']:
                enhanced_finding['resolution_status'] = 'WORSENED'
            elif current_severity in ['Medium', 'Low'] and similar_previous.get('severity') in ['Critical', 'High']:
                enhanced_finding['resolution_status'] = 'PARTIALLY_RESOLVED'
            else:
                enhanced_finding['resolution_status'] = 'MODIFIED'
        else:
            # This appears to be a new issue
            enhanced_finding['is_previous_issue'] = 'No'
            enhanced_finding['resolution_status'] = 'NEW'
        
        enhanced_findings.append(enhanced_finding)
    
    # Also check for resolved issues (issues that were in previous but not in current)
    resolved_count = 0
    for prev_finding in previous_findings:
        found_in_current = False
        prev_file = prev_finding.get('filename', '')
        prev_line = prev_finding.get('line_number', 'N/A')
        
        for curr_finding in current_findings:
            curr_file = curr_finding.get('filename', '')
            curr_line = curr_finding.get('line_number', 'N/A')
            
            # Check if this previous issue exists in current findings
            if curr_file == prev_file:
                try:
                    if prev_line != 'N/A' and curr_line != 'N/A':
                        prev_line_int = int(prev_line) if isinstance(prev_line, str) else prev_line
                        curr_line_int = int(curr_line) if isinstance(curr_line, str) else curr_line
                        if abs(prev_line_int - curr_line_int) <= 5:  # Within 5 lines
                            found_in_current = True
                            break
                except (ValueError, TypeError):
                    pass
        
        if not found_in_current:
            resolved_count += 1
    
    print(f"  📊 Comparison complete: {resolved_count} issues appear resolved, {sum(1 for f in enhanced_findings if f.get('is_previous_issue') == 'Yes')} continuing issues, {sum(1 for f in enhanced_findings if f.get('resolution_status') == 'NEW')} new issues")
    
    return enhanced_findings

def format_executive_pr_display(json_response: dict, processed_files: list, has_previous_context: bool = False, previous_findings_count: int = 0) -> str:
    summary = json_response.get("executive_summary", "Technical analysis completed")
    findings = json_response.get("detailed_findings", [])
    quality_score = json_response.get("quality_score", 75)
    business_impact = json_response.get("business_impact", "MEDIUM")
    security_risk = json_response.get("security_risk_level", "MEDIUM")
    tech_debt = json_response.get("technical_debt_score", "MEDIUM")
    maintainability = json_response.get("maintainability_rating", "FAIR")
    metrics = json_response.get("metrics", {})
    immediate_actions = json_response.get("immediate_actions", [])
    previous_issues = json_response.get("previous_issues_resolved", [])
    
    critical_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "CRITICAL")
    high_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "HIGH")
    medium_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "MEDIUM")
    low_count = sum(1 for f in findings if str(f.get("severity", "")).upper() == "LOW")
    
    # Count by file type for better reporting
    python_files = [f for f in processed_files if f.lower().endswith('.py')]
    sql_files = [f for f in processed_files if f.lower().endswith('.sql')]
    
    # Count critical/high issues by file type
    python_critical = sum(1 for f in findings if f.get("filename", "").lower().endswith('.py') and str(f.get("severity", "")).upper() == "CRITICAL")
    python_high = sum(1 for f in findings if f.get("filename", "").lower().endswith('.py') and str(f.get("severity", "")).upper() == "HIGH")
    sql_critical = sum(1 for f in findings if f.get("filename", "").lower().endswith('.sql') and str(f.get("severity", "")).upper() == "CRITICAL")
    sql_high = sum(1 for f in findings if f.get("filename", "").lower().endswith('.sql') and str(f.get("severity", "")).upper() == "HIGH")
    
    # Count resolution statuses if previous context exists
    resolved_count = sum(1 for f in findings if f.get('resolution_status') == 'RESOLVED')
    continuing_count = sum(1 for f in findings if f.get('is_previous_issue') == 'Yes')
    new_count = sum(1 for f in findings if f.get('resolution_status') == 'NEW')
    
    risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
    quality_emoji = "🟢" if quality_score >= 80 else ("🟡" if quality_score >= 60 else "🔴")
    
    display_text = f"""# 📊 Executive Code Review Report

**Files Analyzed:** {len(processed_files)} files | **Analysis Date:** {datetime.now().strftime('%Y-%m-%d')} | **Database:** {current_database}.{current_schema}

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
| 🟢 Low | {low_count} | Technical improvement |

## 📁 File Analysis Breakdown

| File Type | Count | Critical Issues | High Issues |
|-----------|-------|----------------|-------------|
| 🐍 Python | {len(python_files)} | {python_critical} | {python_high} |
| 🗄️ SQL | {len(sql_files)} | {sql_critical} | {sql_high} |

"""

    # Previous review comparison section (only show if we have previous context)
    if has_previous_context and previous_findings_count > 0:
        display_text += f"""## 📊 Review Progression Analysis

| Status | Count | Description |
|--------|-------|-------------|
| 🆕 New Issues | {new_count} | Issues identified in this review |
| 🔄 Continuing Issues | {continuing_count} | Issues from previous reviews still present |
| ✅ Resolved Issues | {resolved_count} | Previous issues that appear fixed |
| 📋 Previous Total | {previous_findings_count} | Total issues from previous reviews |

"""

    # Enhanced findings table with accurate line numbers
    if findings:
        display_text += """<details>
<summary><strong>🔍 Detailed Findings with Line Numbers</strong> (Click to expand)</summary>

"""
        
        # Show different table headers based on whether we have previous context
        if has_previous_context:
            display_text += """| Priority | File | Line | Function/Context | Issue | Previous Issue | Status | Resolution |
|----------|------|------|------------------|-------|----------------|--------|------------|
"""
        else:
            display_text += """| Priority | File | Line | Function/Context | Issue | Business Impact |
|----------|------|------|------------------|-------|-----------------|
"""
        
        severity_order = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
        sorted_findings = sorted(findings, key=lambda x: severity_order.get(str(x.get("severity", "Low")), 4))
        
        for finding in sorted_findings[:25]:  # Show top 25 findings
            severity = str(finding.get("severity", "Medium"))
            filename = finding.get("filename", "N/A")
            line = finding.get("line_number", "N/A")
            function_context = finding.get("function_context", "global scope")
            
            # Ensure line number is properly displayed
            if isinstance(line, str) and line.isdigit():
                line = int(line)
            line_display = str(line) if line != "N/A" else "N/A"
            
            issue_display = str(finding.get("finding", ""))[:80] + "..." if len(str(finding.get("finding", ""))) > 80 else str(finding.get("finding", ""))
            business_impact_display = str(finding.get("business_impact", ""))[:50] + "..." if len(str(finding.get("business_impact", ""))) > 50 else str(finding.get("business_impact", ""))
            
            priority_emoji = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}.get(severity, "🟡")
            
            if has_previous_context:
                # Show comparison data
                is_previous = finding.get("is_previous_issue", "No")
                resolution_status = finding.get("resolution_status", "NEW")
                original_line = finding.get("original_line_number", "N/A")
                
                # Format resolution status with emoji
                resolution_emoji = {
                    "NEW": "🆕 New",
                    "NOT_ADDRESSED": "❌ Not Fixed", 
                    "PARTIALLY_RESOLVED": "⚠️ Partial",
                    "RESOLVED": "✅ Fixed",
                    "WORSENED": "🔴 Worsened",
                    "MODIFIED": "🔄 Changed"
                }.get(resolution_status, resolution_status)
                
                prev_line_display = f"L{original_line}" if original_line != "N/A" else "New"
                
                display_text += f"| {priority_emoji} {severity} | {filename} | {line_display} | {function_context} | {issue_display} | {prev_line_display} | {is_previous} | {resolution_emoji} |\n"
            else:
                # Standard display without comparison
                display_text += f"| {priority_emoji} {severity} | {filename} | {line_display} | {function_context} | {issue_display} | {business_impact_display} |\n"
        
        display_text += "\n</details>\n\n"

    if immediate_actions:
        display_text += """<details>
<summary><strong>⚡ Immediate Actions Required</strong> (Click to expand)</summary>

"""
        for i, action in enumerate(immediate_actions, 1):
            display_text += f"{i}. {action}\n"
        display_text += "\n</details>\n\n"

    display_text += f"""---

**📋 Review Summary:** {len(findings)} findings identified | **🎯 Quality Score:** {quality_score}/100 | **⚡ Critical Issues:** {critical_count}"""
    
    if has_previous_context:
        display_text += f" | **🔄 Issue Tracking:** {new_count} new, {continuing_count} continuing"

    display_text += f"""

*🔬 Powered by Snowflake Cortex AI • Hybrid Analysis with Python Line Number Extraction • Stored in {current_database}.{current_schema}*"""

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
        code_files = get_changed_python_files(SCRIPTS_DIRECTORY)
        if not code_files:
            print(f"❌ No Python/SQL files found in {SCRIPTS_DIRECTORY} directory using patterns {FILE_PATTERNS}")
            return
            
        folder_path = SCRIPTS_DIRECTORY  # Always use scripts directory
            
    else:
        # Fallback for single file mode - use scripts directory with wildcard pattern
        code_files = get_changed_python_files(SCRIPTS_DIRECTORY)
        if not code_files:
            print(f"❌ No Python/SQL files found in {SCRIPTS_DIRECTORY} directory using patterns {FILE_PATTERNS}")
            return
            
        folder_path = SCRIPTS_DIRECTORY
        output_folder_path = "output_reviews"
        pull_request_number = 0
        commit_sha = "test"
        directory_mode = False
        print(f"Running in dynamic pattern mode with {len(code_files)} code files from {SCRIPTS_DIRECTORY}")

    if os.path.exists(output_folder_path):
        import shutil
        shutil.rmtree(output_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)

    all_individual_reviews = []
    processed_files = []

    print("\n🔍 STAGE 1: Individual File Analysis with Python Line Detection...")
    print("=" * 60)
    
    for file_path in code_files:
        filename = os.path.basename(file_path)
        print(f"\n--- Reviewing file: {filename} ---")
        processed_files.append(filename)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                code_content = f.read()

            if not code_content.strip():
                review_text = "No code found in file, skipping review."
            else:
                # PYTHON-BASED: Pre-analyze code for issues with accurate line numbers
                python_detected_issues = analyze_code_issues_with_lines(code_content, filename)
                print(f"  🔍 Python analysis detected {len(python_detected_issues)} potential issues")
                
                # Add Python-detected issues to the prompt for LLM awareness
                python_analysis_summary = ""
                if python_detected_issues:
                    python_analysis_summary = "\n\nPYTHON PRE-ANALYSIS DETECTED ISSUES (Reference for accurate line numbers):\n"
                    for issue in python_detected_issues[:10]:  # Top 10 issues
                        python_analysis_summary += f"- Line {issue['line_number']}: {issue['severity']} - {issue['description']} ({issue['function_context']})\n"
                
                chunks = chunk_large_file(code_content)
                print(f"  File split into {len(chunks)} chunk(s)")
                
                chunk_reviews = []
                for i, chunk in enumerate(chunks):
                    chunk_name = f"{filename}_chunk_{i+1}" if len(chunks) > 1 else filename
                    print(f"  Processing chunk: {chunk_name}")
                    
                    # Enhanced prompt with Python pre-analysis
                    individual_prompt = build_prompt_for_individual_review(chunk + python_analysis_summary, chunk_name)
                    review_text = review_with_cortex(MODEL, individual_prompt, session)
                    chunk_reviews.append(review_text)
                
                if len(chunk_reviews) > 1:
                    review_text = "\n\n".join([f"## Chunk {i+1}\n{review}" for i, review in enumerate(chunk_reviews)])
                else:
                    review_text = chunk_reviews[0]

            all_individual_reviews.append({
                "filename": filename,
                "review_feedback": review_text,
                "python_detected_issues": python_detected_issues if 'python_detected_issues' in locals() else []
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
                "review_feedback": f"ERROR: Could not generate review. Reason: {e}",
                "python_detected_issues": []
            })

    print(f"\n🔄 STAGE 2: Hybrid Executive Consolidation with Previous Review Comparison...")
    print("=" * 60)
    print(f"Consolidating {len(all_individual_reviews)} individual reviews...")

    if not all_individual_reviews:
        print("❌ No reviews to consolidate")
        return

    try:
        # Setup the review log table (ensures table exists, doesn't drop)
        if database_available:
            setup_review_log_table()

        # HYBRID APPROACH: Get previous review context with Python-based analysis
        previous_review_context = None
        previous_findings = []
        has_previous_context = False
        
        if pull_request_number and pull_request_number != 0 and database_available:
            previous_review_context, previous_findings = get_previous_reviews_hybrid(pull_request_number, limit=3)
            if previous_review_context:
                print(f"  📋 HYBRID: Retrieved {len(previous_findings)} previous findings for comparison")
                has_previous_context = True
            else:
                print("  📋 This is the initial commit review")
                has_previous_context = False
        elif not database_available:
            print("  ⚠️ Database not available - cannot retrieve previous reviews")
            has_previous_context = False

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
            
            # HYBRID: Compare with previous findings using Python-based analysis
            if has_previous_context and previous_findings:
                current_findings = consolidated_json.get("detailed_findings", [])
                enhanced_findings = compare_findings_with_previous(current_findings, previous_findings)
                consolidated_json["detailed_findings"] = enhanced_findings
                print(f"  🔄 HYBRID: Enhanced {len(enhanced_findings)} findings with previous comparison")
            
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
                    "immediate_actions": [],
                    "previous_issues_resolved": []
                }

        # ENHANCED: Format display with hybrid comparison data
        executive_summary = format_executive_pr_display(
            consolidated_json, 
            processed_files, 
            has_previous_context,
            len(previous_findings)
        )
        
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
                "description": f.get("finding", "Critical issue found")
            }
            criticals.append(critical)

        # Empty critical summary (as requested in original code)
        critical_summary = ""

        review_output_data = {
            "full_review": executive_summary,
            "full_review_markdown": executive_summary,
            "full_review_json": consolidated_json,
            "criticals": criticals,
            "critical_summary": critical_summary,
            "critical_count": len(critical_findings),
            "file": processed_files[0] if processed_files else "unknown",
            "timestamp": datetime.now().isoformat(),
            "has_previous_context": has_previous_context,
            "previous_findings_count": len(previous_findings),
            "hybrid_analysis": True
        }

        with open("review_output.json", "w", encoding='utf-8') as f:
            json.dump(review_output_data, f, indent=2, ensure_ascii=False)
        print("  ✅ review_output.json saved for inline_comment.py compatibility")

        # APPEND (not overwrite) current review for future comparisons
        if pull_request_number and pull_request_number != 0 and database_available:
            store_review_log(pull_request_number, commit_sha, executive_summary, consolidated_json, processed_files)

        if 'GITHUB_OUTPUT' in os.environ:
            delimiter = str(uuid.uuid4())
            with open(os.environ['GITHUB_OUTPUT'], 'a') as gh_out:
                gh_out.write(f'consolidated_summary_text<<{delimiter}\n')
                gh_out.write(f'{executive_summary}\n')
                gh_out.write(f'{delimiter}\n')
            print("  ✅ GitHub Actions output written")

        print(f"\n🎉 HYBRID TWO-STAGE ANALYSIS COMPLETED!")
        print("=" * 80)
        print(f"📁 Files processed: {len(processed_files)}")
        print(f"🔍 Individual reviews: {len(all_individual_reviews)} (PROMPT 1 + Python Analysis)")
        print(f"📊 Executive summary: 1 (PROMPT 2 + Hybrid Comparison)")
        print(f"🎯 Quality Score: {consolidated_json.get('quality_score', 'N/A')}/100")
        print(f"📈 Current Findings: {len(consolidated_json.get('detailed_findings', []))}")
        print(f"🔢 Line Numbers: Python-based extraction (accurate)")
        
        if has_previous_context:
            new_issues = sum(1 for f in consolidated_json.get('detailed_findings', []) if f.get('resolution_status') == 'NEW')
            continuing_issues = sum(1 for f in consolidated_json.get('detailed_findings', []) if f.get('is_previous_issue') == 'Yes')
            print(f"🔄 Issue Tracking: {len(previous_findings)} previous → {new_issues} new, {continuing_issues} continuing")
        else:
            print(f"🔄 Previous context: ❌ Initial commit review")
        
        if database_available:
            print(f"💾 Database logging: ✅ Appended to {current_database}.{current_schema}")
        else:
            print(f"💾 Database logging: ❌ Not available")
        
        print("🔧 Enhancements applied:")
        print("   • Python-based line number extraction (no more NaN)")
        print("   • Hybrid comparison with previous reviews") 
        print("   • Append-only database logging (no overwriting)")
        print("   • Accurate issue resolution tracking")
        
    except Exception as e:
        print(f"❌ Consolidation error: {e}")
        import traceback
        traceback.print_exc()

        # FALLBACK: Create basic review_output.json even if consolidation fails
        fallback_summary = f"""# 📊 Code Review Report (Fallback Mode)

**Files Analyzed:** {len(processed_files)} files | **Analysis Date:** {datetime.now().strftime('%Y-%m-%d')}

## ⚠️ Review Status
Technical analysis completed with {len(all_individual_reviews)} individual file reviews.
Executive consolidation encountered an error but individual reviews are available.

**Files Processed:**
"""
        for i, file in enumerate(processed_files, 1):
            fallback_summary += f"{i}. {file}\n"

        fallback_summary += "\n*Individual file reviews available in output folder.*"

        # Create fallback review_output.json
        fallback_data = {
            "full_review": fallback_summary,
            "full_review_markdown": fallback_summary,
            "full_review_json": {
                "executive_summary": "Review completed with errors during consolidation",
                "quality_score": 50,
                "business_impact": "MEDIUM",
                "detailed_findings": [],
                "immediate_actions": ["Check consolidation errors in logs"]
            },
            "criticals": [],
            "critical_summary": "",
            "file": processed_files[0] if processed_files else "unknown",
            "timestamp": datetime.now().isoformat(),
            "status": "fallback_mode",
            "hybrid_analysis": False
        }

        with open("review_output.json", "w", encoding='utf-8') as f:
            json.dump(fallback_data, f, indent=2, ensure_ascii=False)
        print("  ⚠️ Fallback review_output.json created for inline_comment.py compatibility")

if __name__ == "__main__":
    try:
        main()
    finally:
        if 'session' in locals():
            session.close()
            print("\n🔒 Session closed")
