import os, sys, json, re, uuid, glob
from pathlib import Path
from snowflake.snowpark import Session
import pandas as pd  # kept in case you need it later
from datetime import datetime

# ---------------------
# Config
# ---------------------
MODEL = "openai-gpt-4.1"
MAX_CHARS_FOR_FINAL_SUMMARY_FILE = 65000
MAX_TOKENS_FOR_SUMMARY_INPUT = 100000

# Dynamic file pattern - processes all Python AND SQL files in scripts directory
SCRIPTS_DIRECTORY = "scripts"  # Base directory to scan
FILE_PATTERNS = ["*.py", "*.sql"]  # Python + SQL

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

# DB state
database_available = False
current_database = None
current_schema = None

def setup_database_with_fallback():
    """Setup database with multiple fallback strategies"""
    global database_available, current_database, current_schema

    print("🔧 Setting up database for review logging...")

    # Strategy 1: Use MY_DB.PUBLIC (grant via ACCOUNTADMIN)
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

    # Strategy 2: Create CODE_REVIEWS.REVIEWS
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

    # Strategy 3: Personal DB
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
# Python-based line detection (hybrid assist)
# ---------------------
def extract_function_context(code_content, line_number):
    lines = code_content.split('\n')
    if line_number < 1 or line_number > len(lines):
        return "global scope"
    for i in range(line_number - 1, -1, -1):
        line = lines[i].strip()
        if line.startswith('def ') or line.startswith('class '):
            if line.startswith('def '):
                func_name = line.split('(')[0].replace('def ', '').strip()
                return f"function: {func_name}"
            elif line.startswith('class '):
                class_name = line.split(':')[0].replace('class ', '').strip()
                return f"class: {class_name}"
    return "global scope"

def determine_severity(issue_type, line_content):
    critical_patterns = ['password =', 'api_key =', 'execute(f"', "execute(f'"]
    high_patterns = ['subprocess.call', 'eval(', 'exec(']
    line_lower = line_content.lower()
    for p in critical_patterns:
        if p in line_lower:
            return "Critical"
    for p in high_patterns:
        if p in line_lower:
            return "High"
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
    descriptions = {
        'hardcoded_credentials': f"Potential hardcoded credential found: {line_content}",
        'sql_injection': f"Potential SQL injection vulnerability: {line_content}",
        'security_concerns': f"Security concern with dynamic execution: {line_content}",
        'missing_error_handling': f"Missing specific error handling: {line_content}",
        'performance_issues': f"Performance concern: {line_content}",
        'maintainability': f"Maintainability issue: {line_content}"
    }
    return descriptions.get(issue_type, f"Issue found: {line_content}")

def analyze_code_issues_with_lines(code_content, filename):
    """Lightweight heuristic scan to surface line numbers for common risk patterns."""
    lines = code_content.split('\n')
    issues = []
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
                    issues.append({
                        'line_number': i,
                        'filename': filename,
                        'issue_type': issue_type,
                        'line_content': line.strip(),
                        'function_context': extract_function_context(code_content, i),
                        'severity': determine_severity(issue_type, line.strip()),
                        'description': generate_issue_description(issue_type, line.strip())
                    })
    return issues

# ---------------------
# PROMPT TEMPLATES (asks for line numbers explicitly)
# ---------------------
PROMPT_TEMPLATE_INDIVIDUAL = """Please act as a principal-level code reviewer with expertise in Python, SQL, and database security. Your review must be concise, accurate, and directly actionable.

---
# PRIORITIES (in order)
1) Security & Correctness  2) Reliability & Error-handling  3) Performance  4) Maintainability  5) Testability

# LINE NUMBERS — MANDATORY
- Reference the **exact line number(s)** for each finding (count first line as 1)
- If a finding spans multiple lines, cite the **start line**
- Quote the exact snippet for evidence

# Severity realism
- Critical: only confirmed injection with user input or production credentials (0–2%)
- High: significant security/reliability gaps (5–15%)
- Medium: code quality/maintainability (50–60%)
- Low: style/docs/minors (25–40%)

# Output format (repeat per finding)
**File:** {filename}
- **Severity:** {Critical | High | Medium | Low}
- **Line:** {line_number}
- **Function/Context:** `{function_name_if_applicable}`
- **Finding:** {concise description with impact + minimal safe fix}

---
# CODE TO REVIEW
{PY_CONTENT}
"""

PROMPT_TEMPLATE_CONSOLIDATED = """
You are an executive-level code review summarizer. Return ONLY valid JSON.

Fields:
1) executive_summary (string, 1–2 sentences)
2) quality_score (number 0–100)
3) business_impact ("LOW"|"MEDIUM"|"HIGH")
4) technical_debt_score ("LOW"|"MEDIUM"|"HIGH")
5) security_risk_level ("LOW"|"MEDIUM"|"HIGH"|"CRITICAL")
6) maintainability_rating ("POOR"|"FAIR"|"GOOD"|"EXCELLENT")
7) detailed_findings (array of objects):
   - severity ("Low"|"Medium"|"High"|"Critical")
   - category ("Security"|"Performance"|"Maintainability"|"Best Practices"|"Documentation"|"Error Handling")
   - line_number (number or "N/A")
   - function_context ("global scope" or name)
   - finding (string)
   - business_impact (string)
   - recommendation (string)
   - effort_estimate ("LOW"|"MEDIUM"|"HIGH")
   - priority_ranking (number)
   - filename (string)
8) metrics:
   - lines_of_code (number)
   - complexity_score ("LOW"|"MEDIUM"|"HIGH")
   - code_coverage_gaps (array)
   - dependency_risks (array)
9) immediate_actions (array of strings)
10) previous_issues_resolved (array of objects):
   - original_issue (string)
   - status ("RESOLVED"|"PARTIALLY_RESOLVED"|"NOT_ADDRESSED"|"WORSENED")
   - details (string)
   - original_line_number (number or "N/A")
   - current_line_number (number or "N/A")
   - filename (string)

Hard limits:
- Max response length: {MAX_CHARS_FOR_FINAL_SUMMARY_FILE} chars
- Severity distributions must be realistic as above.

Here are the individual reviews (JSON list with filename + review_feedback):
{ALL_REVIEWS_CONTENT}
"""

PROMPT_TEMPLATE_WITH_CONTEXT = """
You are reviewing subsequent commits for Pull Request #{pr_number}.

PREVIOUS REVIEW SUMMARY AND FINDINGS (verbatim for your context — keep continuity and line numbers):
{previous_context}

Your job: produce the same JSON schema as usual (see included template), but reflect progress vs. previous feedback.

{consolidated_template}
"""

# ---------------------
# Utility: file discovery & chunking
# ---------------------
def get_changed_python_files(folder_path=None):
    if not folder_path:
        folder_path = SCRIPTS_DIRECTORY
    if not os.path.exists(folder_path):
        print(f"❌ Directory {folder_path} not found")
        return []
    all_files = []
    for pattern in FILE_PATTERNS:
        pattern_path = os.path.join(folder_path, pattern)
        found_files = glob.glob(pattern_path)
        recursive_pattern = os.path.join(folder_path, "**", pattern)
        found_files.extend(glob.glob(recursive_pattern, recursive=True))
        all_files.extend(found_files)
    all_files = sorted(list(set(all_files)))
    print(f"📁 Found {len(all_files)} code files in {folder_path} using patterns {FILE_PATTERNS}:")
    for file in all_files:
        file_type = "SQL" if file.lower().endswith('.sql') else "Python"
        print(f"  - {file} ({file_type})")
    return all_files

def chunk_large_file(code_text: str, max_chunk_size: int = 50000) -> list:
    if len(code_text) <= max_chunk_size:
        return [code_text]
    lines = code_text.split('\n')
    chunks, current_chunk, current_size = [], [], 0
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

# ---------------------
# Snowflake Cortex call
# ---------------------
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

def build_prompt_for_individual_review(code_text: str, filename: str = "code_file", python_detected=None) -> str:
    if python_detected:
        # Add a small appendix with precise lines to bias the LLM into quoting correct lines
        appendix = "\n\n# Python Pre-Scan (line-anchored hints)\n"
        for issue in python_detected[:10]:
            appendix += f"- Line {issue['line_number']}: {issue['severity']} — {issue['description']} ({issue['function_context']})\n"
    else:
        appendix = ""
    prompt = PROMPT_TEMPLATE_INDIVIDUAL.replace("{PY_CONTENT}", code_text + appendix)
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

# ---------------------
# Scoring
# ---------------------
def calculate_executive_quality_score(findings: list, total_lines_of_code: int) -> int:
    if not findings or len(findings) == 0:
        return 100
    base_score = 100
    total_deductions = 0
    severity_weights = {"Critical": 12, "High": 4, "Medium": 1.5, "Low": 0.3}
    severity_counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    print(f"  📊 Scoring {len(findings)} findings...")
    for f in findings:
        sev = str(f.get("severity", "")).strip()
        if sev in severity_counts:
            severity_counts[sev] += 1
        else:
            print(f"    ⚠️ UNRECOGNIZED SEVERITY: '{sev}' - SKIPPING")
    print(f"  📈 Severity breakdown: Critical={severity_counts['Critical']}, High={severity_counts['High']}, Medium={severity_counts['Medium']}, Low={severity_counts['Low']}")
    for sev, cnt in severity_counts.items():
        if cnt == 0:
            continue
        w = severity_weights[sev]
        if sev == "Critical":
            deduction = min(30, w * cnt if cnt <= 2 else w * 2 + (cnt - 2) * (w + 3))
        elif sev == "High":
            deduction = min(25, w * cnt if cnt <= 10 else w * 10 + (cnt - 10) * (w + 1))
        else:
            deduction = w * cnt
            deduction = min(20, deduction) if sev == "Medium" else min(10, deduction)
        total_deductions += deduction
        print(f"    {sev}: {cnt} issues = -{deduction:.1f} points (capped)")
    if total_lines_of_code > 0:
        affected_ratio = min(1.0, len(findings) / max(1, total_lines_of_code))
        if affected_ratio > 0.4:
            coverage_penalty = min(5, int(affected_ratio * 20))
            total_deductions += coverage_penalty
            print(f"    Coverage penalty: -{coverage_penalty} points ({affected_ratio:.1%} affected)")
    if severity_counts["Critical"] >= 3:
        total_deductions += 10
        print(f"    Executive threshold penalty: -10 points (3+ critical issues)")
    if severity_counts["Critical"] + severity_counts["High"] >= 20:
        total_deductions += 5
        print(f"    Production readiness penalty: -5 points (20+ critical/high issues)")
    final_score = max(0, base_score - int(total_deductions))
    print(f"  🎯 Final calculation: {base_score} - {int(total_deductions)} = {final_score}")
    if final_score >= 85: return min(100, final_score)
    if final_score >= 70: return final_score
    if final_score >= 50: return final_score
    return max(30, final_score)

# ---------------------
# Logging table (append-only, with comparison + files)
# ---------------------
def setup_review_log_table():
    """Create table if missing—no drops. Includes COMPARISON_RESULT + FILES_ANALYZED."""
    global database_available
    if not database_available:
        return False
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {current_database}.{current_schema}.CODE_REVIEW_LOG (
            REVIEW_ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
            PULL_REQUEST_NUMBER INTEGER,
            COMMIT_SHA VARCHAR(40),
            REVIEW_SUMMARY VARIANT,
            DETAILED_FINDINGS_JSON VARIANT,
            COMPARISON_RESULT VARIANT,
            FILES_ANALYZED VARIANT,
            REVIEW_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        session.sql(create_table_query).collect()
        print(f"✅ Review log table ready in {current_database}.{current_schema}")
        return True
    except Exception as e:
        print(f"❌ Failed to create review log table: {e}")
        return False

def store_review_log(pull_request_number, commit_sha, executive_summary, consolidated_json, processed_files):
    """Append review rows (never overwrite)."""
    global database_available
    if not database_available:
        print("  ⚠️ Database not available - cannot store review")
        return False
    try:
        findings = consolidated_json.get("detailed_findings", [])
        comparison_result = consolidated_json.get("previous_issues_resolved", [])
        files_analysed = processed_files or []
        insert_sql = f"""
        INSERT INTO {current_database}.{current_schema}.CODE_REVIEW_LOG
            (PULL_REQUEST_NUMBER, COMMIT_SHA, REVIEW_SUMMARY, DETAILED_FINDINGS_JSON, COMPARISON_RESULT, FILES_ANALYZED)
        SELECT ?, ?, PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?)
        """
        params = [
            pull_request_number,
            commit_sha,
            json.dumps(consolidated_json) if consolidated_json else None,  # Full JSON
            json.dumps(findings) if findings else None,                    # Findings only
            json.dumps(comparison_result) if comparison_result else None,  # Comparison only
            json.dumps(files_analysed)                                     # Files analyzed
        ]
        session.sql(insert_sql, params=params).collect()
        print(f"  ✅ Review appended to {current_database}.{current_schema}.CODE_REVIEW_LOG")

        count_query = f"""
        SELECT COUNT(*) as TOTAL_REVIEWS
        FROM {current_database}.{current_schema}.CODE_REVIEW_LOG
        WHERE PULL_REQUEST_NUMBER = {pull_request_number}
        """
        result = session.sql(count_query).collect()
        total_reviews = result[0]['TOTAL_REVIEWS'] if result else 0
        print(f"  📊 Total reviews for PR #{pull_request_number}: {total_reviews}")
        return True
    except Exception as e:
        print(f"  ❌ Failed to store review: {e}")
        import traceback; traceback.print_exc()
        return False

# ---------------------
# Previous review retrieval (hybrid)
# ---------------------
def get_previous_reviews_hybrid(pull_request_number, limit=3):
    """Return (previous_context_text, previous_findings_list) for the PR."""
    global database_available
    if not database_available:
        return None, []
    try:
        query = f"""
        SELECT REVIEW_SUMMARY, DETAILED_FINDINGS_JSON, FILES_ANALYZED, REVIEW_TIMESTAMP, COMMIT_SHA
        FROM {current_database}.{current_schema}.CODE_REVIEW_LOG
        WHERE PULL_REQUEST_NUMBER = {pull_request_number}
        ORDER BY REVIEW_TIMESTAMP DESC
        LIMIT {limit}
        """
        rows = session.sql(query).collect()
        if not rows:
            print("  📋 No previous reviews found for this PR")
            return None, []

        previous_findings = []
        parts = []
        for idx, row in enumerate(rows):
            summary = json.loads(str(row['REVIEW_SUMMARY'])) if row['REVIEW_SUMMARY'] else {}
            findings = json.loads(str(row['DETAILED_FINDINGS_JSON'])) if row['DETAILED_FINDINGS_JSON'] else []
            files = json.loads(str(row['FILES_ANALYZED'])) if row['FILES_ANALYZED'] else []
            for f in findings:
                ln = f.get('line_number', 'N/A')
                if isinstance(ln, str) and ln.isdigit():
                    ln = int(ln)
                elif not isinstance(ln, int):
                    ln = 'N/A'
                previous_findings.append({
                    'filename': f.get('filename', 'unknown'),
                    'line_number': ln,
                    'original_line_number': ln,
                    'severity': f.get('severity', 'Medium'),
                    'finding': f.get('finding', ''),
                    'function_context': f.get('function_context', 'global scope'),
                    'category': f.get('category', 'General'),
                    'review_timestamp': row['REVIEW_TIMESTAMP'],
                    'commit_sha': row['COMMIT_SHA']
                })
            date_str = row['REVIEW_TIMESTAMP'].strftime('%Y-%m-%d %H:%M') if row['REVIEW_TIMESTAMP'] else 'Unknown'
            parts.append(
                f"Review #{idx+1} from {date_str} (Commit: {row['COMMIT_SHA'][:8]}):\n"
                f"- Quality Score: {summary.get('quality_score', 'N/A')}\n"
                f"- Findings: {len(findings)}\n"
                f"- Files: {', '.join(files) if files else 'N/A'}\n"
                f"- Critical: {sum(1 for f in findings if f.get('severity') == 'Critical')}, "
                f"High: {sum(1 for f in findings if f.get('severity') == 'High')}\n"
            )
        context = "\n".join(parts)
        print(f"  📊 Processed {len(previous_findings)} previous findings across {len(rows)} review(s)")
        return context, previous_findings
    except Exception as e:
        print(f"  ⚠️ Error retrieving previous reviews: {e}")
        return None, []

# ---------------------
# Comparison logic (current vs previous)
# ---------------------
def _similarity_score(curr_desc, prev_desc):
    if not curr_desc or not prev_desc:
        return 0
    cw = set(re.findall(r"[a-zA-Z0-9_]+", curr_desc.lower()))
    pw = set(re.findall(r"[a-zA-Z0-9_]+", prev_desc.lower()))
    if not cw or not pw:
        return 0
    return min(20, len(cw.intersection(pw)) * 2)

def _line_proximity_score(curr_line, prev_line):
    try:
        if curr_line == 'N/A' or prev_line == 'N/A':
            return 0
        d = abs(int(curr_line) - int(prev_line))
        if d == 0: return 30
        if d <= 5: return 20
        if d <= 10: return 10
        return 0
    except Exception:
        return 0

def compare_findings_with_previous(current_findings, previous_findings):
    """Return (enhanced_current_findings, previous_issues_resolved_list)."""
    if not previous_findings:
        return current_findings, []

    print(f"  🔄 Comparing {len(current_findings)} current findings with {len(previous_findings)} previous findings")
    enhanced = []
    # Map each current finding to best previous (if any)
    for curr in current_findings:
        best, best_score = None, 0
        for prev in previous_findings:
            score = 0
            # File name match weight
            if curr.get('filename','') == prev.get('filename',''):
                score += 50
            # Line proximity
            score += _line_proximity_score(curr.get('line_number','N/A'), prev.get('line_number','N/A'))
            # Text similarity
            score += _similarity_score(curr.get('finding',''), prev.get('finding',''))
            if score > best_score:
                best_score, best = score, prev
        new_curr = curr.copy()
        if best and best_score >= 40:  # match threshold
            new_curr['is_previous_issue'] = 'Yes'
            new_curr['original_line_number'] = best.get('original_line_number', 'N/A')
            prev_sev = best.get('severity', '')
            cur_sev = curr.get('severity', '')
            if cur_sev == prev_sev:
                new_curr['resolution_status'] = 'NOT_ADDRESSED'
            elif cur_sev in ['Critical','High'] and prev_sev in ['Medium','Low']:
                new_curr['resolution_status'] = 'WORSENED'
            elif cur_sev in ['Medium','Low'] and prev_sev in ['Critical','High']:
                new_curr['resolution_status'] = 'PARTIALLY_RESOLVED'
            else:
                new_curr['resolution_status'] = 'MODIFIED'
        else:
            new_curr['is_previous_issue'] = 'No'
            new_curr['resolution_status'] = 'NEW'
        enhanced.append(new_curr)

    # Build previous_issues_resolved array from the perspective of previous items
    previous_resolution = []
    for prev in previous_findings:
        # find closest current
        best, best_score = None, 0
        for curr in current_findings:
            score = 0
            if curr.get('filename','') == prev.get('filename',''):
                score += 50
            score += _line_proximity_score(curr.get('line_number','N/A'), prev.get('line_number','N/A'))
            score += _similarity_score(curr.get('finding',''), prev.get('finding',''))
            if score > best_score:
                best_score, best = score, curr
        if best and best_score >= 40:
            status = 'NOT_ADDRESSED'  # default if still present
            if best.get('severity') in ['Medium','Low'] and prev.get('severity') in ['Critical','High']:
                status = 'PARTIALLY_RESOLVED'
            if best.get('severity') in ['Critical','High'] and prev.get('severity') in ['Medium','Low']:
                status = 'WORSENED'
            previous_resolution.append({
                "original_issue": prev.get('finding',''),
                "status": status,
                "details": f"Matched in current review for file {prev.get('filename','')}."
                           f" Prev line {prev.get('line_number','N/A')} → Curr line {best.get('line_number','N/A')}.",
                "original_line_number": prev.get('line_number','N/A'),
                "current_line_number": best.get('line_number','N/A'),
                "filename": prev.get('filename','unknown')
            })
        else:
            previous_resolution.append({
                "original_issue": prev.get('finding',''),
                "status": "RESOLVED",
                "details": f"No matching issue found in current review for file {prev.get('filename','')}.",
                "original_line_number": prev.get('line_number','N/A'),
                "current_line_number": "N/A",
                "filename": prev.get('filename','unknown')
            })

    print(f"  📊 Comparison: {sum(1 for f in enhanced if f.get('resolution_status')=='NEW')} new,"
          f" {sum(1 for f in enhanced if f.get('is_previous_issue')=='Yes')} continuing,"
          f" {sum(1 for p in previous_resolution if p.get('status')=='RESOLVED')} resolved")
    return enhanced, previous_resolution

# ---------------------
# Display formatting (exec report)
# ---------------------
def format_executive_pr_display(json_response: dict, processed_files: list,
                                has_previous_context: bool = False,
                                previous_findings_count: int = 0) -> str:
    summary = json_response.get("executive_summary", "Technical analysis completed")
    findings = json_response.get("detailed_findings", [])
    quality_score = json_response.get("quality_score", 75)
    business_impact = json_response.get("business_impact", "MEDIUM")
    security_risk = json_response.get("security_risk_level", "MEDIUM")
    tech_debt = json_response.get("technical_debt_score", "MEDIUM")
    maintainability = json_response.get("maintainability_rating", "FAIR")
    immediate_actions = json_response.get("immediate_actions", [])
    previous_issues = json_response.get("previous_issues_resolved", [])

    critical_count = sum(1 for f in findings if str(f.get("severity","")).upper()=="CRITICAL")
    high_count = sum(1 for f in findings if str(f.get("severity","")).upper()=="HIGH")
    medium_count = sum(1 for f in findings if str(f.get("severity","")).upper()=="MEDIUM")
    low_count = sum(1 for f in findings if str(f.get("severity","")).upper()=="LOW")

    python_files = [f for f in processed_files if f.lower().endswith('.py')]
    sql_files = [f for f in processed_files if f.lower().endswith('.sql')]

    python_critical = sum(1 for f in findings if f.get("filename","").lower().endswith('.py') and str(f.get("severity","")).upper()=="CRITICAL")
    python_high = sum(1 for f in findings if f.get("filename","").lower().endswith('.py') and str(f.get("severity","")).upper()=="HIGH")
    sql_critical = sum(1 for f in findings if f.get("filename","").lower().endswith('.sql') and str(f.get("severity","")).upper()=="CRITICAL")
    sql_high = sum(1 for f in findings if f.get("filename","").lower().endswith('.sql') and str(f.get("severity","")).upper()=="HIGH")

    risk_emoji = {"LOW":"🟢","MEDIUM":"🟡","HIGH":"🟠","CRITICAL":"🔴"}
    quality_emoji = "🟢" if quality_score >= 80 else ("🟡" if quality_score >= 60 else "🔴")

    display_text = f"""# 📊 Executive Code Review Report

**Files Analyzed:** {len(processed_files)} | **Date:** {datetime.now().strftime('%Y-%m-%d')} | **Database:** {current_database}.{current_schema}

## 🎯 Executive Summary
{summary}

## 📈 Quality Dashboard

| Metric | Score | Status | Business Impact |
|--------|-------|--------|-----------------|
| **Overall Quality** | {quality_score}/100 | {quality_emoji} | {business_impact} |
| **Security Risk** | {security_risk} | {risk_emoji.get(security_risk,'🟡')} | —
| **Technical Debt** | {tech_debt} | {risk_emoji.get(tech_debt,'🟡')} | {len(findings)} items |
| **Maintainability** | {maintainability} | {risk_emoji.get(maintainability,'🟡')} | — |

## 🔍 Issue Distribution

| Severity | Count | Priority |
|----------|-------|---------|
| 🔴 Critical | {critical_count} | Immediate |
| 🟠 High | {high_count} | This sprint |
| 🟡 Medium | {medium_count} | Next release |
| 🟢 Low | {low_count} | Backlog |

## 📁 File Analysis Breakdown

| File Type | Count | Critical | High |
|-----------|-------|---------|------|
| 🐍 Python | {len(python_files)} | {python_critical} | {python_high} |
| 🗄️ SQL | {len(sql_files)} | {sql_critical} | {sql_high} |
"""

    # Progression section
    if has_previous_context and previous_findings_count is not None:
        new_count = sum(1 for f in findings if f.get('resolution_status') == 'NEW')
        continuing_count = sum(1 for f in findings if f.get('is_previous_issue') == 'Yes')
        resolved_count = sum(1 for p in (json_response.get("previous_issues_resolved") or []) if p.get("status")=="RESOLVED")
        display_text += f"""
## 📊 Review Progression

| Status | Count |
|--------|-------|
| 🆕 New | {new_count} |
| 🔄 Continuing | {continuing_count} |
| ✅ Resolved from previous | {resolved_count} |
| 📋 Previous total (reference) | {previous_findings_count} |

"""

    # Previous issues resolution table
    if previous_issues:
        display_text += """<details>
<summary><strong>📈 Previous Issues Resolution Status</strong> (Click to expand)</summary>

| File | Original Line | Current Line | Status | Previous Issue |
|------|---------------|--------------|--------|----------------|
"""
        for issue in previous_issues[:40]:
            status = issue.get("status","UNKNOWN")
            status_emoji = {"RESOLVED":"✅","PARTIALLY_RESOLVED":"⚠️","NOT_ADDRESSED":"❌","WORSENED":"🔴"}.get(status,"❓")
            display_text += f"| {issue.get('filename','N/A')} | {issue.get('original_line_number','N/A')} | {issue.get('current_line_number','N/A')} | {status_emoji} {status} | {issue.get('original_issue','')[:80]} |\n"
        display_text += "\n</details>\n\n"

    # Findings table
    if findings:
        display_text += """<details>
<summary><strong>🔍 Detailed Findings (with line numbers)</strong> (Click to expand)</summary>

| Priority | File | Line | Function/Context | Issue |
|----------|------|------|------------------|-------|
"""
        severity_order = {"Critical":1,"High":2,"Medium":3,"Low":4}
        sorted_findings = sorted(findings, key=lambda x: severity_order.get(str(x.get("severity","Low")), 4))
        for f in sorted_findings[:50]:
            sev = str(f.get("severity","Medium"))
            priority_emoji = {"Critical":"🔴","High":"🟠","Medium":"🟡","Low":"🟢"}.get(sev,"🟡")
            filename = f.get("filename","N/A")
            line = f.get("line_number","N/A")
            ctx = f.get("function_context","global scope")
            issue = str(f.get("finding",""))[:120]
            display_text += f"| {priority_emoji} {sev} | {filename} | {line} | {ctx} | {issue} |\n"
        display_text += "\n</details>\n\n"

    if immediate_actions:
        display_text += """<details>
<summary><strong>⚡ Immediate Actions</strong> (Click to expand)</summary>

"""
        for i, action in enumerate(immediate_actions, 1):
            display_text += f"{i}. {action}\n"
        display_text += "\n</details>\n\n"

    display_text += f"""---

**📋 Review Summary:** {len(findings)} findings | **🎯 Quality Score:** {quality_score}/100

*🔬 Powered by Snowflake Cortex AI • Hybrid Python Line Detection • Stored in {current_database}.{current_schema}*
"""
    return display_text

# ---------------------
# Main
# ---------------------
def main():
    if len(sys.argv) >= 5:
        output_folder_path = sys.argv[2]
        try:
            pull_request_number = int(sys.argv[3]) if sys.argv[3] and sys.argv[3].strip() else None
        except (ValueError, IndexError):
            print(f"⚠️  Warning: Invalid or empty PR number '{sys.argv[3] if len(sys.argv) > 3 else 'None'}', using None")
            pull_request_number = None
        commit_sha = sys.argv[4]
        print(f"📁 Command line mode: Using {SCRIPTS_DIRECTORY} directory instead of '{sys.argv[1]}'")
        code_files = get_changed_python_files(SCRIPTS_DIRECTORY)
        if not code_files:
            print(f"❌ No Python/SQL files found in {SCRIPTS_DIRECTORY} using patterns {FILE_PATTERNS}")
            return
    else:
        code_files = get_changed_python_files(SCRIPTS_DIRECTORY)
        if not code_files:
            print(f"❌ No Python/SQL files found in {SCRIPTS_DIRECTORY} using patterns {FILE_PATTERNS}")
            return
        output_folder_path = "output_reviews"
        pull_request_number = 0
        commit_sha = "test"
        print(f"Running in dynamic pattern mode with {len(code_files)} code files from {SCRIPTS_DIRECTORY}")

    if os.path.exists(output_folder_path):
        import shutil; shutil.rmtree(output_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)

    all_individual_reviews = []
    processed_files = []

    print("\n🔍 STAGE 1: Individual File Analysis (Hybrid)")
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
                python_detected_issues = []
            else:
                # Python-based pre-scan with line numbers
                python_detected_issues = analyze_code_issues_with_lines(code_content, filename)
                print(f"  🔍 Python pre-scan found {len(python_detected_issues)} line-anchored hints")

                chunks = chunk_large_file(code_content)
                print(f"  File split into {len(chunks)} chunk(s)")
                chunk_reviews = []
                for i, chunk in enumerate(chunks):
                    chunk_name = f"{filename}_chunk_{i+1}" if len(chunks) > 1 else filename
                    print(f"  Processing chunk: {chunk_name}")
                    individual_prompt = build_prompt_for_individual_review(chunk, chunk_name, python_detected_issues)
                    review_text = review_with_cortex(MODEL, individual_prompt, session)
                    chunk_reviews.append(review_text)

                review_text = "\n\n".join([f"## Chunk {i+1}\n{rv}" for i, rv in enumerate(chunk_reviews)]) if len(chunk_reviews) > 1 else chunk_reviews[0]

            all_individual_reviews.append({
                "filename": filename,
                "review_feedback": review_text,
                "python_detected_issues": python_detected_issues
            })

            out_file = os.path.join(output_folder_path, f"{Path(filename).stem}_individual_review.md")
            with open(out_file, 'w', encoding='utf-8') as outfile:
                outfile.write(review_text)
            print(f"  ✅ Individual review saved: {os.path.basename(out_file)}")

        except Exception as e:
            print(f"  ❌ Error processing {filename}: {e}")
            all_individual_reviews.append({
                "filename": filename,
                "review_feedback": f"ERROR: Could not generate review. Reason: {e}",
                "python_detected_issues": []
            })

    print(f"\n🔄 STAGE 2: Executive Consolidation + Previous Comparison")
    print("=" * 60)
    if not all_individual_reviews:
        print("❌ No reviews to consolidate")
        return

    try:
        if database_available:
            setup_review_log_table()

        # Previous reviews (hybrid)
        previous_review_context, previous_findings = (None, [])
        has_previous_context = False
        if pull_request_number and pull_request_number != 0 and database_available:
            previous_review_context, previous_findings = get_previous_reviews_hybrid(pull_request_number, limit=3)
            has_previous_context = bool(previous_review_context)
            if has_previous_context:
                print(f"  📋 Previous context available with {len(previous_findings)} finding(s)")
            else:
                print("  📋 Initial commit review (no previous context)")
        elif not database_available:
            print("  ⚠️ Database not available - cannot retrieve previous reviews")

        combined_reviews_json = json.dumps(all_individual_reviews, indent=2)
        print(f"  Combined reviews: {len(combined_reviews_json)} characters")

        consolidation_prompt = build_prompt_for_consolidated_summary(
            combined_reviews_json,
            previous_review_context,
            pull_request_number
        )
        consolidation_prompt = consolidation_prompt.replace(
            "{MAX_CHARS_FOR_FINAL_SUMMARY_FILE}",
            str(MAX_CHARS_FOR_FINAL_SUMMARY_FILE)
        )

        consolidated_raw = review_with_cortex(MODEL, consolidation_prompt, session)

        try:
            consolidated_json = json.loads(consolidated_raw)
            print("  ✅ Consolidated JSON parsed")
        except json.JSONDecodeError:
            print("  ⚠️ Raw consolidation not pure JSON; attempting to extract...")
            m = re.search(r'\{.*\}\s*$', consolidated_raw, re.DOTALL)
            if m:
                consolidated_json = json.loads(m.group(0))
            else:
                consolidated_json = {
                    "executive_summary": "Consolidation failed - fallback",
                    "quality_score": 75,
                    "business_impact": "MEDIUM",
                    "detailed_findings": [],
                    "immediate_actions": [],
                    "previous_issues_resolved": []
                }

        # If we have previous, do Python-based comparison and inject line-aware statuses
        if has_previous_context and previous_findings:
            current_findings = consolidated_json.get("detailed_findings", [])
            enhanced_findings, previous_resolution = compare_findings_with_previous(current_findings, previous_findings)
            consolidated_json["detailed_findings"] = enhanced_findings
            consolidated_json["previous_issues_resolved"] = previous_resolution
            print(f"  🔄 Hybrid comparison applied: {len(previous_resolution)} previous issues evaluated")

        # Re-score with rule-based calc
        findings_now = consolidated_json.get("detailed_findings", [])
        total_lines = sum(len(item.get("review_feedback","").split('\n')) for item in all_individual_reviews)
        rule_score = calculate_executive_quality_score(findings_now, total_lines)
        consolidated_json["quality_score"] = rule_score
        print(f"  🎯 Rule-based quality score: {rule_score}/100")

        # Pretty executive summary for artifact
        exec_md = format_executive_pr_display(
            consolidated_json,
            [x for x in (processed_files or [])],
            has_previous_context,
            len(previous_findings)
        )
        with open(os.path.join(output_folder_path, "consolidated_executive_summary.md"), 'w', encoding='utf-8') as f:
            f.write(exec_md)
        print("  ✅ Executive summary saved: consolidated_executive_summary.md")

        # Save full JSON
        with open(os.path.join(output_folder_path, "consolidated_data.json"), 'w', encoding='utf-8') as f:
            json.dump(consolidated_json, f, indent=2)

        # Build review_output.json (compat)
        criticals = []
        for fnd in [f for f in findings_now if str(f.get("severity","")).upper()=="CRITICAL"]:
            criticals.append({
                "line": fnd.get("line_number", "N/A"),
                "issue": fnd.get("finding", "Critical issue"),
                "recommendation": fnd.get("recommendation", fnd.get("finding","")),
                "severity": fnd.get("severity", "Critical"),
                "filename": fnd.get("filename","N/A"),
                "business_impact": fnd.get("business_impact",""),
                "description": fnd.get("finding","")
            })

        review_output_payload = {
            "full_review": exec_md,
            "full_review_markdown": exec_md,
            "full_review_json": consolidated_json,
            "criticals": criticals,
            "critical_summary": "",
            "critical_count": len(criticals),
            "file": processed_files[0] if processed_files else "unknown",
            "timestamp": datetime.now().isoformat(),
            "has_previous_context": has_previous_context,
            "previous_findings_count": len(previous_findings),
            "hybrid_analysis": True
        }
        with open("review_output.json", "w", encoding='utf-8') as f:
            json.dump(review_output_payload, f, indent=2, ensure_ascii=False)
        print("  ✅ review_output.json saved")

        # Append to DB
        if pull_request_number and pull_request_number != 0 and database_available:
            store_review_log(pull_request_number, commit_sha, exec_md, consolidated_json, processed_files)

        if 'GITHUB_OUTPUT' in os.environ:
            delimiter = str(uuid.uuid4())
            with open(os.environ['GITHUB_OUTPUT'], 'a') as gh_out:
                gh_out.write(f'consolidated_summary_text<<{delimiter}\n')
                gh_out.write(f'{exec_md}\n')
                gh_out.write(f'{delimiter}\n')
            print("  ✅ GitHub Actions output written")

        print("\n🎉 HYBRID TWO-STAGE ANALYSIS COMPLETED!")
        print("=" * 80)
        print(f"📁 Files processed: {len(processed_files)}")
        print(f"🔍 Individual reviews: {len(all_individual_reviews)} (PROMPT 1 + Python pre-scan)")
        print(f"📊 Executive summary: 1 (PROMPT 2 + Hybrid comparison)")
        print(f"🎯 Quality Score: {consolidated_json.get('quality_score','N/A')}/100")
        print(f"📈 Current Findings: {len(findings_now)}")
        if has_previous_context:
            print(f"🔄 Previous context: ✅ Included ({len(previous_findings)} prior findings considered)")
        else:
            print("🔄 Previous context: ❌ Initial commit review")
        if database_available:
            print(f"💾 Database logging: ✅ Appended to {current_database}.{current_schema}")
        else:
            print("💾 Database logging: ❌ Not available")
        print("🔧 Enhancements: Python line detection • Line-aware comparison • Append-only logging")

    except Exception as e:
        print(f"❌ Consolidation error: {e}")
        import traceback; traceback.print_exc()

        fallback_summary = f"""# 📊 Code Review Report (Fallback)

**Files Analyzed:** {len(processed_files)} | **Date:** {datetime.now().strftime('%Y-%m-%d')}

## ⚠️ Review Status
Technical analysis completed with {len(all_individual_reviews)} individual file reviews.
Executive consolidation encountered an error but individual reviews are available.

**Files:**
""" + "\n".join(f"- {f}" for f in processed_files)

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
        print("  ⚠️ Fallback review_output.json created")

if __name__ == "__main__":
    try:
        main()
    finally:
        if 'session' in locals():
            session.close()
            print("\n🔒 Session closed")
