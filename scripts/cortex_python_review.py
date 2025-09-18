import os, sys, json, re, uuid
from pathlib import Path
from snowflake.snowpark import Session
import pandas as pd
from datetime import datetime

# ---------------------
# Config
# ---------------------
MODEL = "openai-gpt-4.1"
# Safety limit to keep prompts compact 
MAX_CHARS_FOR_FINAL_SUMMARY_FILE = 65000
MAX_TOKENS_FOR_SUMMARY_INPUT = 100000

# For single file testing (when not using directory mode)
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
# PROMPT 1: Individual File Review
# ---------------------
PROMPT_TEMPLATE_INDIVIDUAL = """Please act as a principal-level Python code reviewer. Your review must be concise, accurate, and directly actionable, as it will be posted as a GitHub Pull Request comment.

---
# CONTEXT: HOW TO REVIEW (Apply Silently)

1.  **You are reviewing a code file for executive-level analysis.** Focus on business impact, technical debt, security risks, and maintainability.
2.  **Focus your review on the most critical aspects.** Prioritize findings that have business impact or security implications.
3.  **Infer context from the full code.** Base your review on the complete file provided.
4.  **Your entire response MUST be under 65,000 characters.** Prioritize findings with `High` or `Critical` severity. If the review is extensive, omit `Low` severity findings to meet the length constraint.

# REVIEW PRIORITIES (Strict Order)
1.  Security & Correctness
2.  Reliability & Error-handling
3.  Performance & Complexity
4.  Readability & Maintainability
5.  Testability

# ELIGIBILITY CRITERIA FOR FINDINGS (ALL must be met)
-   **Evidence:** Quote the exact code snippet and cite the line number.
-   **Severity:** Assign {Low | Medium | High | Critical}.
-   **Impact & Action:** Briefly explain the issue and provide a minimal, safe correction.
-   **Non-trivial:** Skip purely stylistic nits (e.g., import order, line length) that a linter would catch.

# HARD CONSTRAINTS (For accuracy & anti-hallucination)
-   Do NOT propose APIs that don't exist for the imported modules.
-   Treat parameters like `db_path` as correct dependency injection; do NOT call them hardcoded.
-   NEVER suggest logging sensitive user data or internal paths. Suggest non-reversible fingerprints if context is needed.
-   Do NOT recommend removing correct type hints or docstrings.
-   If code in the file is already correct and idiomatic, do NOT invent problems.

---
# OUTPUT FORMAT (Strict, professional, audit-ready)

Your entire response MUST be under 65,000 characters. Prioritize findings with High or Critical severity. If the review is extensive, omit Low severity findings to meet the length constraint.

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

# ---------------------
# PROMPT 2: Consolidation
# ---------------------
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
         -   **`severity`**: Assess and assign severity: "Low", "Medium", "High", or "Critical".
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

**CRITICAL INSTRUCTION FOR LARGE REVIEWS:**
Your entire response MUST be under {MAX_CHARS_FOR_FINAL_SUMMARY_FILE} characters. If the number of findings is very large, you MUST prioritize.
-   First, only include findings with **'Critical' and 'High' severity** in the `detailed_findings` array.
-   If there is still not enough space, summarize the 'Medium' severity findings in the main `executive_summary` field instead of listing them individually.
-   'Low' severity findings can be ignored if space is limited.

Here are the individual code reviews to process:
{ALL_REVIEWS_CONTENT}
"""

def build_prompt_for_individual_review(code_text: str, filename: str = "code_file") -> str:
    prompt = PROMPT_TEMPLATE_INDIVIDUAL.replace("{PY_CONTENT}", code_text)
    prompt = prompt.replace("{filename}", filename)
    return prompt

def build_prompt_for_consolidated_summary(all_reviews_content: str) -> str:
    return PROMPT_TEMPLATE_CONSOLIDATED.replace("{ALL_REVIEWS_CONTENT}", all_reviews_content)

def review_with_cortex(model, prompt_text: str, session) -> str:
    try:
        clean_prompt = prompt_text.replace("'", "''").replace("\\", "\\\\")
        
        query = f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                '{clean_prompt}'
            ) as response
        """
        
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

    if metrics:
        loc = metrics.get("lines_of_code", "N/A")
        complexity = metrics.get("complexity_score", "N/A")
        coverage_gaps = len(metrics.get("code_coverage_gaps", []))
        dep_risks = len(metrics.get("dependency_risks", []))
        
        display_text += f"""## 📊 Technical Metrics

| Metric | Value | Assessment |
|--------|-------|------------|
| **Lines of Code** | {loc} | {'🟢 Manageable' if isinstance(loc, int) and loc < 500 else '🟡 Monitor'} |
| **Complexity** | {complexity} | {risk_emoji.get(complexity, "🟡")} |
| **Coverage Gaps** | {coverage_gaps} areas | {'🟢 Good' if coverage_gaps < 3 else '🟡 Needs attention'} |
| **Dependency Risks** | {dep_risks} items | {'🟢 Low risk' if dep_risks < 3 else '🟡 Monitor'} |

"""

    if findings:
        display_text += """<details>
<summary><strong>🔍 Detailed Technical Findings</strong> (Click to expand)</summary>

| Priority | File | Line | Issue | Business Impact |
|----------|------|------|-------|-----------------|
"""
        
        severity_order = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}
        sorted_findings = sorted(findings, key=lambda x: severity_order.get(str(x.get("severity", "Low")), 4))
        
        for finding in sorted_findings[:15]:
            severity = str(finding.get("severity", "Medium"))
            filename = finding.get("filename", "N/A")
            line = finding.get("line_number", "N/A")
            issue = str(finding.get("finding", ""))[:100] + ("..." if len(str(finding.get("finding", ""))) > 100 else "")
            business_impact_text = str(finding.get("business_impact", ""))[:80] + ("..." if len(str(finding.get("business_impact", ""))) > 80 else "")
            
            priority_emoji = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}.get(severity, "🟡")
            
            display_text += f"| {priority_emoji} {severity} | {filename} | {line} | {issue} | {business_impact_text} |\n"
        
        display_text += "\n</details>\n\n"

    if strategic_recs:
        display_text += """## 🎯 Strategic Recommendations

<details>
<summary><strong>💡 Leadership Actions</strong> (Click to expand)</summary>

"""
        for i, rec in enumerate(strategic_recs, 1):
            display_text += f"{i}. {rec}\n"
        display_text += "\n</details>\n\n"

    if immediate_actions:
        display_text += """## ⚡ Immediate Actions Required

<details>
<summary><strong>🚨 Critical Tasks</strong> (Click to expand)</summary>

"""
        for i, action in enumerate(immediate_actions, 1):
            display_text += f"{i}. {action}\n"
        display_text += "\n</details>\n\n"

    display_text += f"""---

**📋 Review Summary:** {len(findings)} findings identified | **🎯 Quality Score:** {quality_score}/100 | **⚡ Critical Issues:** {critical_count}

*🔬 Powered by Snowflake Cortex AI • Two-Stage Executive Analysis*"""

    return display_text

def main():
    # FIXED: Handle command line arguments properly to avoid ValueError
    if len(sys.argv) >= 5:
        folder_path = sys.argv[1]
        output_folder_path = sys.argv[2]
        # FIXED: Handle empty or invalid PR number gracefully
        try:
            pull_request_number = int(sys.argv[3]) if sys.argv[3] and sys.argv[3].strip() else None
        except (ValueError, IndexError):
            print(f"⚠️  Warning: Invalid or empty PR number '{sys.argv[3] if len(sys.argv) > 3 else 'None'}', using None")
            pull_request_number = None
        commit_sha = sys.argv[4]
        directory_mode = True
    else:
        folder_path = None
        output_folder_path = "output_reviews"
        pull_request_number = 0
        commit_sha = "test"
        directory_mode = False
        print(f"Running in single-file mode with: {FILE_TO_REVIEW}")

    if os.path.exists(output_folder_path):
        import shutil
        shutil.rmtree(output_folder_path)
    os.makedirs(output_folder_path, exist_ok=True)

    all_individual_reviews = []
    processed_files = []

    print("\n🔍 STAGE 1: Individual File Analysis...")
    print("=" * 60)
    
    if directory_mode:
        files_to_process = [f for f in os.listdir(folder_path) if f.endswith((".py", ".sql"))]
    else:
        if not os.path.exists(FILE_TO_REVIEW):
            print(f"❌ File {FILE_TO_REVIEW} not found")
            return
        files_to_process = [FILE_TO_REVIEW]
        folder_path = os.path.dirname(FILE_TO_REVIEW)

    for filename in files_to_process:
        if directory_mode:
            file_path = os.path.join(folder_path, filename)
        else:
            file_path = filename
            filename = os.path.basename(filename)
            
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
        combined_reviews_json = json.dumps(all_individual_reviews, indent=2)
        print(f"  Combined reviews: {len(combined_reviews_json)} characters")

        consolidation_prompt = build_prompt_for_consolidated_summary(combined_reviews_json)
        consolidation_prompt = consolidation_prompt.replace("{MAX_CHARS_FOR_FINAL_SUMMARY_FILE}", str(MAX_CHARS_FOR_FINAL_SUMMARY_FILE))
        consolidated_raw = review_with_cortex(MODEL, consolidation_prompt, session)
        
        try:
            consolidated_json = json.loads(consolidated_raw)
            print("  ✅ Successfully parsed consolidated JSON response")
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
                    "immediate_actions": []
                }

        executive_summary = format_executive_pr_display(consolidated_json, processed_files)
        
        # ADDED: Compare with previous review if this is a subsequent commit
        previous_review_comparison = None
        if pull_request_number and pull_request_number != 0:
            try:
                # Setup table if it doesn't exist
                create_table_query = """
                CREATE TABLE IF NOT EXISTS CODE_REVIEW_LOG (
                    REVIEW_ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                    PULL_REQUEST_NUMBER INTEGER,
                    COMMIT_SHA VARCHAR(40),
                    REVIEW_SUMMARY VARCHAR,
                    REVIEW_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );
                """
                session.sql(create_table_query).collect()
                
                # Check for previous reviews
                query = f"""
                    SELECT REVIEW_SUMMARY FROM CODE_REVIEW_LOG 
                    WHERE PULL_REQUEST_NUMBER = {pull_request_number}
                    ORDER BY REVIEW_TIMESTAMP DESC 
                    LIMIT 2
                """
                result = session.sql(query).collect()
                
                if len(result) >= 1:
                    previous_review = result[0]["REVIEW_SUMMARY"][:3000]
                    current_review = executive_summary[:3000]
                    
                    comparison_prompt = f"""Compare the previous and current code reviews and identify what issues were resolved or improved. Be specific about which issues were addressed.

PREVIOUS REVIEW:
{previous_review}

CURRENT REVIEW:  
{current_review}

Respond in this format:
**Issues Resolved Since Last Review:**
- [Specific issue]: ✅ Resolved / ⚠️ Partially Resolved / ❌ Not Addressed

**New Issues Identified:**
- [New issue found in current review]

**Overall Progress:** Brief summary of improvement or regression
"""
                    
                    comparison_result = review_with_cortex(MODEL, comparison_prompt, session)
                    previous_review_comparison = comparison_result
                    print("  ✅ Generated comparison with previous review")
                    
            except Exception as e:
                print(f"  Warning: Could not compare with previous review: {e}")

        # Add comparison section to executive summary
        if previous_review_comparison:
            executive_summary += f"""

<details>
<summary><strong>📈 Progress Since Last Review</strong> (Click to expand)</summary>

{previous_review_comparison}

*This comparison analyzes changes since the previous commit review to track issue resolution progress.*

</details>
"""

        consolidated_path = os.path.join(output_folder_path, "consolidated_executive_summary.md")
        with open(consolidated_path, 'w', encoding='utf-8') as f:
            f.write(executive_summary)
        print(f"  ✅ Executive summary saved: consolidated_executive_summary.md")

        json_path = os.path.join(output_folder_path, "consolidated_data.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(consolidated_json, f, indent=2)

        # Generate review_output.json for inline_comment.py compatibility with proper field mapping
        criticals = []
        for f in consolidated_json.get("detailed_findings", []):
            if str(f.get("severity", "")).upper() == "CRITICAL":
                critical = {
                    "line": f.get("line_number", 1),
                    "issue": f.get("finding", "Critical issue found"),
                    "recommendation": f.get("recommendation", f.get("finding", "")),
                    "severity": f.get("severity", "Critical")
                }
                criticals.append(critical)

        review_output_data = {
            "full_review": executive_summary,
            "full_review_markdown": executive_summary,
            "full_review_json": consolidated_json,
            "criticals": criticals,
            "file": processed_files[0] if processed_files else "unknown",
            "timestamp": datetime.now().isoformat(),
            "previous_comparison": previous_review_comparison
        }

        with open("review_output.json", "w", encoding='utf-8') as f:
            json.dump(review_output_data, f, indent=2, ensure_ascii=False)
        print("  ✅ review_output.json saved for inline_comment.py compatibility")

        # Store current review for future comparisons
        if pull_request_number and pull_request_number != 0:
            try:
                insert_sql = """
                    INSERT INTO CODE_REVIEW_LOG (PULL_REQUEST_NUMBER, COMMIT_SHA, REVIEW_SUMMARY)
                    VALUES (?, ?, ?)
                """
                params = [pull_request_number, commit_sha, executive_summary[:10000]]  # Truncate for storage
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
        if previous_review_comparison:
            print(f"🔄 Comparison with previous review: ✅ Generated")
        
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
