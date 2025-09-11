#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automated LLM Code Review — polished output for PRs
- Professional Markdown with subtle emojis & icons
- ANSI-colored console logs
- LOW-severity filtered for PR body
- Critical findings extracted for inline comments
- Credentials read from env (no hardcoded secrets)
"""

import os, json, sys
from pathlib import Path
from datetime import datetime
from snowflake.snowpark import Session
import pandas as pd

# ======================================================
# 🎛️ Configuration
# ======================================================
MODEL = "llama3.1-70b"
MAX_CODE_CHARS = 40_000
FILE_TO_REVIEW = os.environ.get("FILE_TO_REVIEW", "scripts/simple_test.py")

# Snowflake credentials (read from env)
SF_ACCOUNT   = os.environ.get("SNOWFLAKE_ACCOUNT", "XKB93357.us-west-2")
SF_USER      = os.environ.get("SNOWFLAKE_USER", "MANISHAT007")
SF_PASSWORD  = os.environ.get("SNOWFLAKE_PASSWORD")  # set in CI/terminal
SF_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "ORGADMIN")
SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "MY_DB")
SF_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

# ======================================================
# 🎨 Console colors (ANSI) — subtle & professional
# ======================================================
class C:
    RST = "\033[0m"
    B  = "\033[1m"
    DIM= "\033[2m"
    GR = "\033[32m"
    YL = "\033[33m"
    RD = "\033[31m"
    BL = "\033[34m"
    CY = "\033[36m"

def info(msg):   print(f"{C.CY}ℹ {msg}{C.RST}")
def good(msg):   print(f"{C.GR}✅ {msg}{C.RST}")
def warn(msg):   print(f"{C.YL}⚠️  {msg}{C.RST}")
def bad(msg):    print(f"{C.RD}❌ {msg}{C.RST}")
def step(msg):   print(f"{C.BL}{C.B}▶ {msg}{C.RST}")

# ======================================================
# 🔐 Safety check for secrets
# ======================================================
if not SF_PASSWORD:
    bad("Missing SNOWFLAKE_PASSWORD env var.")
    print("Set it and rerun, e.g.:")
    print("  export SNOWFLAKE_PASSWORD='••••••••'")
    sys.exit(1)

# ======================================================
# 🧊 Snowflake session
# ======================================================
cfg = {
    "account":   SF_ACCOUNT,
    "user":      SF_USER,
    "password":  SF_PASSWORD,
    "role":      SF_ROLE,
    "warehouse": SF_WAREHOUSE,
    "database":  SF_DATABASE,
    "schema":    SF_SCHEMA,
}
session = Session.builder.configs(cfg).create()

# ======================================================
# 🧠 Review Prompt
# ======================================================
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
- **Evidence:** Quote the exact changed snippet (`+` lines) and cite the new line number.
- **Severity:** Assign {Low | Medium | High | Critical}.
- **Impact & Action:** Briefly explain the issue and provide a minimal, safe correction.
- **Non-trivial:** Skip purely stylistic nits (import order, line length) that a linter would catch.

# HARD CONSTRAINTS (For accuracy & anti-hallucination)
- Do NOT propose APIs that don't exist for the imported modules.
- Treat parameters like `db_path` as correct dependency injection; do NOT hardcode them.
- NEVER suggest logging sensitive user data or internal paths. Prefer non-reversible fingerprints if context is needed.
- Do NOT recommend removing correct type hints or docstrings.
- If code in the diff is already correct and idiomatic, do NOT invent problems.

---
# OUTPUT FORMAT (Strict, professional, audit-ready)
## Code Review Summary
*A 2–3 sentence high-level summary. Mention strengths and the most critical areas for improvement across all changed files.*

---
### Detailed Findings
*A list of all material findings. If no significant issues are found, state "No significant issues found."*
**File:** `{FILE_PATH}`
- **Severity:** {Critical | High | Medium | Low}
- **Line:** {line_number}
- **Function/Context:** `{function_name_if_applicable}`
- **Finding:** {Clear description of the issue, its impact, and a recommended correction.}
*(Repeat for each finding in each file)*

---
### Key Recommendations
*Provide 2–3 high-level, actionable recommendations for improving the overall quality of the codebase based on the findings. Do not repeat the findings themselves.*

---
# CODE DIFF TO REVIEW
{PY_CONTENT}
"""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return (PROMPT_TEMPLATE
            .replace("{PY_CONTENT}", code_text)
            .replace("{FILE_PATH}", FILE_TO_REVIEW))

# ======================================================
# 🤖 Call Cortex
# ======================================================
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
        step(f"Requesting review from {model}…")
        df = session.sql(query)
        result = df.collect()[0][0]

        # Attempt to parse JSON; if not JSON, wrap as text
        try:
            json_response = json.loads(result)
            good("Received structured JSON response.")
            return json_response
        except json.JSONDecodeError:
            warn("Response is plain text; formatting gracefully.")
            return {
                "summary": "Analysis completed (text format)",
                "detailed_findings": [],
                "key_recommendations": [],
                "raw_text": result
            }

    except Exception as e:
        bad(f"Cortex API error: {e}")
        return {
            "summary": f"Error occurred: {e}",
            "detailed_findings": [],
            "key_recommendations": ["Manual review recommended due to API error"]
        }

# ======================================================
# 🔎 Helpers
# ======================================================
SEV_ICONS = {
    "CRITICAL": "🟥",
    "HIGH":     "🔴",
    "MEDIUM":   "🟠",
    "LOW":      "🟡",
    "UNKNOWN":  "⚪",
}

def sev_icon(sev: str) -> str:
    return SEV_ICONS.get((sev or "UNKNOWN").upper(), "⚪")

def filter_low_severity(json_response: dict) -> dict:
    """Remove LOW severity findings from JSON response."""
    filtered = json_response.copy()
    if "detailed_findings" in filtered:
        original = len(filtered["detailed_findings"])
        filtered["detailed_findings"] = [
            f for f in filtered["detailed_findings"]
            if (f.get("severity","").upper() != "LOW")
        ]
        removed = original - len(filtered["detailed_findings"])
        info(f"Filtered out {removed} LOW-severity finding(s).")
    return filtered

def extract_critical_findings(json_response: dict) -> list:
    """Extract CRITICAL findings for inline comments (dynamic)."""
    findings = []
    for f in json_response.get("detailed_findings", []):
        if f.get("severity","").upper() == "CRITICAL" and f.get("line_number"):
            findings.append({
                "line": int(f["line_number"]),
                "issue": f.get("finding", "Critical issue"),
                "recommendation": f.get("finding", "Address this critical issue"),
                "severity": "CRITICAL",
            })
    info(f"Critical issues for inline comments: {len(findings)}")
    if findings:
        print(f"  Lines: {[c['line'] for c in findings]}")
    return findings

def _summarize_counts(findings: list) -> dict:
    counts = {"CRITICAL":0,"HIGH":0,"MEDIUM":0,"LOW":0,"UNKNOWN":0}
    for f in findings:
        s = (f.get("severity") or "UNKNOWN").upper()
        counts[s] = counts.get(s,0) + 1
    return counts

# ======================================================
# 🧾 PR Markdown Formatter (clean & executive)
# ======================================================
def format_for_pr_display(json_response: dict) -> str:
    """
    Build a professional, emoji-enhanced Markdown review suitable for GitHub PRs.
    Uses tables, icons, and collapsible details while staying conservative.
    """
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    title = f"🚦 **Automated LLM Code Review** — `{FILE_TO_REVIEW}`  \n_{ts}_\n"

    # If raw text, present minimal but polished wrapper
    if "raw_text" in json_response:
        raw = json_response["raw_text"].strip()
        body = f"\n> {raw[:800]}{'…' if len(raw) > 800 else ''}\n"
        footer = "\n---\n*Generated by Snowflake Cortex AI (llama3.1-70b)*"
        return title + body + footer

    summary = json_response.get("summary", "Code review completed.")
    findings = json_response.get("detailed_findings", [])
    recs = json_response.get("key_recommendations", [])

    counts = _summarize_counts(findings)
    badge_line = (
        f"**Findings:** "
        f"{SEV_ICONS['CRITICAL']} {counts.get('CRITICAL',0)}  ·  "
        f"{SEV_ICONS['HIGH']} {counts.get('HIGH',0)}  ·  "
        f"{SEV_ICONS['MEDIUM']} {counts.get('MEDIUM',0)}  ·  "
        f"{SEV_ICONS['LOW']} {counts.get('LOW',0)}"
    )

    # Must-fix subset
    must_fix = [f for f in findings if f.get("severity","").upper() in ("CRITICAL","HIGH")]

    # Header + TL;DR
    out = []
    out.append(title)
    out.append(f"**TL;DR:** {summary}\n")
    out.append(badge_line + "\n")

    if must_fix:
        out.append("\n**✅ Must Fix Before Merge**\n")
        for i, f in enumerate(must_fix, 1):
            sev = f.get("severity","").upper()
            line = f.get("line_number","N/A")
            ctx  = f.get("function_context") or f.get("function") or "N/A"
            txt  = f.get("finding","(no description)").strip()
            out.append(f"{i}. {sev_icon(sev)} **{sev.title()}** · L{line} · `{ctx}` — {txt}")

    # Detailed table
    out.append("\n<details>\n<summary><b>📄 Detailed Findings</b></summary>\n\n")
    if findings:
        out.append("| Sev | Line | Context | Finding |")
        out.append("|:---:|:----:|:-------:|:--------|")
        for f in findings:
            sev = (f.get("severity") or "UNKNOWN").upper()
            icon = sev_icon(sev)
            line = f.get("line_number","N/A")
            ctx  = f.get("function_context") or f.get("function") or "—"
            txt  = (f.get("finding","").strip() or "—").replace("\n"," ")
            out.append(f"| {icon} **{sev.title()}** | {line} | `{ctx}` | {txt} |")
    else:
        out.append("> **No significant issues found.**")
    out.append("\n</details>\n")

    # Recommendations
    if recs:
        out.append("\n**🧭 Key Recommendations**\n")
        for i, r in enumerate(recs, 1):
            out.append(f"{i}. {r}")

    # Legend (collapsible)
    out.append(
        "\n<details>\n<summary>Legend</summary>\n\n"
        f"- {SEV_ICONS['CRITICAL']} **Critical** — security/data loss or crash\n"
        f"- {SEV_ICONS['HIGH']} **High** — correctness gaps; blocks merge\n"
        f"- {SEV_ICONS['MEDIUM']} **Medium** — reliability/UX; quick fix\n"
        f"- {SEV_ICONS['LOW']} **Low** — polish/maintainability\n"
        "</details>\n"
    )

    out.append("\n---\n*Generated by Snowflake Cortex AI (llama3.1-70b)*")
    return "\n".join(out)

# ======================================================
# 🚀 Main
# ======================================================
if __name__ == "__main__":
    try:
        step(f"Reading target file: {FILE_TO_REVIEW}")
        if not os.path.exists(FILE_TO_REVIEW):
            bad(f"File not found: {FILE_TO_REVIEW}")
            sys.exit(1)

        code_text = Path(FILE_TO_REVIEW).read_text(encoding="utf-8")
        info(f"Chars to review: {len(code_text)} (max {MAX_CODE_CHARS})")

        step("Requesting Cortex review")
        report = review_with_cortex(MODEL, code_text)

        # Show raw JSON for traceability (truncated)
        print(f"{C.DIM}\n=== ORIGINAL RESPONSE (truncated if large) ==={C.RST}")
        try:
            raw_json = json.dumps(report, indent=2)
            print(raw_json[:2000] + ("…" if len(raw_json) > 2000 else ""))
        except Exception:
            print(str(report)[:2000])

        # Tabular view if structured
        detailed_findings = report.get("detailed_findings", [])
        if detailed_findings:
            df = pd.DataFrame(detailed_findings)
            print(f"{C.DIM}\n=== FINDINGS DATAFRAME ==={C.RST}")
            try:
                print(df.to_string(index=False))
            except Exception:
                print(df.head().to_string(index=False))
        else:
            info("No structured findings to tabulate.")

        # Filter out LOW findings for PR
        filtered_json = filter_low_severity(report.copy())

        # Extract CRITICAL for inline comments
        criticals = extract_critical_findings(filtered_json)

        # Build polished PR comment
        formatted_review = format_for_pr_display(filtered_json)

        # Persist output bundle
        output = {
            "full_review": formatted_review,
            "full_review_json": filtered_json,
            "criticals": criticals,
            "file": FILE_TO_REVIEW,
            "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "model": MODEL,
        }
        Path("review_output.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
        Path("review_output.md").write_text(formatted_review, encoding="utf-8")

        # Print PR Markdown for quick copy-paste
        print(f"{C.DIM}\n=== PR COMMENT (Markdown) ==={C.RST}")
        print(formatted_review[:4000] + ("…\n[truncated]" if len(formatted_review) > 4000 else ""))

        good("Review artifacts written:")
        print("  - review_output.json")
        print("  - review_output.md")
        good("Done.")

    except Exception as e:
        bad(f"Error: {e}")
        if 'session' in locals():
            session.close()
        sys.exit(1)
    finally:
        try:
            session.close()
        except Exception:
            pass
