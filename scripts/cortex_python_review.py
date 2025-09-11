#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automated LLM Code Review — polished output for PRs
- Professional Markdown with subtle emojis & icons
- ANSI-colored console logs
- LOW-severity filtered for PR body
- Critical findings extracted for inline comments
- Snowflake credentials hardcoded for POC (⚠ not secure)
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
FILE_TO_REVIEW = "scripts/simple_test.py"   # direct fallback

# ======================================================
# 🎨 Console colors (ANSI)
# ======================================================
class C:
    RST = "\033[0m"; B  = "\033[1m"; DIM= "\033[2m"
    GR = "\033[32m"; YL = "\033[33m"; RD = "\033[31m"
    BL = "\033[34m"; CY = "\033[36m"

def info(msg):   print(f"{C.CY}ℹ {msg}{C.RST}")
def good(msg):   print(f"{C.GR}✅ {msg}{C.RST}")
def warn(msg):   print(f"{C.YL}⚠️  {msg}{C.RST}")
def bad(msg):    print(f"{C.RD}❌ {msg}{C.RST}")
def step(msg):   print(f"{C.BL}{C.B}▶ {msg}{C.RST}")

# ======================================================
# 🧊 Snowflake session (hardcoded for POC)
# ======================================================
cfg = {
    "account":   "XKB93357.us-west-2",
    "user":      "MANISHAT007",
    "password":  "Welcome@987654321",   # ⚠ Hardcoded for POC
    "role":      "ORGADMIN",
    "warehouse": "COMPUTE_WH",
    "database":  "MY_DB",
    "schema":    "PUBLIC",
}
session = Session.builder.configs(cfg).create()

# ======================================================
# 🧠 Review Prompt
# ======================================================
PROMPT_TEMPLATE = """Please act as a principal-level Python code reviewer...
# CODE DIFF TO REVIEW
{PY_CONTENT}
"""

def build_prompt(code_text: str) -> str:
    code_text = code_text[:MAX_CODE_CHARS]
    return PROMPT_TEMPLATE.replace("{PY_CONTENT}", code_text).replace("{FILE_PATH}", FILE_TO_REVIEW)

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
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return {"summary": "Analysis completed (text format)", "raw_text": result}
    except Exception as e:
        bad(f"Cortex API error: {e}")
        return {"summary": f"Error occurred: {e}"}

# ======================================================
# 🔎 Helpers
# ======================================================
SEV_ICONS = {"CRITICAL": "🟥","HIGH": "🔴","MEDIUM":"🟠","LOW":"🟡","UNKNOWN":"⚪"}
def sev_icon(sev: str) -> str: return SEV_ICONS.get((sev or "UNKNOWN").upper(), "⚪")
def filter_low_severity(js): return {**js, "detailed_findings":[f for f in js.get("detailed_findings",[]) if f.get("severity","").upper()!="LOW"]}

# ======================================================
# 🧾 PR Formatter
# ======================================================
def format_for_pr_display(js: dict) -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    summary = js.get("summary","Code review completed")
    findings = js.get("detailed_findings",[])
    out = [f"🚦 **Automated LLM Code Review** — `{FILE_TO_REVIEW}`  \n_{ts}_\n", f"**TL;DR:** {summary}\n"]
    if findings:
        out.append("| Sev | Line | Context | Finding |")
        out.append("|:---:|:----:|:-------:|:--------|")
        for f in findings:
            sev = (f.get("severity") or "UNKNOWN").upper()
            out.append(f"| {sev_icon(sev)} {sev} | {f.get('line_number','N/A')} | `{f.get('function_context','—')}` | {f.get('finding','—')} |")
    else:
        out.append("> **No significant issues found.**")
    return "\n".join(out)

# ======================================================
# 🚀 Main
# ======================================================
if __name__ == "__main__":
    try:
        if not os.path.exists(FILE_TO_REVIEW):
            bad(f"File not found: {FILE_TO_REVIEW}")
            sys.exit(1)

        code_text = Path(FILE_TO_REVIEW).read_text(encoding="utf-8")
        report = review_with_cortex(MODEL, code_text)
        filtered = filter_low_severity(report)

        # Extract criticals properly
        criticals = []
        if "detailed_findings" in filtered:
            for f in filtered["detailed_findings"]:
                if f.get("severity", "").upper() == "CRITICAL":
                    criticals.append({
                        "line": f.get("line_number", "N/A"),
                        "issue": f.get("finding", "Critical issue"),
                        "severity": "CRITICAL"
                    })

        formatted = format_for_pr_display(filtered)

        output = {
            "full_review": formatted,
            "full_review_json": filtered,
            "criticals": criticals,   # ✅ always include this
            "file": FILE_TO_REVIEW,
            "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
            "model": MODEL,
        }

        Path("review_output.json").write_text(json.dumps(output, indent=2), encoding="utf-8")
        Path("review_output.md").write_text(formatted, encoding="utf-8")

        print(formatted[:2000])
        good("Review completed and saved.")
    finally:
        try:
            session.close()
        except Exception:
            pass
