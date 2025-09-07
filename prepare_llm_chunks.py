# prepare_llm_chunks.py
# Purpose:
#  - Create LLM-friendly chunks from a Python file (with real line numbers)
#  - If PR context is available, limit to changed lines (+context) and export
#    new-file-line -> GitHub "position" map for inline comments later.
#
# Inputs (env):
#   GH_TOKEN or GITHUB_TOKEN (optional unless using PR mode)
#   OWNER  (e.g., "manishaitrivedi-dot")
#   REPO   (e.g., "pr-diff-demo1")
#   PR_NUMBER (optional; if set, we'll use PR diff and last commit)
#   TARGET_FILE (required, e.g., "scripts/simple_test.py")
#   ONLY_DIFF (optional, "true"/"false"; default true when PR_NUMBER is set)
#   MAX_CHARS (optional, default "2500")  - max chars per chunk
#   CTX_LINES (optional, default "3")     - context lines around each change
#
# Outputs (files in repo workspace):
#   llm_chunks.json         -> list of chunks (path, chunk_id, start_line, end_line, text)
#   line_to_position.json   -> map { "<path>": { "<line>": <position>, ... } } (PR mode only)
#   llm_chunks.md           -> human-readable preview

import os
import re
import json
import requests
from typing import Dict, List, Tuple, Optional

# ---------------------- helpers: env + github headers ------------------------

def env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def gh_headers():
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        return None
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

# ---------------------- PR utilities ----------------------

HUNK_RE = re.compile(r"@@ -(?P<old_start>\d+),?(?P<old_count>\d+)? \+(?P<new_start>\d+),?(?P<new_count>\d+)? @@")

def get_last_commit_sha(owner: str, repo: str, pr_number: int, headers) -> Optional[str]:
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f"⚠️  Unable to fetch PR commits: {r.status_code} {r.text}")
        return None
    commits = r.json()
    return commits[-1]["sha"] if commits else None

def get_pr_files(owner: str, repo: str, pr_number: int, headers) -> List[dict]:
    out = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files?per_page=100&page={page}"
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            print(f"⚠️  Unable to fetch PR files: {r.status_code} {r.text}")
            break
        batch = r.json()
        if not batch:
            break
        out.extend(batch)
        page += 1
    return out

def build_position_map(patch: str) -> Tuple[Dict[int, int], List[int]]:
    """
    Map new-file line -> diff position (GitHub semantics), and collect changed (added) lines.
    """
    if not patch:
        return {}, []

    new_to_pos: Dict[int, int] = {}
    added_lines: List[int] = []
    pos = 0
    current_new_line: Optional[int] = None

    for raw in patch.splitlines():
        line = raw.rstrip("\n")
        if line.startswith("@@"):
            m = HUNK_RE.match(line)
            pos += 1  # hunk header counts
            if not m:
                continue
            current_new_line = int(m.group("new_start"))
            continue

        pos += 1
        if line.startswith("+") and not line.startswith("+++"):
            if current_new_line is not None:
                new_to_pos[current_new_line] = pos
                added_lines.append(current_new_line)
                current_new_line += 1
        elif line.startswith(" "):
            if current_new_line is not None:
                current_new_line += 1
        elif line.startswith("-"):
            # deletion: do not advance new-line counter
            pass
        else:
            # other lines; ignore
            pass

    return new_to_pos, added_lines

# ---------------------- chunking ----------------------

def chunk_added_lines(file_content: str, added: List[int], max_chars: int, ctx: int):
    """Create context windows around changed lines and pack under max_chars."""
    if not added:
        return
    lines = file_content.splitlines()
    spans: List[Tuple[int, int]] = []
    for ln in added:
        a = max(1, ln - ctx)
        b = min(len(lines), ln + ctx)
        spans.append((a, b))

    # merge overlaps
    spans.sort()
    merged: List[List[int]] = []
    for a, b in spans:
        if not merged or a > merged[-1][1] + 1:
            merged.append([a, b])
        else:
            merged[-1][1] = max(merged[-1][1], b)

    # pack
    cur_a = cur_b = None
    cur_text = ""
    for a, b in merged:
        frag = "\n".join(f"{i}:{lines[i-1]}" for i in range(a, b+1))
        if cur_a is None:
            cur_a, cur_b, cur_text = a, b, frag
            continue
        candidate = cur_text + "\n" + frag
        if len(candidate) <= max_chars:
            cur_b = b
            cur_text = candidate
        else:
            yield (cur_a, cur_b, cur_text)
            cur_a, cur_b, cur_text = a, b, frag
    if cur_a is not None:
        yield (cur_a, cur_b, cur_text)

def chunk_full_file(file_content: str, max_chars: int):
    """Split entire file into max_chars windows; lines prefixed with their real numbers."""
    lines = file_content.splitlines()
    cur_start = 1
    cur_text = ""
    cur_len = 0
    for i, line in enumerate(lines, start=1):
        stamped = f"{i}:{line}"
        add_len = len(stamped) + 1  # + newline
        if cur_len + add_len > max_chars and cur_text:
            yield (cur_start, i-1, cur_text)
            cur_start = i
            cur_text = stamped
            cur_len = add_len
        else:
            cur_text = (cur_text + "\n" if cur_text else "") + stamped
            cur_len += add_len
    if cur_text:
        yield (cur_start, len(lines), cur_text)

# ---------------------- main ----------------------

def main():
    target_file = os.environ.get("TARGET_FILE")
    if not target_file:
        raise RuntimeError("Set TARGET_FILE env (e.g., scripts/simple_test.py).")

    owner = os.environ.get("OWNER", "").strip()
    repo  = os.environ.get("REPO", "").strip()
    pr_env = os.environ.get("PR_NUMBER")
    pr_number = int(pr_env) if pr_env and pr_env.isdigit() else None

    max_chars = int(os.environ.get("MAX_CHARS", "2500"))
    ctx_lines = int(os.environ.get("CTX_LINES", "3"))
    only_diff = env_bool("ONLY_DIFF", default=True) if pr_number else False

    print(f"📄 TARGET_FILE = {target_file}")
    print(f"🔧 MAX_CHARS = {max_chars}, CTX_LINES = {ctx_lines}")
    if pr_number:
        print(f"🔗 PR_NUMBER = {pr_number} (only_diff={only_diff})")
        print(f"📦 Repo = {owner}/{repo}")

    # Load content (from workspace)
    content = None
    if os.path.exists(target_file):
        with open(target_file, "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()

    # If PR mode and no local content, fetch via raw:
    commit_sha = None
    line_to_position: Dict[str, Dict[str, int]] = {}

    if pr_number:
        headers = gh_headers()
        if not headers:
            raise RuntimeError("GH_TOKEN or GITHUB_TOKEN required when PR_NUMBER is set.")

        commit_sha = get_last_commit_sha(owner, repo, pr_number, headers)
        if not commit_sha:
            print("⚠️  Could not resolve PR commit SHA; will still try local file.")

        files = get_pr_files(owner, repo, pr_number, headers)
        pr_file = next((f for f in files if f["filename"] == target_file), None)
        if not pr_file:
            print(f"⚠️  {target_file} is not part of PR #{pr_number}.")
            # fallback to full file chunking
            only_diff = False
        else:
            patch = pr_file.get("patch")
            if not patch:
                print(f"⚠️  No patch available for {target_file} (renamed or too large?) -> full file chunking.")
                only_diff = False
            else:
                new_to_pos, added = build_position_map(patch)
                line_to_position[target_file] = {str(k): v for k, v in new_to_pos.items()}

                # If no local content, fetch raw at commit
                if content is None and commit_sha:
                    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{target_file}"
                    rr = requests.get(raw_url)
                    if rr.status_code == 200:
                        content = rr.text

                if not content:
                    raise RuntimeError(f"Could not load content for {target_file}.")

                if only_diff:
                    # produce chunks only around changed lines
                    chunks = list(chunk_added_lines(content, added, max_chars=max_chars, ctx=ctx_lines))
                else:
                    # entire file
                    chunks = list(chunk_full_file(content, max_chars=max_chars))

                # write outputs
                out_chunks = []
                for idx, (a, b, text) in enumerate(chunks, start=1):
                    out_chunks.append({
                        "path": target_file,
                        "chunk_id": idx,
                        "start_line": a,
                        "end_line": b,
                        "text": text
                    })

                with open("llm_chunks.json", "w", encoding="utf-8") as f:
                    json.dump(out_chunks, f, ensure_ascii=False, indent=2)
                with open("line_to_position.json", "w", encoding="utf-8") as f:
                    json.dump(line_to_position, f, ensure_ascii=False, indent=2)

                # pretty preview
                with open("llm_chunks.md", "w", encoding="utf-8") as f:
                    f.write(f"# LLM Chunks for {target_file}\n\n")
                    for c in out_chunks:
                        f.write(f"## Chunk {c['chunk_id']} ({c['start_line']}–{c['end_line']})\n\n")
                        f.write("```text\n" + c["text"] + "\n```\n\n")

                print(f"✅ Wrote llm_chunks.json ({len(out_chunks)} chunk(s))")
                print("✅ Wrote line_to_position.json (for inline posting)")
                print("✅ Wrote llm_chunks.md (preview)")
                return

    # ---- fallback: full-file chunking when no PR or ONLY_DIFF disabled ----
    if content is None:
        raise RuntimeError(f"Could not load content for {target_file}.")

    chunks = list(chunk_full_file(content, max_chars=max_chars))
    out_chunks = []
    for idx, (a, b, text) in enumerate(chunks, start=1):
        out_chunks.append({
            "path": target_file,
            "chunk_id": idx,
            "start_line": a,
            "end_line": b,
            "text": text
        })

    with open("llm_chunks.json", "w", encoding="utf-8") as f:
        json.dump(out_chunks, f, ensure_ascii=False, indent=2)

    with open("llm_chunks.md", "w", encoding="utf-8") as f:
        f.write(f"# LLM Chunks for {target_file}\n\n")
        for c in out_chunks:
            f.write(f"## Chunk {c['chunk_id']} ({c['start_line']}–{c['end_line']})\n\n")
            f.write("```text\n" + c["text"] + "\n```\n\n")

    print(f"✅ Wrote llm_chunks.json ({len(out_chunks)} chunk(s))")
    print("✅ Wrote llm_chunks.md (preview)")
    if pr_number:
        print("ℹ️ No PR patch available; produced full-file chunks (no position map).")

if __name__ == "__main__":
    main()
