# prepare_llm_chunks.py
# Creates LLM-friendly chunks for a *full Python file* and (when in a PR)
# also builds a map from new-file line -> GitHub diff "position"
# so you can attach inline comments later.
#
# Env:
#   GH_TOKEN / GITHUB_TOKEN (required if PR_NUMBER is set)
#   OWNER, REPO, PR_NUMBER   (optional; if set, we try to map diff positions)
#   TARGET_FILE              (required; e.g. "simple_test.py")
#   ONLY_DIFF                ("true"/"false"; when PR set. We use "false" here.)
#   MAX_CHARS                (default "2500")
#   CTX_LINES                (default "3")
#
# Outputs:
#   llm_chunks.json
#   llm_chunks.md
#   line_to_position.json  (only if PR_NUMBER set and file is in the PR)

import os, re, json, requests
from typing import Dict, List, Tuple, Optional

def env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def gh_headers():
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        return None
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

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
    out, page = [], 1
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

def build_position_map(patch: str):
    """
    Returns:
      new_to_pos: {new_file_line -> diff position}
      added_lines: [list of new-file line numbers that were added/changed]
    """
    if not patch:
        return {}, []
    new_to_pos, added_lines = {}, []
    pos = 0
    current_new = None

    for raw in patch.splitlines():
        line = raw.rstrip("\n")
        if line.startswith("@@"):
            m = HUNK_RE.match(line)
            pos += 1  # hunk header counts
            if not m:
                continue
            current_new = int(m.group("new_start"))
            continue

        pos += 1
        if line.startswith("+") and not line.startswith("+++"):
            if current_new is not None:
                new_to_pos[current_new] = pos
                added_lines.append(current_new)
                current_new += 1
        elif line.startswith(" "):
            if current_new is not None:
                current_new += 1
        # "-" (deletions) do not advance new-line counter

    return new_to_pos, added_lines

def chunk_full_file(file_content: str, max_chars: int):
    """Split entire file by characters, prefix each line with its source line number."""
    lines = file_content.splitlines()
    cur_start = 1
    cur_text, cur_len = "", 0

    for i, line in enumerate(lines, start=1):
        stamped = f"{i}:{line}"
        add_len = len(stamped) + 1  # newline
        if cur_len + add_len > max_chars and cur_text:
            yield (cur_start, i - 1, cur_text)
            cur_start = i
            cur_text = stamped
            cur_len = add_len
        else:
            cur_text = (cur_text + "\n" if cur_text else "") + stamped
            cur_len += add_len
    if cur_text:
        yield (cur_start, len(lines), cur_text)

def main():
    target_file = os.environ.get("TARGET_FILE")
    if not target_file:
        raise RuntimeError("Set TARGET_FILE env (e.g., simple_test.py).")

    owner = os.environ.get("OWNER", "").strip()
    repo = os.environ.get("REPO", "").strip()
    pr_env = os.environ.get("PR_NUMBER")
    pr_number = int(pr_env) if pr_env and pr_env.isdigit() else None

    max_chars = int(os.environ.get("MAX_CHARS", "2500"))
    only_diff = env_bool("ONLY_DIFF", default=True) if pr_number else False

    print(f"📄 TARGET_FILE = {target_file}")
    print(f"🔧 MAX_CHARS   = {max_chars}")
    if pr_number:
        print(f"🔗 PR_NUMBER   = {pr_number} (only_diff={only_diff})")
        print(f"📦 Repo        = {owner}/{repo}")

    # Load content from workspace
    content = None
    if os.path.exists(target_file):
        with open(target_file, "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read()

    commit_sha = None
    line_to_position: Dict[str, Dict[str, int]] = {}

    if pr_number:
        headers = gh_headers()
        if not headers:
            raise RuntimeError("GH_TOKEN or GITHUB_TOKEN required when PR_NUMBER is set.")

        commit_sha = get_last_commit_sha(owner, repo, pr_number, headers)
        files = get_pr_files(owner, repo, pr_number, headers)
        pr_file = next((f for f in files if f["filename"] == target_file), None)

        if pr_file and pr_file.get("patch"):
            new_to_pos, added = build_position_map(pr_file["patch"])
            line_to_position[target_file] = {str(k): v for k, v in new_to_pos.items()}
            # If no local content, try raw at commit
            if content is None and commit_sha:
                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{target_file}"
                rr = requests.get(raw_url)
                if rr.status_code == 200:
                    content = rr.text
        else:
            print(f"ℹ️ {target_file} not in PR or no patch; we’ll still chunk full file.")

    if content is None:
        raise RuntimeError(f"Could not load content for {target_file}.")

    # Always chunk the entire file (your request)
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

    if pr_number and line_to_position:
        with open("line_to_position.json", "w", encoding="utf-8") as f:
            json.dump(line_to_position, f, ensure_ascii=False, indent=2)
        print("✅ Wrote line_to_position.json (PR diff positions)")

    print(f"✅ Wrote llm_chunks.json ({len(out_chunks)} chunk(s))")
    print("✅ Wrote llm_chunks.md (preview)")

if __name__ == "__main__":
    main()
