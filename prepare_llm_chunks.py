# prepare_llm_chunks.py
# - Builds LLM-friendly chunks for a single file
# - If PR context exists, limits to changed lines (+context) and writes
#   line_to_position.json mapping { "<path>": { "<line>": <position>, ... } }

import os, re, json, requests
from typing import Dict, List, Tuple, Optional

def env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None: return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

def gh_headers():
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token: return None
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
        if not batch: break
        out.extend(batch); page += 1
    return out

def build_position_map(patch: str) -> Tuple[Dict[int, int], List[int]]:
    """Map new-file line -> diff position; collect added lines."""
    if not patch: return {}, []
    new_to_pos, added = {}, []
    pos = 0
    current_new = None
    for raw in patch.splitlines():
        line = raw.rstrip("\n")
        if line.startswith("@@"):
            m = HUNK_RE.match(line)
            pos += 1
            if m: current_new = int(m.group("new_start"))
            continue
        pos += 1
        if line.startswith("+") and not line.startswith("+++"):
            if current_new is not None:
                new_to_pos[current_new] = pos
                added.append(current_new)
                current_new += 1
        elif line.startswith(" "):
            if current_new is not None: current_new += 1
        elif line.startswith("-"):
            pass
    return new_to_pos, added

def chunk_added_lines(file_content: str, added: List[int], max_chars: int, ctx: int):
    if not added: return
    lines = file_content.splitlines()
    spans = []
    for ln in added:
        a = max(1, ln - ctx); b = min(len(lines), ln + ctx)
        spans.append((a, b))
    spans.sort()
    merged: List[List[int]] = []
    for a, b in spans:
        if not merged or a > merged[-1][1] + 1:
            merged.append([a, b])
        else:
            merged[-1][1] = max(merged[-1][1], b)
    cur_a = cur_b = None
    cur_text = ""
    for a, b in merged:
        frag = "\n".join(f"{i}:{lines[i-1]}" for i in range(a, b+1))
        if cur_a is None:
            cur_a, cur_b, cur_text = a, b, frag
            continue
        candidate = cur_text + "\n" + frag
        if len(candidate) <= max_chars:
            cur_b = b; cur_text = candidate
        else:
            yield (cur_a, cur_b, cur_text)
            cur_a, cur_b, cur_text = a, b, frag
    if cur_a is not None:
        yield (cur_a, cur_b, cur_text)

def chunk_full_file(file_content: str, max_chars: int):
    lines = file_content.splitlines()
    cur_start, cur_text, cur_len = 1, "", 0
    for i, line in enumerate(lines, start=1):
        stamped = f"{i}:{line}"
        add_len = len(stamped) + 1
        if cur_len + add_len > max_chars and cur_text:
            yield (cur_start, i-1, cur_text)
            cur_start, cur_text, cur_len = i, stamped, add_len
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
    repo  = os.environ.get("REPO", "").strip()
    pr_env = os.environ.get("PR_NUMBER")
    pr_number = int(pr_env) if pr_env and str(pr_env).isdigit() else None

    max_chars = int(os.environ.get("MAX_CHARS", "250"))
    ctx_lines = int(os.environ.get("CTX_LINES", "3"))
    only_diff = env_bool("ONLY_DIFF", default=True) if pr_number else False

    print(f"📄 TARGET_FILE = {target_file}")
    print(f"🔧 MAX_CHARS = {max_chars}, CTX_LINES = {ctx_lines}")
    if pr_number:
        print(f"🔗 PR_NUMBER = {pr_number} (only_diff={only_diff})")
        print(f"📦 Repo = {owner}/{repo}")

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
            patch = pr_file["patch"]
            new_to_pos, added = build_position_map(patch)
            line_to_position[target_file] = {str(k): v for k, v in new_to_pos.items()}

            if content is None and commit_sha:
                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{target_file}"
                rr = requests.get(raw_url)
                if rr.status_code == 200: content = rr.text

            if not content:
                raise RuntimeError(f"Could not load content for {target_file}.")

            if only_diff:
                chunks = list(chunk_added_lines(content, added, max_chars=max_chars, ctx=ctx_lines))
            else:
                chunks = list(chunk_full_file(content, max_chars=max_chars))

            out_chunks = []
            for idx, (a, b, text) in enumerate(chunks, start=1):
                out_chunks.append({"path": target_file, "chunk_id": idx, "start_line": a, "end_line": b, "text": text})

            with open("llm_chunks.json", "w", encoding="utf-8") as f:
                json.dump(out_chunks, f, ensure_ascii=False, indent=2)
            with open("line_to_position.json", "w", encoding="utf-8") as f:
                json.dump(line_to_position, f, ensure_ascii=False, indent=2)
            with open("llm_chunks.md", "w", encoding="utf-8") as f:
                f.write(f"# LLM Chunks for {target_file}\n\n")
                for c in out_chunks:
                    f.write(f"## Chunk {c['chunk_id']} ({c['start_line']}–{c['end_line']})\n\n")
                    f.write("```text\n" + c["text"] + "\n```\n\n")

            print(f"✅ Wrote llm_chunks.json ({len(out_chunks)} chunk(s))")
            print("✅ Wrote line_to_position.json (for inline posting)")
            print("✅ Wrote llm_chunks.md (preview)")
            return
        else:
            print(f"ℹ️ {target_file} not in PR or no patch -> chunk entire file.")
            only_diff = False

    if content is None:
        raise RuntimeError(f"Could not load content for {target_file}.")

    chunks = list(chunk_full_file(content, max_chars=max_chars))
    out_chunks = []
    for idx, (a, b, text) in enumerate(chunks, start=1):
        out_chunks.append({"path": target_file, "chunk_id": idx, "start_line": a, "end_line": b, "text": text})

    with open("llm_chunks.json", "w", encoding="utf-8") as f:
        json.dump(out_chunks, f, ensure_ascii=False, indent=2)
    with open("llm_chunks.md", "w", encoding="utf-8") as f:
        f.write(f"# LLM Chunks for {target_file}\n\n")
        for c in out_chunks:
            f.write(f"## Chunk {c['chunk_id']} ({c['start_line']}–{c['end_line']})\n\n")
            f.write("```text\n" + c["text"] + "\n```\n\n")

    print(f"✅ Wrote llm_chunks.json ({len(out_chunks)} chunk(s))")
    print("ℹ️ Full-file mode (no line_to_position.json).")

if __name__ == "__main__":
    main()
