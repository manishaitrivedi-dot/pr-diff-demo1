import json, os, subprocess
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

REPO = Path(__file__).resolve().parents[1]

def sh(cmd: List[str]) -> str:
    p = subprocess.run(cmd, cwd=str(REPO), text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout

def get_event() -> Dict[str, Any]:
    path = os.getenv("GITHUB_EVENT_PATH")
    if path and Path(path).exists():
        return json.loads(Path(path).read_text(encoding="utf-8"))
    return {}

def resolve_base_head() -> Tuple[str, str]:
    ev = get_event()
    pr = ev.get("pull_request") or {}
    base_sha = pr.get("base", {}).get("sha")
    head_sha = pr.get("head", {}).get("sha")
    if not base_sha:
        base_ref = os.getenv("GITHUB_BASE_REF")
        if base_ref:
            sh(["git", "fetch", "origin", base_ref, "--depth=1"])
            base_sha = sh(["git", "rev-parse", f"origin/{base_ref}"]).strip()
        else:
            base_sha = sh(["git", "rev-parse", "HEAD~1"]).strip()
    if not head_sha:
        head_sha = sh(["git", "rev-parse", "HEAD"]).strip()
    return base_sha, head_sha

def changed_python_files(base: str, head: str):
    out = sh(["git", "diff", "--name-status", base, head, "--", "*.py"]).strip()
    files = []
    for line in out.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        status = parts[0][0]
        path = parts[-1]
        files.append((status, path))
    return files

def parse_hunks(base: str, head: str, path: str):
    diff = sh(["git", "diff", "--unified=0", base, head, "--", path])
    ranges = []
    for line in diff.splitlines():
        s = line.strip()
        if s.startswith("@@"):
            seg = s.split(" ")[2]  # +c,d
            if seg.startswith("+"):
                seg = seg[1:]
            if "," in seg:
                a, b = seg.split(",", 1)
                start, cnt = int(a), int(b)
            else:
                start, cnt = int(seg), 1
            if cnt > 0:
                ranges.append((start, cnt))
    return ranges

def file_lines(path: str):
    p = REPO / path
    if not p.exists():
        return []
    return p.read_text(encoding="utf-8", errors="replace").splitlines()

def slice_ctx(lines, start, count, ctx=3):
    n = len(lines)
    s0 = max(1, start - ctx)
    e0 = min(n, start + count - 1 + ctx)
    return s0, lines[s0-1:e0]

def to_md(base: str, head: str, items):
    parts = [f"### Python Diff Summary\nBase `{base[:7]}` → Head `{head[:7]}`\n"]
    if not items:
        parts.append("\n_No Python changes detected._\n")
        return "".join(parts)
    for f in items:
        parts.append("\n---\n")
        parts.append(f"**File:** `{f['path']}` (status: {f['status']})\n")
        if f["status"] == "A":
            parts.append("Scope: Entire file (new)\n\n```python\n")
            for ln, text in f["full_file"]:
                parts.append(f"{ln:>4}: {text}\n")
            parts.append("```\n")
        else:
            for h in f["hunks"]:
                parts.append(f"\nScope: Lines {h['start']}..{h['end']} (±{h['ctx']} ctx)\n\n```python\n")
                for ln, text in h["lines"]:
                    parts.append(f"{ln:>4}: {text}\n")
                parts.append("```\n")
    return "".join(parts)

def set_output(name: str, value: str):
    out = os.getenv("GITHUB_OUTPUT")
    if not out:
        return
    with open(out, "a", encoding="utf-8") as f:
        f.write(f"{name}<<__EOF__\n{value}\n__EOF__\n")

def main():
    base, head = resolve_base_head()
    items = []
    for status, path in changed_python_files(base, head):
        lines = file_lines(path)
        if not lines:
            continue
        if status == "A":
            items.append({
                "status": status, "path": path,
                "full_file": [(i+1, ln) for i, ln in enumerate(lines)],
                "hunks": []
            })
        else:
            hunks = []
            for start, cnt in parse_hunks(base, head, path):
                sline, chunk = slice_ctx(lines, start, cnt, ctx=3)
                hunks.append({
                    "start": start, "end": start+cnt-1, "count": cnt, "ctx": 3,
                    "lines": [(sline+i, l) for i, l in enumerate(chunk)]
                })
            items.append({"status": status, "path": path, "hunks": hunks})

    md = to_md(base, head, items)
    print(md)
    set_output("diff_markdown", md)

if __name__ == "__main__":
    main()
