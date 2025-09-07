# inline_comment.py
import os
import sys
import requests
from typing import Dict, List, Tuple, Optional

# ========= CONFIGURE THESE TWO =========
REPO = "manishaitrivedi-dot/pr-diff-demo1"   # owner/repo
TARGET_FILE = "simple_test.py"                # file to comment on
TARGET_LINES = [11, 13]                       # file line numbers to target
# ======================================

API = "https://api.github.com"
TOKEN = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")

if not TOKEN:
    print("❌ Missing GH_TOKEN/GITHUB_TOKEN environment variable.")
    sys.exit(1)

HEADERS = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github+json",
}

def get_latest_open_pr(repo: str) -> Optional[int]:
    """Return the number of the most recently created OPEN PR, or None."""
    url = f"{API}/repos/{repo}/pulls"
    params = {"state": "open", "sort": "created", "direction": "desc", "per_page": 1}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    items = r.json()
    if not items:
        return None
    return items[0]["number"]

def get_latest_commit_sha(repo: str, pr_number: int) -> str:
    url = f"{API}/repos/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    commits = r.json()
    return commits[-1]["sha"]

def get_pr_files(repo: str, pr_number: int) -> List[dict]:
    url = f"{API}/repos/{repo}/pulls/{pr_number}/files"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

def build_line_to_position_map(patch: str) -> Tuple[Dict[int, int], List[int]]:
    """
    Create mapping: new-file line number -> GitHub 'position' for POST /pulls/:number/comments
    Also return all positions in case you want to see what's valid.
    Rules:
      - Position 1 = first line AFTER the first '@@' hunk header in the file's patch.
      - Count continues across multiple hunks until file ends.
      - For '+': advance position and new_line++, and record mapping.
      - For ' ' (context): advance position and new_line++.
      - For '-': advance position only (deletion line).
      - Ignore '+++', '---' headers and the '@@' lines themselves for counting.
    """
    line_to_pos: Dict[int, int] = {}
    all_positions: List[int] = []

    position = 0
    new_line = None

    for raw in patch.splitlines():
        line = raw.rstrip("\n")

        # Hunk header: @@ -old_start,old_count +new_start,new_count @@
        if line.startswith("@@"):
            # Parse new_start
            # Example: @@ -1,2 +1,10 @@
            try:
                hunk = line.split("@@")[1].strip()  # '-1,2 +1,10'
                parts = hunk.split()
                plus = [p for p in parts if p.startswith("+")][0]
                plus = plus.lstrip("+")
                # "+a,b" or "+a"
                if "," in plus:
                    new_start = int(plus.split(",")[0])
                else:
                    new_start = int(plus)
                new_line = new_start
            except Exception:
                # If parsing fails, we can't safely map—bail out of hunk.
                new_line = None
            continue

        # Ignore file headers inside patch
        if line.startswith("+++ ") or line.startswith("--- "):
            continue

        # Only count positions after first hunk header
        if new_line is None:
            continue

        # Now count this line as a position
        position += 1
        all_positions.append(position)

        if line.startswith("+") and not line.startswith("+++"):
            # Addition line contributes to new file content
            # Record mapping for this new_file line -> current position
            line_to_pos[new_line] = position
            new_line += 1
        elif line.startswith("-") and not line.startswith("---"):
            # Deletion: position advances, new_line does NOT
            pass
        else:
            # Context (unchanged) line begins with space
            # Some patches may show no leading char; be defensive:
            if line[:1] == " " or (line and not line.startswith(("+", "-"))):
                new_line += 1

    return line_to_pos, all_positions

def post_inline_comment(repo: str, pr_number: int, commit_sha: str,
                        path: str, position: int, body: str) -> requests.Response:
    url = f"{API}/repos/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": body,
        "commit_id": commit_sha,
        "path": path,
        "position": position
    }
    r = requests.post(url, headers=HEADERS, json=payload)
    return r

def main():
    # 1) Choose PR: latest open PR
    pr_number = get_latest_open_pr(REPO)
    if not pr_number:
        print("❌ No open pull requests found. Create a PR first.")
        sys.exit(0)

    print(f"ℹ️ Using latest open PR: #{pr_number}")

    # 2) Get latest commit SHA
    commit_sha = get_latest_commit_sha(REPO, pr_number)
    print(f"ℹ️ Latest commit SHA: {commit_sha}")

    # 3) Find our target file in the PR & get patch
    files = get_pr_files(REPO, pr_number)
    target_entry = next((f for f in files if f.get("filename") == TARGET_FILE), None)
    if not target_entry:
        print(f"❌ `{TARGET_FILE}` is NOT part of PR #{pr_number}. Nothing to comment.")
        sys.exit(0)

    patch = target_entry.get("patch")
    if not patch:
        print(f"❌ GitHub did not provide a patch for `{TARGET_FILE}` (possibly very large file or renamed-only).")
        sys.exit(0)

    # 4) Build line→position map
    line_to_pos, all_positions = build_line_to_position_map(patch)

    # 5) Post comments for our fixed target lines (11 and 13)
    total = 0
    for ln in TARGET_LINES:
        if ln not in line_to_pos:
            print(f"⚠️ Line {ln} is not part of the PR diff for `{TARGET_FILE}`. Skipping.")
            continue
        pos = line_to_pos[ln]
        body = f"🤖 Auto inline note on `{TARGET_FILE}` line {ln}."
        resp = post_inline_comment(REPO, pr_number, commit_sha, TARGET_FILE, pos, body)
        if resp.status_code == 201:
            print(f"✅ Comment posted on {TARGET_FILE}:{ln} (position {pos})")
            total += 1
        else:
            print(f"❌ Failed for {TARGET_FILE}:{ln} (position {pos}) — {resp.status_code} {resp.text}")

    print(f"\nDone. Posted {total} comment(s).")
    if total == 0:
        print("👉 Tip: Make sure lines 11 and/or 13 are actually **added/changed** in this PR. "
              "GitHub will only accept comments on lines that appear in the PR diff.")

if __name__ == "__main__":
    main()
