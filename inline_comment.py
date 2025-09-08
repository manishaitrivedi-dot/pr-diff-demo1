import os
import requests
import re
from typing import Dict, List, Optional, Tuple

GH_TOKEN_ENV = "GH_TOKEN"   # or "GITHUB_TOKEN" if you prefer

# ------------ GitHub helpers ------------

def gh_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

def get_latest_open_pr(repo: str, headers: Dict[str, str]) -> Optional[int]:
    """Return the most recently updated OPEN PR number, or None if none."""
    url = f"https://api.github.com/repos/{repo}/pulls?state=open&sort=updated&direction=desc&per_page=1"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()
    return data[0]["number"] if data else None

def get_pr_latest_commit_sha(repo: str, pr_number: int, headers: Dict[str, str]) -> str:
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()[-1]["sha"]

def get_pr_files(repo: str, pr_number: int, headers: Dict[str, str]) -> List[Dict]:
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files?per_page=100"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

# ------------ Diff parsing --------------

def build_newline_to_position_map(patch: str) -> Dict[int, int]:
    """
    Map 'new file' line numbers -> GitHub diff 'position' for a single file patch.
    Position is the 1-based index across *all lines in the unified diff body*
    (excluding @@ hunk headers), counting context/additions/deletions.
    We also track the 'new file' line number and only map positions for added lines.
    """
    mapping: Dict[int, int] = {}

    if not patch:
        return mapping

    # Example hunk header: @@ -10,7 +10,9 @@
    hunk_re = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@")

    position = 0                 # counts lines in diff body (no @@ lines)
    new_line = None              # current new-file line number in this hunk

    for raw in patch.splitlines():
        if raw.startswith("@@"):
            m = hunk_re.match(raw)
            if not m:
                # Ignore malformed header safely
                new_line = None
                continue
            # Start line for the "new" file in this hunk
            new_line = int(m.group(2))
            continue

        # Only body lines (add/del/context) increment position
        position += 1

        if new_line is None:
            # Shouldn't happen, but be robust
            continue

        if raw.startswith("+") and not raw.startswith("+++"):
            # Added line -> maps to a diff position; record it
            mapping[new_line] = position
            new_line += 1
        elif raw.startswith("-") and not raw.startswith("---"):
            # Removed line -> affects old file only
            # new_line unchanged
            pass
        else:
            # Context line ' ' (or anything else) -> advance new_line
            new_line += 1

    return mapping

# ------------ Comment poster ------------

def post_inline_comment_by_position(
    repo: str,
    pr_number: Optional[int],
    file_path: str,
    comments: List[Tuple[int, str]],  # [(new_file_line, message), ...]
) -> int:
    """
    Post inline comments on a PR for a given file. 'comments' takes NEW FILE line numbers.
    Only lines that are part of the diff (added/changed) will be commented using 'position'.
    """
    token = os.environ.get(GH_TOKEN_ENV)
    if not token:
        raise RuntimeError(f"Environment variable {GH_TOKEN_ENV} is required.")

    headers = gh_headers(token)

    # Resolve PR number dynamically if needed
    if pr_number is None:
        pr_number = get_latest_open_pr(repo, headers)
        if pr_number is None:
            print("No open PRs found.")
            return 0
        print(f" Using latest open PR number: {pr_number}")

    # Get latest commit on the PR (required by API)
    commit_sha = get_pr_latest_commit_sha(repo, pr_number, headers)

    # Find the target file in the PR and get its patch
    files = get_pr_files(repo, pr_number, headers)
    target = next((f for f in files if f.get("filename") == file_path), None)
    if not target:
        print(f" File '{file_path}' not found in PR #{pr_number}")
        return 0

    patch = target.get("patch")
    if not patch:
        print(f" No patch available for '{file_path}' in PR #{pr_number} (GitHub can omit very large patches).")
        return 0

    # Build mapping of NEW file line -> diff position
    line_to_pos = build_newline_to_position_map(patch)

    # Post comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    posted = 0

    for new_line, message in comments:
        pos = line_to_pos.get(new_line)
        if not pos:
            print(f" Skipping line {new_line}: not part of the diff (or mapping unavailable).")
            continue

        payload = {
            "body": message,
            "commit_id": commit_sha,
            "path": file_path,
            "position": pos,     # <-- reliable for added/changed lines
        }

        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 201:
            print(f" Commented on {file_path}:{new_line} (position {pos}) — {message}")
            posted += 1
        else:
            print(f" Failed on {file_path}:{new_line} (position {pos}) — {resp.status_code} {resp.text}")

    return posted

# ------------- Example usage -------------

if __name__ == "__main__":
    REPO = "manishaitrivedi-dot/pr-diff-demo1"

    # OPTION A: target a specific PR (e.g., #5)
    PR_NUMBER = 5

    # OPTION B: make it dynamic (use latest open PR)
    # PR_NUMBER = None

    # File you want to comment on (Python file)
    FILE_PATH = "scripts/simple_test.py"   # adjust if your file is in repo root: "simple_test.py"

    # List of (new_file_line_number, "message") you want to place.
    # Pick line numbers that were *added/changed in this PR*.
    MY_COMMENTS = [
        (13, " Consider removing leftover debug comment."),
        (11, " Nice use of f-strings."),
    ]

    count = post_inline_comment_by_position(REPO, PR_NUMBER, FILE_PATH, MY_COMMENTS)
    print(f"\nPosted {count} inline comment(s).")
