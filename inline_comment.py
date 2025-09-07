# inline_comment.py
# Posts inline comments to the CURRENT PR using diff "position" mapping from line_to_position.json.
# - Targets a single file (default: simple_test.py) or read TARGET_FILE from env.
# - Finds PR number automatically from GitHub Actions env.
# - Uses nearest valid diff position when the exact line isn't in the diff.
#
# Env needed in Actions:
#   GH_TOKEN (already provided as ${{ secrets.GITHUB_TOKEN }} in your workflow)
#
# Optional env:
#   TARGET_FILE            (e.g., "simple_test.py" or "scripts/simple_test.py")
#   PR_NUMBER              (overrides auto-detect)
#   GITHUB_REPOSITORY      (actions default, "owner/repo")
#   GITHUB_REF, GITHUB_EVENT_PATH  (actions defaults for PR runs)

import os
import json
import re
import requests
from typing import Dict, List, Optional, Tuple

# ---------------- Config you can edit ----------------
# File you want to comment on (can be overridden by env TARGET_FILE)
TARGET_FILE = os.environ.get("TARGET_FILE", "simple_test.py")

# Lines you want to comment on (file line numbers, not diff positions!)
# Add/modify these as you like. If a line isn't in the diff, we'll pick the closest valid one.
REQUESTED_COMMENTS = [
    {"line": 13, "body": "💡 Consider removing leftover debug comment."},
    {"line": 11, "body": "🔍 Minor nit: consider adding a docstring here."},
]
# ----------------------------------------------------


def get_repo() -> Tuple[str, str]:
    repostr = os.environ.get("GITHUB_REPOSITORY", "")
    if "/" in repostr:
        owner, repo = repostr.split("/", 1)
        return owner, repo
    # Fallback (edit if you run locally)
    return "manishaitrivedi-dot", "pr-diff-demo1"


def get_pr_number() -> Optional[int]:
    # 1) Respect explicit env override
    pr_env = os.environ.get("PR_NUMBER")
    if pr_env and pr_env.isdigit():
        return int(pr_env)

    # 2) Parse from GITHUB_REF like "refs/pull/5/merge"
    ref = os.environ.get("GITHUB_REF", "")
    m = re.search(r"refs/pull/(\d+)/", ref)
    if m:
        return int(m.group(1))

    # 3) Read event payload (pull_request.number)
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if event_path and os.path.exists(event_path):
        try:
            with open(event_path, "r", encoding="utf-8") as f:
                evt = json.load(f)
            if "pull_request" in evt and "number" in evt["pull_request"]:
                return int(evt["pull_request"]["number"])
        except Exception:
            pass

    return None


def gh_headers() -> Dict[str, str]:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GH_TOKEN (or GITHUB_TOKEN) is required in env.")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def get_latest_commit_sha(owner: str, repo: str, pr_number: int, headers: Dict[str, str]) -> str:
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    commits = r.json()
    if not commits:
        raise RuntimeError("No commits found on this PR.")
    return commits[-1]["sha"]


def load_position_map(target_file: str) -> Dict[int, int]:
    """
    Read line_to_position.json produced by prepare_llm_chunks.py.
    Returns { new_file_line -> diff_position } for TARGET_FILE.
    """
    path = "line_to_position.json"
    if not os.path.exists(path):
        print("ℹ️ line_to_position.json not found. (Run prepare_llm_chunks.py earlier in the job.)")
        return {}

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Possible keys stored with exact path
    file_map = data.get(target_file)
    if not file_map:
        # Try also without leading "./"
        alt_key = target_file.lstrip("./")
        file_map = data.get(alt_key, {})

    # Convert keys to int
    out = {}
    for k, v in file_map.items():
        try:
            out[int(k)] = int(v)
        except Exception:
            continue
    return out


def nearest_valid_position(desired_line: int, pos_map: Dict[int, int]) -> Optional[Tuple[int, int]]:
    """
    Given a desired file line and a map {line -> position}, return (chosen_line, position)
    choosing the closest available line at or before desired_line; if none, try after.
    """
    if not pos_map:
        return None

    if desired_line in pos_map:
        return desired_line, pos_map[desired_line]

    # Try searching downward first
    lower = [ln for ln in pos_map.keys() if ln <= desired_line]
    if lower:
        ln = max(lower)
        return ln, pos_map[ln]

    # Otherwise search upward
    higher = [ln for ln in pos_map.keys() if ln >= desired_line]
    if higher:
        ln = min(higher)
        return ln, pos_map[ln]

    return None


def post_inline_comment(owner: str, repo: str, pr_number: int,
                        commit_sha: str, path: str, position: int,
                        body: str, headers: Dict[str, str]) -> requests.Response:
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": body,
        "commit_id": commit_sha,
        "path": path,
        "position": position
    }
    resp = requests.post(url, headers=headers, json=payload)
    return resp


def main():
    owner, repo = get_repo()
    pr_number = get_pr_number()
    if not pr_number:
        print("❌ Could not determine PR number. Is this running on a PR?")
        return

    headers = gh_headers()
    try:
        commit_sha = get_latest_commit_sha(owner, repo, pr_number, headers)
    except Exception as e:
        print(f"❌ Could not get latest commit SHA for PR #{pr_number}: {e}")
        return

    pos_map = load_position_map(TARGET_FILE)
    if not pos_map:
        print(f"❌ No diff position map for '{TARGET_FILE}'. "
              f"Ensure the file is part of the PR and prepare_llm_chunks.py ran first.")
        return

    # Pretty-print available lines for debugging
    print("\n📋 Available diff-mapped lines for", TARGET_FILE)
    preview = sorted(pos_map.keys())
    print("   ", preview[:50], "..." if len(preview) > 50 else "", "\n")

    posted = 0
    for req in REQUESTED_COMMENTS:
        line = int(req["line"])
        body = str(req["body"])

        choice = nearest_valid_position(line, pos_map)
        if not choice:
            print(f"↪️ Skipping line {line}: no valid diff-mapped positions.")
            continue

        chosen_line, position = choice
        if chosen_line != line:
            print(f"↪️ Requested line {line} not in diff; using nearest valid line {chosen_line} (position {position}).")
        else:
            print(f"✅ Commenting on {TARGET_FILE}:{line} (position {position})")

        resp = post_inline_comment(owner, repo, pr_number, commit_sha, TARGET_FILE, position, body, headers)
        if resp.status_code == 201:
            print(f"   ✓ Posted: {body}")
            posted += 1
        else:
            try:
                print("   ✗ Error:", resp.status_code, resp.json())
            except Exception:
                print("   ✗ Error:", resp.status_code, resp.text)

    print(f"\nPosted {posted} inline comment(s).")


if __name__ == "__main__":
    main()
