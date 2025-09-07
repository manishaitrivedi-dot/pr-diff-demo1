# inline_comment_from_chunks.py
# Posts inline comments on a PR for TARGET_FILE at specific *file* line numbers.
# It uses line_to_position.json (written by prepare_llm_chunks.py in PR context)
# to translate file-line -> diff "position" required by GitHub inline API.
#
# Env:
#   GH_TOKEN / GITHUB_TOKEN
#   OWNER, REPO, PR_NUMBER
#   TARGET_FILE
#   COMMENTS  (JSON array) e.g.:
#     [
#       {"line": 5,  "message": "💡 Consider improving the docstring."},
#       {"line": 13, "message": "🔍 Maybe handle edge cases here?"}
#     ]

import os, json, requests, sys

def gh_headers():
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GH_TOKEN / GITHUB_TOKEN is required.")
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

def get_latest_commit(owner, repo, pr_number, headers):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    commits = r.json()
    if not commits:
        raise RuntimeError("No commits found for PR.")
    return commits[-1]["sha"]

def main():
    owner = os.environ.get("OWNER")
    repo  = os.environ.get("REPO")
    pr_env = os.environ.get("PR_NUMBER")
    pr_number = int(pr_env) if pr_env and pr_env.isdigit() else None
    target_file = os.environ.get("TARGET_FILE")
    comments_raw = os.environ.get("COMMENTS", "[]")

    if not (owner and repo and pr_number and target_file):
        raise RuntimeError("OWNER, REPO, PR_NUMBER and TARGET_FILE env vars are required.")
    try:
        comments = json.loads(comments_raw)
    except Exception as e:
        raise RuntimeError(f"Invalid COMMENTS JSON: {e}")

    headers = gh_headers()
    commit_sha = get_latest_commit(owner, repo, pr_number, headers)

    # Load position map (if present)
    pos_map = {}
    if os.path.exists("line_to_position.json"):
        with open("line_to_position.json", "r", encoding="utf-8") as f:
            all_maps = json.load(f)
            pos_map = all_maps.get(target_file, {})
    else:
        print("ℹ️ No line_to_position.json found. Lines not in the diff cannot be commented inline.")

    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"

    posted = 0
    for c in comments:
        line = int(c["line"])
        message = c["message"]

        # Translate file line -> diff position
        position = None
        if pos_map:
            position = pos_map.get(str(line))

        if position is None:
            print(f"↪️ Skipping line {line}: not part of the PR diff (no diff position).")
            continue

        payload = {
            "body": message,
            "commit_id": commit_sha,
            "path": target_file,
            "position": position  # using 'position' is still accepted & simple
        }

        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 201:
            print(f"✅ Commented on {target_file}:{line} (position {position}) — {message}")
            posted += 1
        else:
            print(f"❌ Failed to comment {target_file}:{line} — {resp.status_code} {resp.text}")

    print(f"\nPosted {posted} inline comment(s).")
    if posted == 0:
        print("Note: GitHub only allows inline comments on lines that appear in the PR diff.")

if __name__ == "__main__":
    main()
