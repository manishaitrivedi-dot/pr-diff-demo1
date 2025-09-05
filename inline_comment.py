import os
import requests

# --- Config ---
GITHUB_TOKEN = os.environ.get("GH_TOKEN")  # Comes from GitHub Actions secret
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Get latest commit SHA from the PR
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
latest_commit_sha = commits_resp.json()[-1]["sha"]

# Post inline comment
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Inline comment from Python script!",
    "commit_id": latest_commit_sha,
    "path": "simple_test.py",  # file in PR
    "line": 4,                 # line in PR diff
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print(resp.status_code, resp.json())
