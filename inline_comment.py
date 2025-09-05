import os
import requests

# --- Config ---
GITHUB_TOKEN = os.environ.get("GH_TOKEN")  # GitHub Actions injects this
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3   # your open PR

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# 1. Get latest commit SHA of the PR
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
latest_commit_sha = commits_resp.json()[-1]["sha"]

# 2. Post inline comment on line 4 of simple_test.py
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Automated inline comment from Python script!",
    "commit_id": latest_commit_sha,
    "path": "simple_test.py",
    "line": 4,  # change this to whichever line you want (1–9)
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print(resp.json())
