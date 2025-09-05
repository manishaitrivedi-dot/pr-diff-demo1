import os
import requests

# Repo info
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3   # <-- update if testing on a different PR

# GitHub token from GitHub Actions secret
GITHUB_TOKEN = os.environ["GH_TOKEN"]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# 1️⃣ Get the latest commit SHA for this PR
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
latest_commit_sha = commits_resp.json()[-1]["sha"]

# 2️⃣ Post an inline comment
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Inline comment added automatically by Python script",
    "commit_id": latest_commit_sha,
    "path": "simple_test.py",   # file in your PR
    "line": 4,                  # line number inside that file
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print("Response:", resp.json())
