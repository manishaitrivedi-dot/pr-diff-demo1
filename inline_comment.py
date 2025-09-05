import os
import requests

# Config
GITHUB_TOKEN = os.environ["GH_TOKEN"]   # Secret set in GitHub Actions
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3   # Your open PR

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# 1. Get latest commit SHA for this PR
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
commit_sha = commits_resp.json()[-1]["sha"]

# 2. Post inline comment
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Inline comment from Python script!",
    "commit_id": commit_sha,
    "path": "simple_test.py",  # must match exactly
    "line": 4,                 # line inside the green diff
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print("Response:", resp.json())
