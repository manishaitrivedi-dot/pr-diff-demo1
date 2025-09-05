import os
import requests

# Inputs
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3
FILE_PATH = "simple_test.py"
COMMENT_BODY = "⚡ Inline comment added by bot"

# GitHub token from workflow
GITHUB_TOKEN = os.environ["GH_TOKEN"]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# 1) Get latest commit SHA
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
latest_commit_sha = commits_resp.json()[-1]["sha"]

# 2) Post inline review comment (use `position` not `line`)
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": COMMENT_BODY,
    "commit_id": latest_commit_sha,
    "path": FILE_PATH,
    "position": 4   # position in the diff
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print("Response:", resp.json())
