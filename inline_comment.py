import os
import requests

GITHUB_TOKEN = os.environ["GH_TOKEN"]  # from GitHub Secrets
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3   # your open PR

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Step 1: Get latest commit SHA
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
commit_id = commits_resp.json()[-1]["sha"]

# Step 2: Post inline comment
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Inline comment added via bot",
    "commit_id": commit_id,
    "path": "simple_test.py",
    "line": 4,
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print("Response:", resp.json())
