import os
import requests

GITHUB_TOKEN = os.environ["GH_TOKEN"]
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3   # keep your PR number

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Step 1: get latest commit from PR
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
commit_id = commits_resp.json()[-1]["sha"]

# Step 2: post inline comment on simple_test.py line 4
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Bot: Please check this function name.",
    "commit_id": commit_id,
    "path": "simple_test.py",
    "line": 4,
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print("Response:", resp.json())
