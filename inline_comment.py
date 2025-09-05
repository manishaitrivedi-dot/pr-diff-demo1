import os
import requests

GITHUB_TOKEN = os.environ["GH_TOKEN"]   # safely read from GitHub Secrets
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3   # example PR

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "This is an inline comment from Python script ✅",
    "commit_id": "YOUR_COMMIT_SHA",
    "path": "simple_test.py",
    "line": 1,
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print(resp.status_code, resp.json())
