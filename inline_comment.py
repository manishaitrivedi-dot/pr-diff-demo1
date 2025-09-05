import os
import requests

# --- Config ---
GITHUB_TOKEN = os.environ.get("GH_TOKEN")  # Make sure GH_TOKEN is saved in repo secrets
REPO = "manishaitrivedi-dot/pr-diff-demo1"  # owner/repo
PR_NUMBER = 3                               # your open PR number

# --- Auth headers ---
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# --- Step 1: Get latest commit SHA from the PR ---
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
latest_commit_sha = commits_resp.json()[-1]["sha"]  # take the last commit SHA

# --- Step 2: Post inline comment ---
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": "⚡ Inline comment added via Python script!",
    "commit_id": latest_commit_sha,
    "path": "simple_test.py",   # file to comment on
    "line": 4,                  # line number in that file (use a changed line)
    "side": "RIGHT"
}

resp = requests.post(url, headers=headers, json=data)
print(resp.status_code, resp.json())
