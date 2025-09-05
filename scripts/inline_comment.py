import os
import requests

# Repo / PR setup
REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3
FILE_PATH = "simple_test.py"
COMMENT_BODY = "💡 Inline comment added by bot"

# GitHub token (from Actions secret)
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

# 2) Get PR files (to find valid positions)
files_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/files"
files_resp = requests.get(files_url, headers=headers)
files_resp.raise_for_status()
files = files_resp.json()

# Find the target file
target_file = next((f for f in files if f["filename"] == FILE_PATH), None)
if not target_file:
    raise SystemExit(f"File {FILE_PATH} not found in PR diff")

# Pick a valid position (GitHub gives it)
valid_position = target_file.get("patch").splitlines().index(
    next(line for line in target_file["patch"].splitlines() if line.startswith("+"))
) + 1

print(f"Using valid position {valid_position} for file {FILE_PATH}")

# 3) Post the inline comment
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
data = {
    "body": COMMENT_BODY,
    "commit_id": latest_commit_sha,
    "path": FILE_PATH,
    "position": valid_position
}

resp = requests.post(url, headers=headers, json=data)
print("Status:", resp.status_code)
print("Response:", resp.json())
