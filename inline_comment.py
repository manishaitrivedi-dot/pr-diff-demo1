import os
import requests

REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3
GITHUB_TOKEN = os.environ["GH_TOKEN"]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Get latest commit SHA
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
commits_resp.raise_for_status()
latest_commit_sha = commits_resp.json()[-1]["sha"]

print(f"Using commit SHA: {latest_commit_sha}")

# Post inline comments on simple_test.py
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"

comments = [
    {
        "body": "Consider adding a docstring to document what this function does.",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "line": 1,
        "side": "RIGHT"
    },
    {
        "body": "Good function implementation! Consider adding type hints for better code documentation.",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "line": 3,
        "side": "RIGHT"
    },
    {
        "body": "Consider using constants or configuration for hardcoded strings.",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "line": 4,
        "side": "RIGHT"
    }
]

success_count = 0
for i, comment_data in enumerate(comments, 1):
    print(f"Posting comment {i}/{len(comments)} on line {comment_data['line']}")
    
    resp = requests.post(url, headers=headers, json=comment_data)
    
    if resp.status_code == 201:
        print(f"  Success!")
        success_count += 1
    else:
        print(f"  Failed: {resp.status_code}")
        print(f"  Response: {resp.text}")

print(f"\nPosted {success_count}/{len(comments)} comments successfully!")
