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
latest_commit_sha = commits_resp.json()[-1]["sha"]

# Custom comments with your specific text
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"

comments = [
    {
        "body": "inline comment add 1",
        "commit_id": latest_commit_sha,
        "path": "extract_pr_diffs.py",
        "line": 1,
        "side": "RIGHT"
    },
    {
        "body": "inline comment add 2", 
        "commit_id": latest_commit_sha,
        "path": "extract_pr_diffs.py",
        "line": 2,
        "side": "RIGHT"
    },
    {
        "body": "inline comment add 3",
        "commit_id": latest_commit_sha,
        "path": "extract_pr_diffs.py", 
        "line": 3,
        "side": "RIGHT"
    }
]

success_count = 0
for i, comment_data in enumerate(comments, 1):
    print(f"Posting: {comment_data['body']}")
    
    resp = requests.post(url, headers=headers, json=comment_data)
    
    if resp.status_code == 201:
        print(f"  Success!")
        success_count += 1
    else:
        print(f"  Failed: {resp.status_code}")

print(f"Posted {success_count}/{len(comments)} comments successfully!")
