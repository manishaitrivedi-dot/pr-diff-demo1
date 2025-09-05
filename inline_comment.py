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

url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"

# Try commenting on simple_test.py with different approaches
test_comments = [
    # Try without specifying side (GitHub will default it)
    {
        "body": "Test comment 1 - no side specified",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "line": 1
    },
    # Try with position instead of line (for new files)
    {
        "body": "Test comment 2 - using position",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "position": 1
    },
    # Try line 2
    {
        "body": "Test comment 3 - line 2",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "line": 2,
        "side": "RIGHT"
    }
]

for i, comment_data in enumerate(test_comments, 1):
    print(f"Trying approach {i}...")
    resp = requests.post(url, headers=headers, json=comment_data)
    
    if resp.status_code == 201:
        print(f"SUCCESS: {comment_data['body']}")
        break  # Stop after first success
    else:
        print(f"Failed: {resp.status_code}")
        print(f"Response: {resp.text}")

print("Test completed")
