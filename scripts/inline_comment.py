import os
import requests

REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3
GITHUB_TOKEN = os.environ["GH_TOKEN"]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Get the files that are actually changed in this PR
files_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/files"
files_resp = requests.get(files_url, headers=headers)
changed_files = files_resp.json()

print("Files changed in this PR:")
for file in changed_files:
    print(f"  - {file['filename']}")

# Get latest commit SHA
commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
commits_resp = requests.get(commits_url, headers=headers)
latest_commit_sha = commits_resp.json()[-1]["sha"]

# Find a Python file that was actually changed
target_file = None
for file in changed_files:
    if file['filename'].endswith('.py') and file['filename'] != 'inline_comment.py':
        target_file = file['filename']
        break

if not target_file:
    print("No suitable Python files found in PR changes")
    exit(1)

print(f"Posting comments on: {target_file}")

# Post inline comments on the file that actually changed
url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"

comments = [
    {
        "body": "Consider adding a docstring to document this function.",
        "commit_id": latest_commit_sha,
        "path": target_file,
        "line": 1,
        "side": "RIGHT"
    },
    {
        "body": "Good code structure! Consider adding type hints.",
        "commit_id": latest_commit_sha,
        "path": target_file,
        "line": 2,
        "side": "RIGHT"
    }
]

success_count = 0
for i, comment_data in enumerate(comments, 1):
    print(f"Posting comment {i}/{len(comments)} on {target_file}:line {comment_data['line']}")
    
    resp = requests.post(url, headers=headers, json=comment_data)
    
    if resp.status_code == 201:
        print(f"  Success!")
        success_count += 1
    else:
        print(f"  Failed: {resp.status_code}")
        print(f"  Response: {resp.text}")

print(f"\nPosted {success_count}/{len(comments)} comments successfully!")
