import os
import requests

REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3
GITHUB_TOKEN = os.environ["GH_TOKEN"]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Check what files are actually changed in this PR
files_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/files"
files_resp = requests.get(files_url, headers=headers)
print("PR #3 changed files:")
for file in files_resp.json():
    print(f"  - {file['filename']} (status: {file['status']})")

# Now use one of the files that actually changed
changed_files = files_resp.json()
if changed_files:
    target_file = changed_files[0]['filename']  # Use the first changed file
    print(f"Will comment on: {target_file}")
    
    # Get commit SHA
    commits_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    latest_commit_sha = commits_resp.json()[-1]["sha"]
    
    # Post comment
    url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
    data = {
        "body": "⚡ Inline comment added by bot",
        "commit_id": latest_commit_sha,
        "path": target_file,
        "line": 1,
        "side": "RIGHT"
    }
    
    resp = requests.post(url, headers=headers, json=data)
    print("Status:", resp.status_code)
    print("Response:", resp.text)
