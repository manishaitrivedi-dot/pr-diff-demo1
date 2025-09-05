import os
import requests
import re

def get_pr_files(repo, pr_number, headers):
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def post_inline_comment(repo, pr_number, file_name, message):
    GITHUB_TOKEN = os.environ["GH_TOKEN"]
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    # 1. Get latest commit SHA
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # 2. Get PR files
    files = get_pr_files(repo, pr_number, headers)
    target_file = next((f for f in files if f["filename"] == file_name), None)

    if not target_file:
        print(f"⚠️ File {file_name} not found in PR")
        return

    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"

    # 3. Build payload
    payload = {
        "body": message,
        "commit_id": latest_commit_sha,
        "path": file_name,
    }

    if target_file["status"] == "added":  
        # New file → must use "position"
        payload["position"] = 1
    else:  
        # Modified file → must use "line"
        payload["line"] = 1
        payload["side"] = "RIGHT"

    # 4. Send request
    resp = requests.post(url, headers=headers, json=payload)
    print("Status:", resp.status_code)
    print("Response:", resp.json())


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    pr_number = 3

    # Try commenting on simple_test.py
    post_inline_comment(
        repo,
        pr_number,
        file_name="simple_test.py",
        message="💡 This is a test inline comment on a new file"
    )
