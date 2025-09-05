import os
import requests
import re

def get_pr_files(repo, pr_number, headers):
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def post_inline_comment(repo, pr_number, file_name, line, message):
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

    # 2. Check PR files and patches
    files = get_pr_files(repo, pr_number, headers)
    print("\n📋 Files in PR and their valid added lines:")
    for f in files:
        print(f"▶ {f['filename']}")
        if "patch" in f:
            added_lines = []
            for match in re.finditer(r"\+(\d+)", f["patch"]):
                added_lines.append(int(match.group(1)))
            print("   Valid lines:", added_lines[:10], "..." if len(added_lines) > 10 else "")
    
    # 3. Post comment
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": message,
        "commit_id": latest_commit_sha,
        "path": file_name,   # must match exactly from API
        "line": line,        # must be one of the valid lines above
        "side": "RIGHT"
    }

    resp = requests.post(url, headers=headers, json=payload)
    print("Status:", resp.status_code)
    print("Response:", resp.json())


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    pr_number = 3

    # Replace file/line after checking the valid ones printed
    post_inline_comment(
        repo,
        pr_number,
        file_name="simple_test.py",   # must match from PR files list
        line=5,                       # must be in "Valid lines"
        message="💡 Add a docstring here"
    )
