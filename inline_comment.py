import os
import requests

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

    # 2. Post inline comment using line + side (NOT position)
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": message,
        "commit_id": latest_commit_sha,
        "path": file_name,
        "line": line,         # line in the PR diff
        "side": "RIGHT"       # RIGHT = additions, LEFT = deletions
    }

    resp = requests.post(url, headers=headers, json=payload)
    print("Status:", resp.status_code)
    print("Response:", resp.json())


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    pr_number = 3

    # 🚨 Choose an actual changed line in your PR diff (e.g., 4 from simple_test.py)
    post_inline_comment(
        repo,
        pr_number,
        file_name="simple_test.py",
        line=4,   # <-- must match a green-added line in "Files changed"
        message="💡 Add a docstring here"
    )
