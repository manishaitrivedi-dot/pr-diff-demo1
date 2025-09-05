import os
import requests

def post_inline_comment(repo, pr_number, file_name, position, message):
    GITHUB_TOKEN = os.environ["GH_TOKEN"]
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # 1. Get latest commit SHA
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # 2. Post inline comment using *position* (not line)
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": message,
        "commit_id": latest_commit_sha,
        "path": file_name,
        "position": position
    }

    resp = requests.post(url, headers=headers, json=payload)
    print("Status:", resp.status_code)
    print("Response:", resp.json())


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    pr_number = 3

    # Pick one of the valid positions you saw printed (e.g. 3 for simple_test.py)
    post_inline_comment(
        repo,
        pr_number,
        file_name="simple_test.py",
        position=3,
        message="💡 This is a test inline comment on simple_test.py"
    )
