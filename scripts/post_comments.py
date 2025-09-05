import os
import requests

def post_inline_comment(path: str, line: int, body: str):
    """
    Post an inline comment to a GitHub PR.
    path  -> file path in PR (e.g. 'demo.py')
    line  -> line number in new code
    body  -> comment text
    """
    # 1. GitHub gives you a temporary token when workflow runs
    token = os.environ["GITHUB_TOKEN"]

    # 2. Repo name like "username/reponame"
    repo = os.environ["GITHUB_REPOSITORY"]

    # 3. PR number (which PR is open)
    pr_number = os.environ["PR_NUMBER"]

    # 4. Get the last commit SHA from this PR
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(commits_url, headers=headers)
    r.raise_for_status()
    latest_commit_sha = r.json()[-1]["sha"]

    # 5. Post the comment
    comments_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": body,                 # text of comment
        "commit_id": latest_commit_sha,  # link to last commit
        "path": path,                 # file name in PR
        "side": "RIGHT",              # always "RIGHT" for new code
        "line": line                  # which line number in the file
    }

    resp = requests.post(comments_url, headers=headers, json=payload)
    if resp.status_code == 201:
        print(f"Comment posted: {path}:{line}")
    else:
        print(f"Failed: {resp.status_code} {resp.text}")


if __name__ == "__main__":
    # Example: add a comment to line 2 of demo.py
    post_inline_comment("demo.py", 2, "Consider handling errors here.")
