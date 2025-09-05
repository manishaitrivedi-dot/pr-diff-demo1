import os
import requests


def get_latest_pr_number(repo, headers):
    """
    Fetch the latest open PR number from GitHub.
    """
    url = f"https://api.github.com/repos/{repo}/pulls?state=open&sort=created&direction=desc"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    prs = resp.json()
    if not prs:
        raise Exception("❌ No open PRs found in repo.")
    return prs[0]["number"]  # latest PR


def post_inline_comments(repo, pr_number, comments_list):
    """
    Post inline comments to a GitHub Pull Request.
    """
    GITHUB_TOKEN = os.environ["GH_TOKEN"]

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Step 1: Get latest commit SHA of the PR
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # Step 2: Post inline comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success_count = 0

    for comment in comments_list:
        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": comment["file"],   # must match file in PR diff
            "line": comment["line"],   # must exist in PR diff
            "side": "RIGHT"
        }

        resp = requests.post(url, headers=headers, json=comment_data)
        if resp.status_code == 201:
            print(f"✅ Posted: {comment['message']}")
            success_count += 1
        else:
            print(f"❌ Failed: {comment['message']} — {resp.text}")

    return success_count


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    GITHUB_TOKEN = os.environ["GH_TOKEN"]

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # 1. Get PR number dynamically
    pr_number = os.environ.get("PR_NUMBER")
    if pr_number:
        pr_number = int(pr_number)
        print(f"ℹ️ Using PR number from environment: {pr_number}")
    else:
        pr_number = get_latest_pr_number(repo, headers)
        print(f"ℹ️ Using latest open PR number: {pr_number}")

    # 2. Example dummy inline comments
    my_comments = [
        {"file": "simple_test.py", "line": 4, "message": "🔍 Inline review: check variable naming"},
        {"file": "simple_test.py", "line": 5, "message": "⚡ Inline review: consider refactoring this loop"},
    ]

    # 3. Post comments
    posted_count = post_inline_comments(repo, pr_number, my_comments)
    print(f"✅ Successfully posted {posted_count} comments")
