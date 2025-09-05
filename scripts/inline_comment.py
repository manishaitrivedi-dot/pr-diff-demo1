import os
import requests
def post_inline_comments(repo, pr_number, comments_list):
    # Read token from environment variable
    GITHUB_TOKEN = os.environ["GH_TOKEN"]
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    # get the latest commit SHA of the PR
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()  # ensures error is raised if request fails
    latest_commit_sha = commits_resp.json()[-1]["sha"]
    # prepare API endpoint for posting comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success_count = 0
    for comment in comments_list:
        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": comment["file"],
            "line": comment["line"],
            "side": "RIGHT"  # inline comment on the changed line
        }
        resp = requests.post(url, headers=headers, json=comment_data)
        if resp.status_code == 201:
            print(f"Posted: {comment['message']}")
            success_count += 1
        else:
            print(f"Failed: {comment['message']} — {resp.text}")
    return success_count
if __name__ == "__main__":
    # example usage
    my_comments = [
        {"file": "extract_pr_diffs.py", "line": 6, "message": "inline comment add 1"},
        {"file": "extract_pr_diffs.py", "line": 7, "message": "inline comment add 2"},
        {"file": "extract_pr_diffs.py", "line": 8, "message": "inline comment add 3"}
    ]
    posted_count = post_inline_comments(
        repo="manishaitrivedi-dot/pr-diff-demo1",
        pr_number=3,
        comments_list=my_comments
    )
    print(f"Successfully posted {posted_count} comments")
