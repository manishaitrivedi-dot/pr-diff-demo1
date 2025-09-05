import os
import requests

def post_inline_comments(repo, pr_number, comments_list):
    """
    Post inline comments to a GitHub Pull Request.

    Args:
        repo (str): "owner/repo-name"
        pr_number (int): Pull Request number
        comments_list (list): List of dicts with keys: file, position, message
                              (position = line number inside the PR diff, not file line)
    """

    # Read token from environment variable
    GITHUB_TOKEN = os.environ["GH_TOKEN"]

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Step 1: Get the latest commit SHA of the PR
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # Step 2: Prepare API endpoint for posting comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"

    success_count = 0
    for comment in comments_list:
        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": comment["file"],
            "position": comment["position"],  # Use diff position for new/changed files
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
    # Example usage (dummy comments)
    my_comments = [
        {"file": "simple_test.py", "position": 3, "message": "💡 Consider adding a docstring"},
        {"file": "simple_test.py", "position": 6, "message": "⚡ greet() could be improved"}
    ]

    posted_count = post_inline_comments(
        repo="manishaitrivedi-dot/pr-diff-demo1",
        pr_number=3,
        comments_list=my_comments
    )

    print(f"Successfully posted {posted_count} comments")
