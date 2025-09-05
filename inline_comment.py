import os
import requests

def post_inline_comments(repo, pr_number, comments_list):
    """
    Post inline comments to a GitHub Pull Request dynamically.
    comments_list must have: file, message, and position (diff position, not file line)
    """

    GITHUB_TOKEN = os.environ["GH_TOKEN"]

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Step 1: Get PR details (to fetch diff positions)
    files_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    files_resp = requests.get(files_url, headers=headers)
    files_resp.raise_for_status()
    files_data = files_resp.json()

    # Build a map of file -> valid positions
    file_positions = {}
    for f in files_data:
        if f["patch"]:   # patch contains diff hunks
            file_positions[f["filename"]] = f["patch"]

    # Step 2: Get latest commit SHA
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # Step 3: Post comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success_count = 0

    for comment in comments_list:
        # You must give GitHub a valid "position" from the diff
        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": comment["file"],
            "position": comment["position"],   # diff position, not raw file line
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
    # Example usage: put comments on simple_test.py diff positions
    my_comments = [
        {"file": "simple_test.py", "position": 3, "message": "💡 Consider adding docstring"},
        {"file": "simple_test.py", "position": 5, "message": "⚡ Inline review: greet() could be improved"}
    ]

    posted_count = post_inline_comments(
        repo="manishaitrivedi-dot/pr-diff-demo1",
        pr_number=3,
        comments_list=my_comments
    )

    print(f"Successfully posted {posted_count} comments")
