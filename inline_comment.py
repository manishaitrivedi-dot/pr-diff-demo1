import os
import requests

def get_pr_files(repo, pr_number, headers):
    """Fetch files and valid positions in the PR."""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def post_inline_comments(repo, pr_number, comments_list):
    """
    Post inline comments to a GitHub Pull Request.
    comments_list = list of dicts {file, message, position}
    """

    GITHUB_TOKEN = os.environ["GH_TOKEN"]
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Step 1: Get latest commit SHA
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # Step 2: Get PR files and their valid positions
    files = get_pr_files(repo, pr_number, headers)
    file_positions = {f["filename"]: f for f in files}

    # Step 3: Post comments only for valid files/positions
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success_count = 0

    for comment in comments_list:
        file_name = comment["file"]
        if file_name not in file_positions:
            print(f"⚠️ Skipping {file_name} — not in this PR diff")
            continue

        # Use a safe position inside the diff (example: 1st line of the patch)
        position = comment.get("position", 1)

        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": file_name,
            "position": position
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
    pr_number = 3

    # Dummy test comments
    my_comments = [
        {"file": "simple_test.py", "message": "💡 Consider adding docstring", "position": 1},
        {"file": "simple_test.py", "message": "⚡ greet() could be improved", "position": 3},
    ]

    posted_count = post_inline_comments(repo, pr_number, my_comments)
    print(f"✅ Successfully posted {posted_count} comments")
