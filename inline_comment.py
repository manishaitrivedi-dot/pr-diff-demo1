import os
import requests

def get_pr_files(repo, pr_number, headers):
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def post_inline_comments(repo, pr_number, comments_list):
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

    # Step 2: Post comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success_count = 0

    for comment in comments_list:
        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": comment["file"],
            "position": comment["position"],   # use patch position, not line
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
    pr_number = 3

    # Use positions from the patch above
    my_comments = [
        {"file": "simple_test.py", "message": "💡 Add docstring to function", "position": 1},
        {"file": "simple_test.py", "message": "⚡ Consider improving greet()", "position": 4},
        {"file": "simple_test.py", "message": "🔍 Avoid hardcoded string", "position": 11},
    ]

    posted_count = post_inline_comments(repo, pr_number, my_comments)
    print(f"\n✅ Successfully posted {posted_count} comments")
