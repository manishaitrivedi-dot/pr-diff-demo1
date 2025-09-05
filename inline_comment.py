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

    # Step 2: Get PR files (with patches)
    files = get_pr_files(repo, pr_number, headers)

    print("\n📋 Files in PR and their valid diff patches:\n")
    for f in files:
        print(f"▶ {f['filename']}")
        print(f"Patch:\n{f['patch']}\n{'-'*50}")

    # Step 3: Post comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success_count = 0

    for comment in comments_list:
        file_name = comment["file"]

        # Find file in PR
        f = next((x for x in files if x["filename"] == file_name), None)
        if not f:
            print(f"⚠️ Skipping {file_name} — not in PR diff")
            continue

        # For new files, use "position", not "line"
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

    my_comments = [
        {"file": "simple_test.py", "message": "💡 Consider adding docstring", "position": 1},
        {"file": "simple_test.py", "message": "⚡ greet() could be improved", "position": 2},
    ]

    posted_count = post_inline_comments(repo, pr_number, my_comments)
    print(f"\n✅ Successfully posted {posted_count} comments")
