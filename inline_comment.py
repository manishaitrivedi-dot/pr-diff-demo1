import os
import requests


def get_latest_pr_number(repo, headers):
    url = f"https://api.github.com/repos/{repo}/pulls?state=open&sort=created&direction=desc"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    prs = resp.json()
    return prs[0]["number"] if prs else None


def get_pr_files(repo, pr_number, headers):
    """Fetch files & changed hunks in PR"""
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def post_inline_comments(repo, pr_number, headers, comments_list):
    # Step 1: get latest commit SHA
    commits_url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/commits"
    commits_resp = requests.get(commits_url, headers=headers)
    commits_resp.raise_for_status()
    latest_commit_sha = commits_resp.json()[-1]["sha"]

    # Step 2: post inline comments
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    success = 0

    for comment in comments_list:
        comment_data = {
            "body": comment["message"],
            "commit_id": latest_commit_sha,
            "path": comment["file"],
            "line": comment["line"],  # must be valid inside diff
            "side": "RIGHT"
        }
        resp = requests.post(url, headers=headers, json=comment_data)
        if resp.status_code == 201:
            print(f"✅ Posted: {comment['message']}")
            success += 1
        else:
            print(f"❌ Failed: {comment['message']} — {resp.text}")
    return success


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    GITHUB_TOKEN = os.environ["GH_TOKEN"]

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # pick PR dynamically
    pr_number = get_latest_pr_number(repo, headers)
    print(f"ℹ️ Using PR #{pr_number}")

    # get files actually changed in this PR
    files = get_pr_files(repo, pr_number, headers)
    for f in files:
        print(f"Changed file: {f['filename']} | Additions: {f['additions']} | Deletions: {f['deletions']}")

    # Example: comment only on first changed file’s first added line
    if files:
        file_to_comment = files[0]["filename"]
        line_to_comment = 1  # ⚠️ must be inside an added hunk

        my_comments = [
            {"file": file_to_comment, "line": line_to_comment, "message": "💡 Auto review: check this line"}
        ]

        posted = post_inline_comments(repo, pr_number, headers, my_comments)
        print(f"✅ Successfully posted {posted} comments")
