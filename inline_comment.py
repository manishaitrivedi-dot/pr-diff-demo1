import os
import requests

def get_pr_files(repo, pr_number, headers):
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

def post_inline_comments(repo, pr_number):
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

    # Step 2: Get files in PR
    files = get_pr_files(repo, pr_number, headers)

    print("\n📋 Files in PR and their valid positions:\n")
    for f in files:
        print(f"▶ {f['filename']}")
        if "patch" in f:
            # Count lines in patch → these are valid "positions"
            positions = [
                i+1 for i, line in enumerate(f["patch"].splitlines())
                if line.startswith("+") and not line.startswith("+++")
            ]
            print(f"Valid positions: {positions}")
        else:
            print("⚠️ No patch info available")

    # Step 3: Example comment on first valid line of simple_test.py
    simple_file = next((f for f in files if f["filename"] == "simple_test.py"), None)
    if not simple_file:
        print("❌ simple_test.py not found in PR diff")
        return

    patch_lines = simple_file["patch"].splitlines()
    # Find first added line
    try:
        position = next(i+1 for i, l in enumerate(patch_lines) if l.startswith("+") and not l.startswith("+++"))
    except StopIteration:
        print("❌ No added lines found in simple_test.py")
        return

    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/comments"
    comment_data = {
        "body": "💡 Automated comment: check this line",
        "commit_id": latest_commit_sha,
        "path": "simple_test.py",
        "position": position
    }

    resp = requests.post(url, headers=headers, json=comment_data)
    print("Status:", resp.status_code, resp.json())


if __name__ == "__main__":
    repo = "manishaitrivedi-dot/pr-diff-demo1"
    pr_number = 3

    post_inline_comments(repo, pr_number)
