# inline_comment.py
import os, json, requests

GH_TOKEN = os.environ["GH_TOKEN"]
REPO = os.environ["GITHUB_REPOSITORY"]   # e.g. "owner/repo"
PR_NUMBER = os.environ["PR_NUMBER"]      # injected by GitHub Actions

headers = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def post_pr_comment(body: str):
    url = f"https://api.github.com/repos/{REPO}/issues/{PR_NUMBER}/comments"
    requests.post(url, headers=headers, json={"body": body})

def post_inline_comments(comments):
    # Get latest commit SHA for this PR
    url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
    commits = requests.get(url, headers=headers).json()
    latest_sha = commits[-1]["sha"]

    url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
    for c in comments:
        data = {
            "body": c["body"],
            "commit_id": latest_sha,
            "path": c["path"],
            "side": "RIGHT",
            "line": c["line"]
        }
        requests.post(url, headers=headers, json=data)

if __name__ == "__main__":
    with open("review_output.json") as f:
        review_data = json.load(f)

    # Post overall PR review
    post_pr_comment("##  Automated LLM Code Review\n\n" + review_data["full_review"])

    # Prepare inline comments for critical findings
    inline_comments = []
    for c in review_data["criticals"]:
        inline_comments.append({
            "path": review_data["file"],
            "line": c["line"],
            "body": f" **Critical Issue**: {c['issue']}\n\n**Recommendation:** {c['recommendation']}"
        })

    if inline_comments:
        post_inline_comments(inline_comments)
