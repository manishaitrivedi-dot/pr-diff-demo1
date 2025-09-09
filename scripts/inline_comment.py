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
    response = requests.post(url, headers=headers, json={"body": body})
    if response.status_code == 201:
        print("Posted PR comment successfully")
    else:
        print(f"Failed to post PR comment: {response.status_code}")

def post_inline_comments(comments):
    # Get latest commit SHA for this PR
    url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/commits"
    commits = requests.get(url, headers=headers).json()
    latest_sha = commits[-1]["sha"]
    
    url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/comments"
    posted_count = 0
    
    for c in comments:
        data = {
            "body": c["body"],
            "commit_id": latest_sha,
            "path": c["path"],
            "side": "RIGHT",
            "line": c["line"]
        }
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 201:
            print(f"Posted inline comment on line {c['line']}")
            posted_count += 1
        else:
            print(f"Failed to post inline comment on line {c['line']}: {response.status_code}")
    
    print(f"Posted {posted_count}/{len(comments)} inline comments")

if __name__ == "__main__":
    try:
        with open("review_output.json") as f:
            review_data = json.load(f)
        
        # Post overall PR review
        review_body = "## Automated LLM Code Review\n\n" + review_data["full_review"]
        post_pr_comment(review_body)
        
        # Prepare inline comments for critical findings
        inline_comments = []
        for c in review_data["criticals"]:
            inline_comments.append({
                "path": review_data["file"],
                "line": c["line"],
                "body": f"**Critical Issue**: {c['issue']}\n\n**Recommendation:** {c['recommendation']}"
            })
        
        if inline_comments:
            print(f"Posting {len(inline_comments)} critical inline comments...")
            post_inline_comments(inline_comments)
        else:
            print("No critical issues found for inline comments")
            
    except FileNotFoundError:
        print("review_output.json not found. Run cortex_python_review.py first.")
    except Exception as e:
        print(f"Error: {e}")
