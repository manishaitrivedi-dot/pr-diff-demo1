# inline_comment.py
# Posts inline review comments on a PR using the line_to_position.json
# produced by prepare_llm_chunks.py. For demo, it comments on the first
# two added lines of TARGET_FILE if available.

import os, json, requests

OWNER       = os.environ.get("OWNER", "").strip()
REPO        = os.environ.get("REPO", "").strip()
PR_NUMBER   = os.environ.get("PR_NUMBER")
TARGET_FILE = os.environ.get("TARGET_FILE", "simple_test.py")

def gh_headers():
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

def last_commit_sha(owner, repo, pr_number, headers):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()[-1]["sha"]

def load_positions():
    try:
        with open("line_to_position.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def post_comment(owner, repo, pr_number, commit_sha, path, position, body, headers):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"
    payload = {"body": body, "commit_id": commit_sha, "path": path, "position": position}
    r = requests.post(url, headers=headers, json=payload)
    return r

def main():
    if not PR_NUMBER:
        print("ℹ️ No PR context (PR_NUMBER is empty). Skipping inline comments.")
        return
    pr_number = int(PR_NUMBER)

    headers = gh_headers()
    if not headers or not headers.get("Authorization"):
        print("❌ Missing GH_TOKEN/GITHUB_TOKEN.")
        return

    # Load the map new-file-line -> diff position
    pos_map = load_positions()
    file_map = pos_map.get(TARGET_FILE) or {}

    if not file_map:
        print(f"ℹ️ No position map for {TARGET_FILE}. Did prepare step run with ONLY_DIFF=true?")
        return

    # Sort by actual file line; pick first two positions for demo
    items = sorted(((int(line), int(pos)) for line, pos in file_map.items()))
    if not items:
        print(f"ℹ️ No added lines found in {TARGET_FILE} for this PR.")
        return

    commit_sha = last_commit_sha(OWNER, REPO, pr_number, headers)

    # Demo comments (customize or drive from your LLM output later)
    planned = [
        ("💡 Consider removing leftover debug comment.", 0),
        ("🧹 Small style nit: consistent blank lines help readability.", 1),
    ]

    posted = 0
    for msg, idx in planned:
        if idx >= len(items): break
        file_line, position = items[idx]
        resp = post_comment(OWNER, REPO, pr_number, commit_sha, TARGET_FILE, position, msg, headers)
        if resp.status_code == 201:
            print(f"✅ Commented on {TARGET_FILE}:{file_line} (position {position}) — {msg}")
            posted += 1
        else:
            print("⚠️ Failed to comment:", resp.status_code, resp.text)

    print(f"\nPosted {posted} inline comment(s).")

if __name__ == "__main__":
    main()
