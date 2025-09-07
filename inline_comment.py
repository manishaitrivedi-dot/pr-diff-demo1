import os, json, re, requests

HUNK_RE = re.compile(r"@@ -(?P<old_start>\d+),?(?P<old_count>\d+)? \+(?P<new_start>\d+),?(?P<new_count>\d+)? @@")

def gh_headers():
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise SystemExit("❌ GH_TOKEN/GITHUB_TOKEN not set")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json"
    }

def detect_pr_from_event():
    """Read PR number from the GitHub event payload (so you don’t hardcode)."""
    evt = os.environ.get("GITHUB_EVENT_PATH")
    if not evt or not os.path.exists(evt):
        return None
    with open(evt, "r", encoding="utf-8") as f:
        data = json.load(f)
    # pull_request event:
    pr = data.get("pull_request")
    if pr and "number" in pr:
        return pr["number"]
    # workflow_run and others won’t have it — return None
    return None

def get_last_commit_sha(owner, repo, pr_number, headers):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/commits"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    commits = r.json()
    return commits[-1]["sha"] if commits else None

def get_pr_files(owner, repo, pr_number, headers):
    files = []
    page = 1
    while True:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/files?per_page=100&page={page}"
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        files.extend(batch)
        page += 1
    return files

def build_position_map(patch: str):
    """Map new-file line -> diff position and return (map, added_lines_set)."""
    if not patch:
        return {}, set()
    new_to_pos = {}
    added = set()
    pos = 0
    current_new = None

    for raw in patch.splitlines():
        line = raw.rstrip("\n")
        if line.startswith("@@"):
            m = HUNK_RE.match(line)
            pos += 1
            if not m:
                continue
            current_new = int(m.group("new_start"))
            continue
        pos += 1
        if line.startswith("+") and not line.startswith("+++"):
            if current_new is not None:
                new_to_pos[current_new] = pos
                added.add(current_new)
                current_new += 1
        elif line.startswith(" "):
            if current_new is not None:
                current_new += 1
        elif line.startswith("-"):
            # deletion: don't advance new counter
            pass
    return new_to_pos, added

def post_file_level_comment(owner, repo, pr_number, headers, path, body):
    """
    File-level review comment (no line). This shows up on the file in the PR,
    even when the specific line isn’t in the diff.
    """
    # GitHub supports subject_type='file' on this endpoint
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"
    payload = {"body": body, "path": path, "subject_type": "file"}
    r = requests.post(url, headers=headers, json=payload)
    return r

def post_inline_comment(owner, repo, pr_number, headers, commit_sha, path, position, body):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"
    payload = {
        "body": body,
        "commit_id": commit_sha,
        "path": path,
        "position": position
    }
    r = requests.post(url, headers=headers, json=payload)
    return r

def main():
    headers = gh_headers()

    repo_full = os.environ.get("GITHUB_REPOSITORY")
    if not repo_full:
        raise SystemExit("❌ GITHUB_REPOSITORY not set")
    owner, repo = repo_full.split("/", 1)

    pr_number = detect_pr_from_event()
    if not pr_number:
        raise SystemExit("❌ Could not detect PR number from event payload")

    # Which file & which lines to comment?
    # We read line_targets.json created by prepare_llm_chunks.py
    target_file = os.environ.get("TARGET_FILE", "simple_test.py")
    if not os.path.exists("line_targets.json"):
        raise SystemExit("❌ line_targets.json not found. Run prepare_llm_chunks.py first.")
    with open("line_targets.json", "r", encoding="utf-8") as f:
        targets = json.load(f)
    target_lines = targets.get(target_file, [])
    if not target_lines:
        print(f"ℹ️ No target lines listed for {target_file}. Nothing to do.")
        return

    # Get diff + commit
    commit_sha = get_last_commit_sha(owner, repo, pr_number, headers)
    pr_files = get_pr_files(owner, repo, pr_number, headers)
    item = next((x for x in pr_files if x["filename"] == target_file), None)

    position_map = {}
    if item and item.get("patch"):
        position_map, added_set = build_position_map(item["patch"])

    # Post comments:
    posted = 0
    for ln in target_lines:
        body = f"🔎 Auto-review: check line {ln}."
        if ln in position_map:
            pos = position_map[ln]
            r = post_inline_comment(owner, repo, pr_number, headers, commit_sha, target_file, pos, body)
            if r.status_code == 201:
                print(f"✅ Inline on {target_file}:{ln} (position {pos})")
                posted += 1
            else:
                print(f"❌ Inline failed ({target_file}:{ln}): {r.status_code} {r.text}")
        else:
            # Fallback to file-level comment so you still see something on the file
            r = post_file_level_comment(owner, repo, pr_number, headers, target_file,
                                        f"{body}\n\n_(File-level comment because this line isn’t in the diff.)_")
            if r.status_code in (200, 201):
                print(f"📝 File-level comment on {target_file} for line {ln}")
                posted += 1
            else:
                print(f"❌ File-level failed ({target_file}:{ln}): {r.status_code} {r.text}")

    print(f"\nPosted {posted} comment(s).")

if __name__ == "__main__":
    main()
