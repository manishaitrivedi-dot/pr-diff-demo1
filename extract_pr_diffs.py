import os
import subprocess

def extract_pr_diffs(base_branch="origin/main"):
    """
    Extract Python (.py) code changes.
    - If this is the first commit in the branch, show full diff vs base_branch.
    - Otherwise, show only the last commit diff.
    """

    # Check how many commits are ahead of base_branch
    ahead_commits = subprocess.run(
        ["git", "rev-list", "--count", f"{base_branch}..HEAD"],
        capture_output=True,
        text=True,
        check=True
    ).stdout.strip()

    # If only 1 commit ahead → show full diff vs base_branch
    if ahead_commits == "1":
        diff_cmd = ["git", "diff", f"{base_branch}...HEAD", "--", "*.py"]
    else:
        # Otherwise → just show the last commit
        diff_cmd = ["git", "diff", "HEAD~1", "HEAD", "--", "*.py"]

    result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
    diff_output = result.stdout.strip()

    if not diff_output:
        return "No Python changes detected."

    # Split diffs per file
    file_diffs = {}
    current_file = None
    buffer = []

    for line in diff_output.splitlines():
        if line.startswith("diff --git"):
            if current_file and buffer:
                file_diffs[current_file] = "\n".join(buffer)
                buffer = []
            parts = line.split(" b/")
            if len(parts) == 2:
                current_file = parts[1]
        elif current_file:
            buffer.append(line)

    if current_file and buffer:
        file_diffs[current_file] = "\n".join(buffer)

    # Format results for markdown
    markdown_output = "### Python Code Changes\n"
    for fname, diff in file_diffs.items():
        markdown_output += f"\n**File:** `{fname}`\n```diff\n{diff}\n```\n"

    return markdown_output


if __name__ == "__main__":
    diff_markdown = extract_pr_diffs()

    # Print for logs
    print(diff_markdown)

    # Export for GitHub Actions if running inside CI
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{diff_markdown}\nEOF\n")
