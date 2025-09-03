import os
import subprocess
import argparse


def extract_pr_diffs(base_branch="origin/main", last_commit_only=False):
    """
    Extract Python (.py) code changes.

    Modes:
      - Default (PR mode): Compare base_branch...HEAD (cumulative PR diff).
      - Last commit mode: Compare HEAD~1 vs HEAD (only latest commit).
    """
    if last_commit_only:
        diff_cmd = ["git", "diff", "HEAD~1", "HEAD", "--", "*.py"]
    else:
        diff_cmd = ["git", "diff", f"{base_branch}...HEAD", "--", "*.py"]

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

    # Format for Markdown (PR comment style)
    markdown_output = "### Python Code Changes\n"
    for fname, diff in file_diffs.items():
        markdown_output += f"\n**File:** `{fname}`\n```diff\n{diff}\n```\n"

    return markdown_output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract Python diffs from PRs or commits.")
    parser.add_argument("--last-commit", action="store_true", help="Show only last commit diff instead of full PR diff.")
    args = parser.parse_args()

    diff_markdown = extract_pr_diffs(last_commit_only=args.last_commit)

    print(diff_markdown)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{diff_markdown}\nEOF\n")
