import os
import subprocess


def extract_pr_diffs(base_branch="origin/main"):
    """
    Extract Python (.py) code changes:
      - If only one commit since base_branch → show full file diff.
      - If more than one commit → show only the last commit diff.
    """

    # Count commits ahead of base
    count_cmd = ["git", "rev-list", "--count", f"{base_branch}..HEAD"]
    commit_count = int(
        subprocess.run(count_cmd, capture_output=True, text=True, check=True).stdout.strip()
    )

    if commit_count <= 1:
        # First commit: compare against base (full diff)
        diff_cmd = ["git", "diff", f"{base_branch}...HEAD", "--", "*.py", ":(exclude)./extract_pr_diffs.py"]
    else:
        # Subsequent commits: just compare last commit
        diff_cmd = ["git", "diff", "HEAD~1", "HEAD", "--", "*.py", ":(exclude)./extract_pr_diffs.py"]

    result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
    diff_output = result.stdout.strip()

    if not diff_output:
        return "No Python changes detected."

    # Split by file for clarity
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

    # Format results as Markdown
    markdown_output = "### Python Code Changes\n"
    for fname, diff in file_diffs.items():
        markdown_output += f"\n**File:** `{fname}`\n```diff\n{diff}\n```\n"

    return markdown_output


if __name__ == "__main__":
    diff_markdown = extract_pr_diffs()

    # Print for logs
    print(diff_markdown)

    # Export for GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{diff_markdown}\nEOF\n")
