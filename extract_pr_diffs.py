import os
import subprocess


def extract_pr_diffs(base_branch="origin/main"):
    """
    Extract Python (.py) code changes:
      - If only one commit since base_branch → show full file diff.
      - If more than one commit → show only the last commit diff.
      - Always exclude this script itself from the diff.
    """

    script_file = os.path.basename(__file__)  # get the filename of this script

    # Count commits ahead of base
    count_cmd = ["git", "rev-list", "--count", f"{base_branch}..HEAD"]
    commit_count = int(
        subprocess.run(count_cmd, capture_output=True, text=True, check=True).stdout.strip()
    )

    if commit_count <= 1:
        # First commit → full diff since base
        diff_cmd = ["git", "diff", f"{base_branch}...HEAD", "--", "*.py", f":(exclude)extract_pr_diffs.py"]
    else:
        # Subsequent commits → last commit only
        diff_cmd = ["git", "diff", "HEAD~1", "HEAD", "--", "*.py", f":(exclude)extract_pr_diffs.py"]

    result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
    diff_output = result.stdout.strip()

    if not diff_output:
        return "No Python changes detected."

    # Parse file diffs
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

    # Format for Markdown
    markdown_output = "### Python Code Changes\n"
    for fname, diff in file_diffs.items():
        markdown_output += f"\n**File:** `{fname}`\n```diff\n{diff}\n```\n"

    return markdown_output


if __name__ == "__main__":
    diff_markdown = extract_pr_diffs()
    print(diff_markdown)

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{diff_markdown}\nEOF\n")
