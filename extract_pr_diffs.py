import os
import subprocess


def extract_pr_diffs(base_branch="origin/main"):
    """
    Extract Python (.py) code changes:
      - First commit → full diff against base.
      - Later commits → only last commit.
      - Always exclude this script itself.
    """

    # Identify this script (both relative + basename)
    script_path = os.path.relpath(__file__).replace("\\", "/")
    script_name = os.path.basename(__file__)

    # Count commits since base
    count_cmd = ["git", "rev-list", "--count", f"{base_branch}..HEAD"]
    commit_count = int(
        subprocess.run(count_cmd, capture_output=True, text=True, check=True).stdout.strip()
    )

    # Build diff command
    if commit_count <= 1:
        diff_cmd = [
            "git", "diff", f"{base_branch}...HEAD", "--", "*.py",
            f":(exclude){script_path}", f":(exclude){script_name}"
        ]
    else:
        diff_cmd = [
            "git", "diff", "HEAD~1", "HEAD", "--", "*.py",
            f":(exclude){script_path}", f":(exclude){script_name}"
        ]

    # Run diff
    result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
    diff_output = result.stdout.strip()

    if not diff_output:
        return "No Python changes detected."

    # Split by file
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

    # Format Markdown
    markdown_output = "### Python Code Changes\n"
    for fname, diff in file_diffs.items():
        markdown_output += f"\n**File:** `{fname}`\n```diff\n{diff}\n```\n"

    return markdown_output


if __name__ == "__main__":
    diff_markdown = extract_pr_diffs()

    print(diff_markdown)  # Log output

    # GitHub Actions output
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{diff_markdown}\nEOF\n")
