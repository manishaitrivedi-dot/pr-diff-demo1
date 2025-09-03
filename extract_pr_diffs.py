import os
import subprocess
from pathlib import Path

def extract_pr_diffs(base_branch="origin/main"):
    """
    Extract Python (.py) code changes from the current PR compared to base_branch.
    Keeps track of original code context per file.
    """

    # Run git diff for Python files only
    result = subprocess.run(
        ["git", "diff", f"{base_branch}...HEAD", "--", "*.py"],
        capture_output=True,
        text=True,
        check=True
    )

    diff_output = result.stdout.strip()

    if not diff_output:
        return "No Python changes detected."

    # Split by file for clarity
    file_diffs = {}
    current_file = None
    buffer = []

    for line in diff_output.splitlines():
        if line.startswith("diff --git"):
            # Save previous file’s changes
            if current_file and buffer:
                file_diffs[current_file] = "\n".join(buffer)
                buffer = []
            # Extract filename
            parts = line.split(" b/")
            if len(parts) == 2:
                current_file = parts[1]
        elif current_file:
            buffer.append(line)

    
    if current_file and buffer:
        file_diffs[current_file] = "\n".join(buffer)

    
    markdown_output = "### Python Code Changes in PR\n"
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
