import os
import subprocess
from pathlib import Path

def extract_pr_diffs(base_branch="origin/main"):
    """
    Extract Python (.py) code changes:
      - First commit → full diff against base.
      - Later commits → only last commit.
      - Always exclude this script itself using git pathspec.
    """
    # Get script name for exclusion
    script_name = os.path.basename(__file__)
    
    print(f"Script to exclude: {script_name}")  # Debug info
    
    # Count commits since base
    try:
        count_cmd = ["git", "rev-list", "--count", f"{base_branch}..HEAD"]
        commit_count = int(
            subprocess.run(count_cmd, capture_output=True, text=True, check=True).stdout.strip()
        )
        print(f"Commits since {base_branch}: {commit_count}")  # Debug info
    except subprocess.CalledProcessError:
        print(f"Warning: Could not count commits from {base_branch}. Using full diff.")
        commit_count = 1
    
    # Build diff command with git pathspec exclusion
    if commit_count <= 1:
        # First commit or single commit - diff against base
        diff_cmd = [
            "git", "diff", f"{base_branch}...HEAD", 
            "--", "*.py", f":(exclude){script_name}"
        ]
    else:
        # Multiple commits - only show last commit changes
        diff_cmd = [
            "git", "diff", "HEAD~1", "HEAD", 
            "--", "*.py", f":(exclude){script_name}"
        ]
    
    print(f"Running: {' '.join(diff_cmd)}")  # Debug info
    
    # Run diff
    try:
        result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
        diff_output = result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return f"Error running git diff: {e}"
    
    if not diff_output:
        return "No Python changes detected (excluding script file)."
    
    # Split by file for better formatting
    file_diffs = {}
    current_file = None
    buffer = []
    
    for line in diff_output.splitlines():
        if line.startswith("diff --git"):
            # Save previous file's changes
            if current_file and buffer:
                file_diffs[current_file] = "\n".join(buffer)
            buffer = []
            
            # Extract filename from "diff --git a/file.py b/file.py"
            parts = line.split()
            if len(parts) >= 4:
                # Get the b/ version (after changes)
                b_file = parts[3]
                if b_file.startswith("b/"):
                    current_file = b_file[2:]  # Remove "b/" prefix
                else:
                    current_file = b_file
            else:
                current_file = None
                
        elif current_file:
            buffer.append(line)
    
    # Handle the last file
    if current_file and buffer:
        file_diffs[current_file] = "\n".join(buffer)
    
    if not file_diffs:
        return "No Python changes detected (after exclusions)."
    
    # Format Markdown
    markdown_output = f"### Python Code Changes\n\n"
    markdown_output += f"**Base Branch:** `{base_branch}`\n"
    markdown_output += f"**Commits Analyzed:** {commit_count}\n"
    markdown_output += f"**Excluded:** `{script_name}`\n"
    markdown_output += "\n---\n"
    
    for fname, diff in file_diffs.items():
        markdown_output += f"\n#### File: `{fname}`\n```diff\n{diff}\n```\n"
    
    return markdown_output


def main():
    """Main function with error handling"""
    try:
        # Check if we're in a git repository
        subprocess.run(["git", "status"], capture_output=True, check=True)
        
        diff_markdown = extract_pr_diffs()
        print(diff_markdown)  # Log output
        
        # GitHub Actions output
        github_output = os.environ.get("GITHUB_OUTPUT")
        if github_output:
            try:
                with open(github_output, "a", encoding="utf-8") as f:
                    f.write(f"diff_markdown<<EOF\n{diff_markdown}\nEOF\n")
                print("✅ Successfully wrote to GITHUB_OUTPUT")
            except Exception as e:
                print(f"❌ Failed to write to GITHUB_OUTPUT: {e}")
        else:
            print("ℹ️  GITHUB_OUTPUT not found (not running in GitHub Actions)")
            
    except subprocess.CalledProcessError:
        print("❌ Error: Not in a git repository or git command failed")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
