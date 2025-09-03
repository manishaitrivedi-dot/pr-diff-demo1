import os
import subprocess

def extract_pr_diffs(base_branch="origin/main"):
    """Extract Python file diffs, excluding this script"""
    script_name = os.path.basename(__file__)
    print(f"DEBUG: Excluding script: {script_name}")
    
    # Get diff with exclusion using git pathspec
    diff_cmd = [
        "git", "diff", f"{base_branch}...HEAD", 
        "--", "*.py", f":(exclude){script_name}"
    ]
    
    print(f"DEBUG: Running command: {' '.join(diff_cmd)}")
    
    try:
        result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
        diff_output = result.stdout.strip()
        print(f"DEBUG: Got {len(diff_output)} characters of diff output")
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"
    
    if not diff_output:
        return "No Python changes detected (script excluded)."
    
    # Parse files
    file_diffs = {}
    current_file = None
    buffer = []
    
    for line in diff_output.splitlines():
        if line.startswith("diff --git"):
            if current_file and buffer:
                file_diffs[current_file] = "\n".join(buffer)
            buffer = []
            
            parts = line.split()
            if len(parts) >= 4 and parts[3].startswith("b/"):
                current_file = parts[3][2:]  # Remove "b/"
                print(f"DEBUG: Found file: {current_file}")
        elif current_file:
            buffer.append(line)
    
    if current_file and buffer:
        file_diffs[current_file] = "\n".join(buffer)
    
    print(f"DEBUG: Final files to show: {list(file_diffs.keys())}")
    
    # Format output
    if not file_diffs:
        return "No changes found after exclusions."
    
    output = "### Python Code Changes (NEW VERSION - Script Excluded)\n\n"
    for fname, diff in file_diffs.items():
        output += f"#### File: `{fname}`\n```diff\n{diff}\n```\n\n"
    
    return output

if __name__ == "__main__":
    print("*** RUNNING NEW VERSION OF SCRIPT ***")
    result = extract_pr_diffs()
    print(result)
    
    # GitHub Actions output
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{result}\nEOF\n")
