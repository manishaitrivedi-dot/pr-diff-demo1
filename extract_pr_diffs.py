import os
import subprocess
import sys

def extract_pr_diffs(base_branch="origin/main", specific_file=None):
    """Extract Python file diffs, optionally for a specific file"""
    script_name = os.path.basename(__file__)
    print(f"DEBUG: Excluding script: {script_name}")
    
    # Build the file pattern
    if specific_file:
        if not specific_file.endswith('.py'):
            specific_file += '.py'
        file_pattern = specific_file
        print(f"DEBUG: Looking for specific file: {specific_file}")
    else:
        file_pattern = "*.py"
        print(f"DEBUG: Looking for all Python files")
    
    # Get diff with exclusion using git pathspec
    diff_cmd = [
        "git", "diff", f"{base_branch}...HEAD", 
        "--", file_pattern, f":(exclude){script_name}"
    ]
    
    print(f"DEBUG: Running command: {' '.join(diff_cmd)}")
    
    try:
        result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
        diff_output = result.stdout.strip()
        print(f"DEBUG: Got {len(diff_output)} characters of diff output")
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"
    
    if not diff_output:
        if specific_file:
            return f"No changes detected for {specific_file} (script excluded)."
        else:
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
    
    if specific_file:
        output = f"### Python Code Changes for `{specific_file}`\n\n"
    else:
        output = "### Python Code Changes (Script Excluded)\n\n"
    
    for fname, diff in file_diffs.items():
        output += f"#### File: `{fname}`\n```diff\n{diff}\n```\n\n"
    
    return output

if __name__ == "__main__":
    print("*** RUNNING NEW VERSION OF SCRIPT ***")
    
    # Check if a specific file was provided as argument
    specific_file = None
    if len(sys.argv) > 1:
        specific_file = sys.argv[1]
        print(f"*** FILTERING FOR SPECIFIC FILE: {specific_file} ***")
    
    result = extract_pr_diffs(specific_file=specific_file)
    print(result)
    
    # GitHub Actions output
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"diff_markdown<<EOF\n{result}\nEOF\n")
