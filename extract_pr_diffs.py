import os
import subprocess
import sys

def extract_pr_diffs(base_branch="origin/main", specific_file=None):
    script_name = os.path.basename(__file__)
    
    if specific_file:
        if not specific_file.endswith('.py'):
            specific_file += '.py'
        file_pattern = specific_file
        print(f"DEBUG: Looking for specific file: {specific_file}")
    else:
        file_pattern = "*.py"
        print(f"DEBUG: Looking for all Python files")
    
    # Changed this line to show incremental diff
    # diff_cmd = [
    #     "git", "diff", "HEAD~1", "HEAD", 
    #     "--", file_pattern, f":(exclude){script_name}"
    # ]

    diff_cmd = [
    "git", "diff", "HEAD~1", "HEAD", "--unified=0",
    "--", file_pattern, f":(exclude){script_name}"
    ]
    
    print(f"DEBUG: Command: {' '.join(diff_cmd)}")
    
    try:
        result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
        diff_output = result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return f"Error: {e}"
    
    if not diff_output:
        return f"No changes found for {specific_file or 'Python files'}"
    
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
                current_file = parts[3][2:]
        elif current_file:
            buffer.append(line)
    
    if current_file and buffer:
        file_diffs[current_file] = "\n".join(buffer)
    
    output = f"### Last Commit Changes for {specific_file or 'All Python Files'}\n\n"
    for fname, diff in file_diffs.items():
        output += f"#### File: `{fname}`\n```diff\n{diff}\n```\n\n"
    
    return output

if __name__ == "__main__":  # Fixed the syntax error
    specific_file = sys.argv[1] if len(sys.argv) > 1 else None
    if specific_file:
        print(f"*** FILTERING FOR: {specific_file} ***")
    
    result = extract_pr_diffs(specific_file=specific_file)
    print(result)
