import os
import subprocess

# Run git diff for Python files only
diff = subprocess.run(
    ["git", "diff", "origin/main...HEAD", "--", "*.py"],
    capture_output=True, text=True
).stdout
# test
if not diff.strip():
    diff = "No Python changes detected."

print(diff)  # Shows up in GitHub Actions logs

# Export diff as GitHub Actions output
with open(os.environ["GITHUB_OUTPUT"], "a") as gh_out:
    gh_out.write(f"diff_markdown<<EOF\n{diff}\nEOF\n")
