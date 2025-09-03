import subprocess
import sys

def get_diff():
    # Get the diff for this PR
    try:
        result = subprocess.run(
            ["git", "diff", "origin/main...HEAD"],
            capture_output=True, text=True, check=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running git diff:", e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    get_diff()
