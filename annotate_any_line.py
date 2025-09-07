# annotate_any_line.py
# Create GitHub Actions annotations on ANY line of TARGET_FILE.
# These show in the PR Checks UI and in the Files changed view as markers.

import os, json, sys

LEVELS = {"notice": "notice", "warning": "warning", "error": "error"}

def main():
    target = os.environ.get("TARGET_FILE")
    if not target:
        print("::error ::TARGET_FILE env is required")
        sys.exit(1)

    try:
        comments = json.loads(os.environ.get("COMMENTS", "[]"))
    except Exception as e:
        print(f"::error ::Invalid COMMENTS JSON: {e}")
        sys.exit(1)

    # Ensure file exists in workspace (so GitHub can link annotations)
    if not os.path.exists(target):
        print(f"::warning ::{target} not found in workspace. Annotations may not link.")
    
    posted = 0
    for c in comments:
        line = int(c.get("line", 0))
        level = LEVELS.get(str(c.get("level", "notice")).lower(), "notice")
        msg = str(c.get("message", "")).replace("\n", r"\n")

        if line <= 0:
            print(f"::warning ::Skipping invalid line '{line}' for {target}")
            continue

        # GitHub Actions annotation command format:
        # ::[notice|warning|error] file=...,line=...,endLine=...,title=...::message
        print(f"::{level} file={target},line={line}::{msg}")
        posted += 1

    print(f"Posted {posted} annotation(s).")
    # Non-zero exit not needed—annotations are already printed.

if __name__ == "__main__":
    main()
