import os
import json

def parse_diff(diff_text):
    file_changes = {}
    current_file = None

    for line in diff_text.splitlines():
        if line.startswith("diff --git"):
            parts = line.split(" ")
            fname = parts[2][2:]  # "a/path/to/file.py"
            if fname.endswith(".py"):  # only track Python files
                current_file = fname
                file_changes[current_file] = []
            else:
                current_file = None
        elif current_file:
            file_changes[current_file].append(line)

    return file_changes

def main():
    diff_file = "diff.patch"
    if not os.path.exists(diff_file):
        print("No diff.patch file found")
        return

    with open(diff_file, "r") as f:
        diff_text = f.read()

    changes = parse_diff(diff_text)

    # Save structured result
    with open("parsed_diffs.json", "w") as f:
        json.dump(changes, f, indent=2)

    # Print summary (so GitHub Actions log shows it)
    for fname, lines in changes.items():
        print(f"\nFile: {fname}")
        for line in lines:
            print(line)

if __name__ == "__main__":
    main()
