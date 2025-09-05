import os
import requests

REPO = "manishaitrivedi-dot/pr-diff-demo1"
PR_NUMBER = 3
GITHUB_TOKEN = os.environ["GH_TOKEN"]

headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Debug: Check what files GitHub API actually sees
files_url = f"https://api.github.com/repos/{REPO}/pulls/{PR_NUMBER}/files"
files_resp = requests.get(files_url, headers=headers)

print("=== FILES IN PR (GitHub API view) ===")
if files_resp.status_code == 200:
    for file in files_resp.json():
        print(f"File: {file['filename']}")
        print(f"Status: {file['status']}")
        print(f"Changes: +{file['additions']} -{file['deletions']}")
        print("---")
else:
    print(f"Failed to get files: {files_resp.status_code}")

# Check if simple_test.py is in the list
simple_test_found = any(f['filename'] == 'simple_test.py' for f in files_resp.json())
print(f"simple_test.py found in PR: {simple_test_found}")
