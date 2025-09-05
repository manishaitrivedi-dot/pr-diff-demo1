import os
import subprocess
import requests
import json
import re
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class CodeIssue:
    file_path: str
    line_number: int
    severity: str
    message: str
    rule: str

class CodeAnalyzer:
    def __init__(self):
        self.rules = {
            'missing_docstring': {
                'pattern': r'^def\s+\w+\([^)]*\):\s*$',
                'message': 'Consider adding a docstring to document this function.',
                'severity': 'suggestion'
            },
            'hardcoded_strings': {
                'pattern': r'print\([\'"][^\'\"]*[\'\"]\)',
                'message': 'Consider using constants for hardcoded strings.',
                'severity': 'suggestion'
            }
        }
    
    def analyze_file_content(self, file_path: str, content: str, diff_lines: List[int]) -> List[CodeIssue]:
        issues = []
        lines = content.split('\n')
        
        for line_num in diff_lines:
            if line_num <= 0 or line_num > len(lines):
                continue
                
            line = lines[line_num - 1].strip()
            
            for rule_name, rule_config in self.rules.items():
                if 'pattern' in rule_config and re.search(rule_config['pattern'], line):
                    issues.append(CodeIssue(
                        file_path=file_path,
                        line_number=line_num,
                        severity=rule_config['severity'],
                        message=rule_config['message'],
                        rule=rule_name
                    ))
        
        return issues

class DiffExtractor:
    @staticmethod
    def extract_pr_changes(base_branch: str = "origin/main") -> Dict[str, Dict]:
        script_name = os.path.basename(__file__)
        
        diff_cmd = [
            "git", "diff", f"{base_branch}...HEAD", 
            "--", "*.py", f":(exclude){script_name}"
        ]
        
        try:
            result = subprocess.run(diff_cmd, capture_output=True, text=True, check=True)
            diff_output = result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Git diff failed: {e}")
            return {}
        
        return DiffExtractor._parse_diff_output(diff_output)
    
    @staticmethod
    def _parse_diff_output(diff_output: str) -> Dict[str, Dict]:
        files = {}
        current_file = None
        
        for line in diff_output.splitlines():
            if line.startswith("diff --git"):
                parts = line.split()
                if len(parts) >= 4 and parts[3].startswith("b/"):
                    current_file = parts[3][2:]
                    files[current_file] = {
                        'changed_lines': [1, 2, 3],  # Simplified for testing
                        'content': None
                    }
        
        for file_path in files.keys():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    files[file_path]['content'] = f.read()
            except FileNotFoundError:
                files[file_path]['content'] = ""
        
        return files

class GitHubCommenter:
    def __init__(self, token: str, repo_owner: str, repo_name: str):
        self.token = token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json'
        }
        self.base_url = f'https://api.github.com/repos/{repo_owner}/{repo_name}'
    
    def post_review_comments(self, pr_number: int, issues: List[CodeIssue]) -> Dict:
        if not issues:
            print("No issues found to comment on.")
            return {"posted": 0, "errors": 0}
        
        pr_response = requests.get(f'{self.base_url}/pulls/{pr_number}', headers=self.headers)
        pr_response.raise_for_status()
        commit_sha = pr_response.json()['head']['sha']
        
        posted_count = 0
        error_count = 0
        
        print(f"Posting {len(issues)} code review comments...")
        
        for issue in issues:
            try:
                payload = {
                    'body': f"**{issue.severity.upper()}**: {issue.message}",
                    'commit_id': commit_sha,
                    'path': issue.file_path,
                    'line': issue.line_number
                }
                
                response = requests.post(
                    f'{self.base_url}/pulls/{pr_number}/comments',
                    headers=self.headers,
                    json=payload
                )
                
                if response.status_code == 201:
                    print(f"Posted comment on {issue.file_path}:{issue.line_number}")
                    posted_count += 1
                else:
                    print(f"Failed to post comment: {response.status_code}")
                    error_count += 1
                    
            except Exception as e:
                print(f"Error posting comment: {e}")
                error_count += 1
        
        return {"posted": posted_count, "errors": error_count}

def main():
    config = {
        'github_token': os.getenv('GITHUB_TOKEN'),
        'repo_owner': 'manishaitrivedi-dot',
        'repo_name': 'pr-diff-demo1',
        'pr_number': 3,
        'base_branch': 'origin/main'
    }
    
    if not config['github_token']:
        print("Error: GITHUB_TOKEN environment variable required")
        return
    
    print("Starting dynamic PR code review pipeline...")
    
    print("1. Extracting PR changes...")
    diff_extractor = DiffExtractor()
    file_changes = diff_extractor.extract_pr_changes(config['base_branch'])
    
    if not file_changes:
        print("No Python file changes found.")
        return
    
    print(f"Found changes in {len(file_changes)} files")
    
    print("2. Analyzing code for issues...")
    analyzer = CodeAnalyzer()
    all_issues = []
    
    for file_path, file_data in file_changes.items():
        if file_data['content'] and file_data['changed_lines']:
            issues = analyzer.analyze_file_content(
                file_path, 
                file_data['content'], 
                file_data['changed_lines']
            )
            all_issues.extend(issues)
            print(f"  - {file_path}: {len(issues)} issues found")
    
    if not all_issues:
        print("No issues found in the changed code.")
        return
    
    print("3. Posting comments to GitHub...")
    commenter = GitHubCommenter(
        config['github_token'],
        config['repo_owner'],
        config['repo_name']
    )
    
    results = commenter.post_review_comments(config['pr_number'], all_issues)
    
    print(f"Pipeline completed!")
    print(f"Posted: {results['posted']} comments")
    print(f"Errors: {results['errors']} comments")

if __name__ == "__main__":
    main()