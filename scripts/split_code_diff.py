import subprocess
import tiktoken
from whatthepatch import parse_patch
import re
import os,sys
# --- Configuration ---
# The target context window size for the LLM.
CONTEXT_WINDOW_TOKENS = 100000 
# The base branch to compare against for generating the diff.
#BASE_BRANCH = "origin/main" 

def count_tokens(text: str, tokenizer) -> int:
    """Calculates the number of tokens in a given text."""
    return len(tokenizer.encode(text))

def get_git_diff(base_branch: str) -> str:
    """Generates the git diff against the specified base branch."""
    try:
        # Using unified=0 removes all context lines, creating the smallest possible diff.
        args = ["git", "diff", "--unified=0", base_branch]
        result = subprocess.run(args, capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running git diff: {e.stderr}")
        return ""

def format_patch_from_hunks(patch, hunks) -> str:
    """Reconstructs a diff string from a patch header and a list of hunks."""
    header = f"diff --git a/{patch.header.old_path} b/{patch.header.new_path}\n"
    header += f"--- a/{patch.header.old_path}\n"
    header += f"+++ b/{patch.header.new_path}\n"
    
    hunk_texts = []
    for hunk in hunks:
        hunk_texts.append(str(hunk))
        
    return header + "\n".join(hunk_texts)

def split_file_diff(patch, tokenizer) -> list[str]:
    """
    Splits a single file's diff into chunks based on functions/classes.
    This is the fallback when a whole file's diff is too large.
    """
    chunks = []
    current_hunks = []
    current_chunk_tokens = 0
    
    # Estimate header tokens once; it's part of every chunk from this file.
    #header_str = f"diff --git a/{patch.header.old_path} b/{patch.header.new_path}\n--- a/{patch.header.old_path}\n+++ b/{patch.header.new_path}\n"
    header_tokens = count_tokens(code_to_review, tokenizer)

    for hunk in patch.hunks:
        hunk_text = str(hunk)
        hunk_tokens = count_tokens(hunk_text, tokenizer)

        # Check for function/class definition in the hunk header (the '@@' line)
        # This signals a good logical point to split the diff.
        is_new_logical_block = hunk.section_header and re.match(r'^(class|def)\s+', hunk.section_header.strip())

        # If we find a new block and the current chunk is not empty,
        # we finalize the previous chunk.
        if is_new_logical_block and current_hunks:
            chunk_diff = format_patch_from_hunks(patch, current_hunks)
            chunks.append(chunk_diff)
            current_hunks = []
            current_chunk_tokens = 0
        
        # Add the new hunk to the current chunk.
        current_hunks.append(hunk)
        current_chunk_tokens += hunk_tokens

        # If even a single function/class chunk is getting too big, split it by hunk.
        # This is the final fallback to ensure we never breach the context window.
        if header_tokens + current_chunk_tokens > CONTEXT_WINDOW_TOKENS:
            # Pop the last hunk that made it overflow
            overflow_hunk = current_hunks.pop()
            
            # Finalize the chunk without the overflowing hunk (if there's anything left)
            if current_hunks:
                chunk_diff = format_patch_from_hunks(patch, current_hunks)
                chunks.append(chunk_diff)
            
            # The overflow hunk becomes the start of the next chunk.
            current_hunks = [overflow_hunk]
            current_chunk_tokens = count_tokens(str(overflow_hunk), tokenizer)

    # Add the last remaining chunk
    if current_hunks:
        chunk_diff = format_patch_from_hunks(patch, current_hunks)
        chunks.append(chunk_diff)
        
    return chunks


def create_diff_chunks(code_to_review,OUTPUT_CHUNKS_DIR) -> list[str]:
    """
    Main function to generate git diff and split it into chunks
    that fit within the LLM's context window.
    """
    print("Initializing tokenizer...")
    tokenizer = tiktoken.get_encoding("cl100k_base")
    full_diff = code_to_review

    if not full_diff:
        print("No diff found or git error occurred.")
        return []

    # --- Level 1: Try the entire diff first ---
    total_tokens = count_tokens(full_diff, tokenizer)
    print(f"Total diff has {total_tokens} tokens.")
    if total_tokens <= CONTEXT_WINDOW_TOKENS:
        print("Entire diff fits within the context window. Creating one chunk.")
        patches = list(parse_patch(full_diff))
        num_patches = len(patches)

        chunk_filename = "" # Initialize filename

        if num_patches == 1:
            # <<< NEW: Exactly one file changed, use its name >>>
            patch = patches[0]
            original_file_path = patch.header.new_path
            
            # Get just the filename, e.g., 'my_script.py' from 'src/app/my_script.py'
            base_filename = os.path.basename(original_file_path).strip()
            print(f" -> base_filename: '{base_filename}'")
            print(f" -> original_file_path: '{original_file_path}'")
            if not (base_filename.endswith(".py") or base_filename.endswith(".sql")):
                print(f" -> Skipping non-.py/.sql file: {base_filename}")       
            else:
                # Create the final output filename, e.g., 'my_script.py.diff'
                chunk_filename = os.path.join(OUTPUT_CHUNKS_DIR, f"{base_filename}")
                print(f" -> Single file detected: '{base_filename}'. Saving to specific filename.")
                with open(chunk_filename, "w") as f:
                    f.write(full_diff)
                print(f" -> Saved full diff chunk to {chunk_filename}")
                final_chunks = [full_diff]
                return final_chunks

        else:
            # <<< MODIFIED: Fallback for multiple files or no files >>>
            # If there are 0 or >1 files, a specific name is misleading.
            print(f" -> {num_patches} files detected. Using generic filename to represent the combined diff.")
            chunk_filename = os.path.join(OUTPUT_CHUNKS_DIR, "full_diff_chunk.py")
            with open(chunk_filename, "w") as f:
                f.write(full_diff)
            print(f" -> Saved full diff chunk to {chunk_filename}")
            final_chunks = [full_diff]
            return final_chunks

    # --- Level 2: Diff is too large, split by file ---
    print("Total diff exceeds context window. Splitting by file...")
    patches = list(parse_patch(full_diff))
    final_chunks = []
    chunk_counter = 0
    for patch in patches:
        # We only care about Python files for this logic
        original_file_path = patch.header.new_path
        base_filename = os.path.basename(original_file_path).strip() # Just the file name, e.g., 'my_script.py'
        print(f" -> base_filename: '{base_filename}'")
        print(f" -> original_file_path: '{original_file_path}'")
        # Filter for Python or SQL files (or other relevant types)
        if not (base_filename.endswith(".py") or base_filename.endswith(".sql")):
            print(f" -> Skipping non-.py/.sql file: {base_filename}")
            continue

        file_diff_str = str(patch)
        file_tokens = count_tokens(file_diff_str, tokenizer)
        
        print(f"\nProcessing file: {patch.header.new_path} ({file_tokens} tokens)")
        if file_tokens <= CONTEXT_WINDOW_TOKENS:
            print(" -> Fits in context window. Adding as a single chunk.")
            final_chunks.append(file_diff_str)
            # --- NEW: Save this chunk to a file ---
            # Use a unique name for each chunk
            chunk_filename = os.path.join(OUTPUT_CHUNKS_DIR, f"{base_filename}")
            with open(chunk_filename, "w") as f:
                f.write(file_diff_str)
            print(f" -> Saved chunk to '{base_filename}' to {chunk_filename}")
            chunk_counter += 1
        else:
            # --- Level 3: File is too large, split by function/class ---
            print(f"File diff for '{base_filename}' is too large. Splitting by logical blocks/functions...")
            individual_file_sub_chunks = split_file_diff(patch, tokenizer)
            print(f" -> Split '{base_filename}' into {len(individual_file_sub_chunks)} smaller chunks.")
            
            final_chunks.extend(individual_file_sub_chunks)
    
            for sub_chunk_idx, sub_chunk_content in enumerate(individual_file_sub_chunks):
                # Create a unique filename for each part of the split file
                # e.g., 'part_1_my_script.py', 'part_2_my_script.py'
                output_filename = os.path.join(OUTPUT_CHUNKS_DIR, f"part_{sub_chunk_idx + 1}_{base_filename}")
                with open(output_filename, "w") as f:
                    f.write(sub_chunk_content)
                print(f"   -> Saved sub-chunk {sub_chunk_idx + 1} for '{base_filename}' to {output_filename}")
                chunk_counter += 1
    
    print(f"\nSuccessfully saved {chunk_counter} diff files to '{os.path.abspath(OUTPUT_CHUNKS_DIR)}'.")
    return final_chunks

if __name__ == "__main__":
    print(f"Reading diff from file diff_code_to_review")
    if len(sys.argv) < 3:
        print("Usage: python split_code_diff.py <input_diff_file_path> <output_directory_path>", file=sys.stderr)
        sys.exit(1)

    input_diff_file = sys.argv[1]
    output_dir = sys.argv[2]
    OUTPUT_CHUNKS_DIR = output_dir
    if os.path.exists(OUTPUT_CHUNKS_DIR):
        import shutil
        shutil.rmtree(OUTPUT_CHUNKS_DIR)
        print(f"Cleaned up previous '{OUTPUT_CHUNKS_DIR}' directory.")
    os.makedirs(OUTPUT_CHUNKS_DIR, exist_ok=True)
    with open(input_diff_file, 'r') as file:
        code_to_review = file.read()
    diff_chunks = create_diff_chunks(code_to_review,OUTPUT_CHUNKS_DIR)

    # print("\n" + "="*50)
    # print(f"Generated {len(diff_chunks)} chunk(s) for the LLM.")
    # print("="*50 + "\n")

    # for i, chunk in enumerate(diff_chunks):
    #     print(f"--- Chunk {i+1} ---")
    #     print(chunk[:500] + "\n..." if len(chunk) > 500 else chunk) # Print a preview
    #     print("-"*(len(f"--- Chunk {i+1} ---")) + "\n")
    files_created = len(os.listdir(OUTPUT_CHUNKS_DIR))
    print(f"Number of chunk files created: {files_created}")
    
    # Use GITHUB_OUTPUT to communicate back to the workflow
    if 'GITHUB_OUTPUT' in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            print(f'files_created={files_created}', file=f)
