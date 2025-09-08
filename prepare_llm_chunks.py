import os, json

# ====== CONFIG YOU MAY EDIT QUICKLY ======
TARGET_FILE = os.environ.get("TARGET_FILE", "simple_test.py")
MAX_CHARS   = int(os.environ.get("MAX_CHARS", "1000"))  # small for demo; increase later
# Pick the exact lines you want comments on (1-based). You can also generate these from your LLM later.
MANUAL_TARGET_LINES = [5, 11, 13]   # <— EDIT THIS LIST to whatever lines you want
# ========================================

def chunk_full_file(text: str, max_chars: int):
    lines = text.splitlines()
    cur_start = 1
    cur_text  = ""
    cur_len   = 0
    for i, line in enumerate(lines, start=1):
        stamped = f"{i}:{line}"
        add_len = len(stamped) + 1
        if cur_len + add_len > max_chars and cur_text:
            yield (cur_start, i-1, cur_text)
            cur_start = i
            cur_text  = stamped
            cur_len   = add_len
        else:
            cur_text  = (cur_text + "\n" if cur_text else "") + stamped
            cur_len  += add_len
    if cur_text:
        yield (cur_start, len(lines), cur_text)

def main():
    if not os.path.exists(TARGET_FILE):
        raise SystemExit(f" {TARGET_FILE} not found in workspace")

    with open(TARGET_FILE, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    chunks = list(chunk_full_file(content, MAX_CHARS))
    out = []
    for idx, (a, b, text) in enumerate(chunks, start=1):
        out.append({
            "path": TARGET_FILE,
            "chunk_id": idx,
            "start_line": a,
            "end_line": b,
            "text": text
        })

    with open("llm_chunks.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # Save the lines you want to comment (edit MANUAL_TARGET_LINES above)
    with open("line_targets.json", "w", encoding="utf-8") as f:
        json.dump({TARGET_FILE: MANUAL_TARGET_LINES}, f, indent=2)

    print(f"Wrote llm_chunks.json ({len(out)} chunk(s))")
    print(f"Wrote line_targets.json for {TARGET_FILE}: {MANUAL_TARGET_LINES}")

if __name__ == "__main__":
    main()
