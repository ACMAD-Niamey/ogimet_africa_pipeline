import argparse
from pathlib import Path
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    root = Path(args.root_dir).expanduser()
    out = Path(args.output_dir)

    # Create the output directory
    out.mkdir(parents=True, exist_ok=True)

    # Write a file that the next steps can use
    base_file = out / "base_dir.txt"
    base_file.write_text(str(root))

    # Optional: metadata for debugging
    meta = {
        "root_dir": str(root),
        "output_dir": str(out)
    }
    (out / "setup_metadata.json").write_text(json.dumps(meta, indent=2))

    print("✅ setup component completed successfully.")
    print(f"📌 Wrote {base_file}")

if __name__ == "__main__":
    main()
