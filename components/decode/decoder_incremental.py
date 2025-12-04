import argparse
from pathlib import Path
import pandas as pd
import csv

# ----------------------------------------------------------
# Helper: read all raw OGIMET text files into a single table
# ----------------------------------------------------------

def load_raw_files(root: Path):
    records = []
    for f in root.rglob("*.csv"):
        try:
            text = f.read_text().strip()
            if not text:
                continue
            records.append((f, text))
        except Exception as ex:
            print(f"⚠️ Could not read {f}: {ex}")
    return records

# ----------------------------------------------------------
# Minimal decoder: converts raw text to structured rows
# ----------------------------------------------------------

def decode_synop_record(filepath: Path, text: str):

    lines = [line.strip() for line in text.split("\n") if line.strip()]
    rows = []

    for line in lines:
        # Basic parsing
        parts = line.split()
        rec = {
            "filepath": str(filepath),
            "raw": line,
            "WMO": None,
            "YY": None,
            "MM": None,
            "DD": None,
            "HH": None
        }

        # Try to detect WMO ID + timestamp from standard AAXX header
        if len(parts) >= 5 and parts[0] == "AAXX":
            try:
                rec["DDHH"] = parts[1]
                rec["WMO"] = parts[2]
            except:
                pass

        rows.append(rec)

    return rows

def decode_metar_record(filepath: Path, text: str):

    lines = [line.strip() for line in text.split("\n") if line.strip()]
    rows = []

    for line in lines:
        rec = {
            "filepath": str(filepath),
            "raw": line,
            "ICAO": None,
            "timestamp": None
        }

        parts = line.split()
        if len(parts) > 0:
            rec["ICAO"] = parts[0]
        rows.append(rec)

    return rows

# ----------------------------------------------------------
# Driver functions
# ----------------------------------------------------------

def decode_synop(root: Path, out: Path):
    out.mkdir(parents=True, exist_ok=True)
    records = load_raw_files(root)

    all_rows = []
    for (filepath, text) in records:
        all_rows.extend(decode_synop_record(filepath, text))

    df = pd.DataFrame(all_rows)
    df.to_csv(out / "decoded_synop.csv", index=False)
    print(f"📁 Saved decoded SYNOP → {out/'decoded_synop.csv'}")

def decode_metar(root: Path, out: Path):
    out.mkdir(parents=True, exist_ok=True)
    records = load_raw_files(root)

    all_rows = []
    for (filepath, text) in records:
        all_rows.extend(decode_metar_record(filepath, text))

    df = pd.DataFrame(all_rows)
    df.to_csv(out / "decoded_metar.csv", index=False)
    print(f"📁 Saved decoded METAR → {out/'decoded_metar.csv'}")

# ----------------------------------------------------------
# Main
# ----------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synop_dir", type=str, required=True)
    parser.add_argument("--metar_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    out = Path(args.output_dir)
    syn = Path(args.synop_dir)
    met = Path(args.metar_dir)

    decode_synop(syn, out / "synop")
    decode_metar(met, out / "metar")

    print("🎉 Decoding complete.")

if __name__ == "__main__":
    main()
