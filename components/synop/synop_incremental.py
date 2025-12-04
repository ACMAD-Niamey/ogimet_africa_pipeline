import argparse
from pathlib import Path
import time
import requests
from datetime import datetime

#-----------------------------------------------------------------------

def safe_get(url, retries=3, timeout=60):
    headers = {"User-Agent": "Mozilla/5.0 (AzureML-Ogimet-Pipeline)"}
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r.text
        except Exception as ex:
            print(f"⚠️ Attempt {attempt}/{retries} failed: {ex}")
            time.sleep(2)
    return None

#-----------------------------------------------------------------------

def ogimet_url(block, begin, end):
    return (
        "http://www.ogimet.com/cgi-bin/getsynop?"
        f"block={block}&begin={begin}&end={end}&header=yes"
    )

#-----------------------------------------------------------------------

def run_synop(base_dir: Path, output_dir: Path):
    hist_dir = output_dir / "historic"
    hist_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.utcnow()
    current_year = now.year
    current_month = now.month

    log_file = output_dir / "synop_log.txt"

    BLOCKS = list(range(60, 70))

    with open(log_file, "a") as log:
        log.write(f"\n=== SYNOP Ingestion Run: {datetime.utcnow()} ===\n")

        for block in BLOCKS:
            print(f"\n🌍 Block {block}")

            # Always ingest last 2 months (safe incremental)
            months_back = [(current_year, current_month)]
            if current_month == 1:
                months_back.append((current_year - 1, 12))
            else:
                months_back.append((current_year, current_month - 1))

            for (year, month) in months_back:
                print(f"➡️ Ingesting {year}-{month:02d}")

                ranges = [
                    (f"{year}{month:02d}010000", f"{year}{month:02d}100000"),
                    (f"{year}{month:02d}100001", f"{year}{month:02d}190001"),
                    (f"{year}{month:02d}190002", f"{year}{month:02d}280002"),
                    (f"{year}{month:02d}280003", f"{year}{month:02d}312359"),
                ]

                for begin, end in ranges:
                    url = ogimet_url(block, begin, end)
                    text = safe_get(url)

                    if not text:
                        print(f"❌ Failed window {begin} → {end}")
                        log.write(f"FAILED {block} {begin} {end}\n")
                        continue

                    out_dir = hist_dir / str(block) / str(year) / f"{month:02d}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    fname = out_dir / f"SYNOP_{begin}_to_{end}.csv"

                    with open(fname, "w") as f:
                        f.write(text)

                    print(f"📁 Saved {fname}")
                    log.write(f"SAVED {fname}\n")
                    time.sleep(1)

    print("🎉 SYNOP ingestion completed.")

#-----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    # base_dir is UNUSED now but kept for pipeline structure
    base = Path(args.base_dir)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    run_synop(base, out)

if __name__ == "__main__":
    main()
