import argparse
from pathlib import Path
from datetime import datetime, timedelta
import time
import requests

METAR_URL = "http://www.ogimet.com/cgi-bin/getmetar"

#-----------------------------------------------------------------------

def http_get(url):
    headers = {"User-Agent": "Mozilla/5.0 (AzureML-Ogimet-Pipeline)"}
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=45, headers=headers)
            if r.status_code == 200 and len(r.text.strip()) > 0:
                return r.text
        except:
            pass
        time.sleep(2 ** attempt)
    return None

#-----------------------------------------------------------------------

def day_windows(day):
    ds = day.strftime("%Y%m%d")
    return [
        (ds + "0000", ds + "0600"),
        (ds + "0601", ds + "1200"),
        (ds + "1201", ds + "1800"),
        (ds + "1801", ds + "2359"),
    ]

#-----------------------------------------------------------------------

def run_metar(output_dir: Path):

    prefixes = [
        'DA','DB','DF','DG','DI','DN','DR','DX','EB','ED','FE','FG','FH','FI','FK','FL',
        'FN','FO','FP','FQ','FS','FT','FV','FW','FY','FZ','GA','GB','GC','GD','GE','GF',
        'GG','GL','GM','GO','GQ','GU','GV','HB','HC','HD','HE','HH','HK','HL','HR','HS',
        'HT','HU','HW'
    ]

    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    days = [yesterday]

    for prefix in prefixes:
        print(f"\n🛫 Prefix {prefix}")

        for day in days:
            print(f"📆 {day.strftime('%Y-%m-%d')}")

            for begin, end in day_windows(day):
                url = f"{METAR_URL}?icao={prefix}&begin={begin}&end={end}&header=yes&lang=eng"
                data = http_get(url)

                if data is None:
                    print(f"⚠️ Empty window {begin} → {end}")
                    continue

                y = day.strftime("%Y")
                m = day.strftime("%m")
                d = day.strftime("%d")

                out_dir = output_dir / y / m / d / prefix
                out_dir.mkdir(parents=True, exist_ok=True)

                fname = out_dir / f"METAR_{begin}_to_{end}.csv"

                with open(fname, "w") as f:
                    f.write(data)

                print(f"📁 Saved {fname}")
                time.sleep(0.5)

    print("🎉 METAR ingestion complete.")

#-----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_dir", type=str, required=False)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    run_metar(out)

if __name__ == "__main__":
    main()
