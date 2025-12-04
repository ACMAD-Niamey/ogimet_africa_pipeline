import argparse
from pathlib import Path
import pandas as pd

# ----------------------------------------------------------
# Load decoded data
# ----------------------------------------------------------

def load_decoded(decoded_dir: Path):
    syn = decoded_dir / "synop" / "decoded_synop.csv"
    met = decoded_dir / "metar" / "decoded_metar.csv"

    df_syn = pd.read_csv(syn) if syn.exists() else pd.DataFrame()
    df_met = pd.read_csv(met) if met.exists() else pd.DataFrame()

    return df_syn, df_met

# ----------------------------------------------------------
# Placeholder aggregation logic (replace with your QC rules)
# ----------------------------------------------------------

def to_hourly(df):
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df.get("timestamp", pd.Timestamp.utcnow()))
    df["hour"] = df["timestamp"].dt.floor("H")
    return df.groupby("hour").size().reset_index(name="count")

def to_daily(df):
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df.get("timestamp", pd.Timestamp.utcnow()))
    df["day"] = df["timestamp"].dt.date
    return df.groupby("day").size().reset_index(name="count")

# ----------------------------------------------------------
# Main
# ----------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--decoded_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    decoded = Path(args.decoded_dir)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    df_syn, df_met = load_decoded(decoded)

    # Combine for aggregation
    df_all = pd.concat([df_syn, df_met], ignore_index=True)

    hourly = to_hourly(df_all)
    daily = to_daily(df_all)

    hourly.to_parquet(out / "hourly_africa.parquet")
    daily.to_parquet(out / "daily_africa.parquet")

    print("📁 Saved hourly and daily aggregated parquet files.")

if __name__ == "__main__":
    main()
