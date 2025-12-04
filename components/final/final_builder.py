import argparse
from pathlib import Path
import pandas as pd
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregated_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    agg = Path(args.aggregated_dir)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    hourly = agg / "hourly_africa.parquet"
    daily = agg / "daily_africa.parquet"

    dfh = pd.read_parquet(hourly) if hourly.exists() else pd.DataFrame()
    dfd = pd.read_parquet(daily) if daily.exists() else pd.DataFrame()

    dfh.to_parquet(out / "africa_weather_hourly.parquet")
    dfd.to_parquet(out / "africa_weather_daily.parquet")

    # ---------------------------------------------
    # Train/val/test split (simple 70/20/10)
    # ---------------------------------------------
    N = len(dfh)
    train = dfh.iloc[: int(N * 0.7)]
    val = dfh.iloc[int(N * 0.7): int(N * 0.9)]
    test = dfh.iloc[int(N * 0.9):]

    (out / "ml").mkdir(exist_ok=True)
    train.to_parquet(out / "ml" / "train.parquet")
    val.to_parquet(out / "ml" / "val.parquet")
    test.to_parquet(out / "ml" / "test.parquet")

    # ---------------------------------------------
    # Metadata summary
    # ---------------------------------------------
    meta = {
        "hourly_rows": len(dfh),
        "daily_rows": len(dfd),
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "columns_hourly": dfh.columns.tolist(),
        "columns_daily": dfd.columns.tolist(),
    }

    with open(out / "metadata_summary.json", "w") as f:
        json.dump(meta, f, indent=2)

    print("🎉 Final dataset built.")

if __name__ == "__main__":
    main()
