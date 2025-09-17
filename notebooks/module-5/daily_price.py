import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


def load_daily_prices(db_url, data_dir, table_name: str = "prices_daily"):
    engine = create_engine(db_url)
    data_path = Path(data_dir)

    for file in data_path.glob("mse-daily-*.csv"):
        print(f"Loading {file.name}...")
        df = pd.read_csv(file)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df_out = df.rename(
            columns={
                "daily_range_high": "high_mwk",
                "daily_range_low": "low_mwk",
                "buy_price": "open_mwk",
                "today_closing_price": "close_mwk",
                "volume_traded": "volume",
            }
        )[
            [
                "counter_id",
                "trade_date",
                "open_mwk",
                "high_mwk",
                "low_mwk",
                "close_mwk",
                "volume",
            ]
        ]
        # Insert into DB
        df_out.to_sql(table_name, engine, if_exists="append", index=False)

        print(f" Inserted {len(df_out)} rows from {file.name}")

    print("All files loaded successfully.")
