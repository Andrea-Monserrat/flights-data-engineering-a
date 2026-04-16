import sys
import argparse
import logging

import pandas as pd
import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

BRONZE_DB = "flights_bronze"
SILVER_DB = "flights_silver"


def extract(bucket: str) -> pd.DataFrame:
    ruta_s3 = f"s3://{bucket}/flights/bronze/flights/"

    try:
        logger.info("Leyendo flights Bronze desde %s...", ruta_s3)
        df = wr.s3.read_parquet(path=ruta_s3, dataset=True)

        assert df is not None, "flights Bronze es None"
        assert not df.empty, "flights Bronze está vacío"

        logger.info("flights Bronze leído exitosamente — %d filas — %s", len(df), ruta_s3)
        return df

    except AssertionError as e:
        logger.exception("Validación fallida en extract(): %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error leyendo flights Bronze")
        sys.exit(1)


def transform(df: pd.DataFrame) -> dict:
    try:
        df = df.copy()
        df.columns = [c.upper() for c in df.columns]

        required_columns = [
            "YEAR",
            "MONTH",
            "DAY",
            "AIRLINE",
            "ORIGIN_AIRPORT",
            "DEPARTURE_DELAY",
            "ARRIVAL_DELAY",
            "CANCELLED",
            "WEATHER_DELAY",
        ]

        assert df is not None, "Input DataFrame es None"
        assert not df.empty, "Input DataFrame está vacío"

        for col in required_columns:
            assert col in df.columns, f"Falta la columna requerida '{col}'"

        for col in ["YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT", "CANCELLED"]:
            assert df[col].notna().all(), f"La columna clave '{col}' contiene nulos"

        for col in ["YEAR", "MONTH", "DAY", "DEPARTURE_DELAY", "ARRIVAL_DELAY", "CANCELLED", "WEATHER_DELAY"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["IS_DELAYED"] = (df["DEPARTURE_DELAY"] > 0).astype(int)
        df["IS_CANCELLED"] = (df["CANCELLED"] == 1).astype(int)
        df["DEPARTURE_DELAY_VALID"] = df["DEPARTURE_DELAY"].where(df["CANCELLED"] != 1)
        df["ARRIVAL_DELAY_VALID"] = df["ARRIVAL_DELAY"].where(df["CANCELLED"] != 1)
        df["IS_ON_TIME_ARRIVAL"] = (df["ARRIVAL_DELAY"] <= 15).astype(int)
        df["TOTAL_DELAY_MINUTES_POSITIVE"] = df["DEPARTURE_DELAY"].clip(lower=0)

        flights_daily = (
            df.groupby(["YEAR", "MONTH", "DAY"], dropna=False)
            .agg(
                total_flights=("DAY", "size"),
                total_delayed=("IS_DELAYED", "sum"),
                total_cancelled=("IS_CANCELLED", "sum"),
                avg_departure_delay=("DEPARTURE_DELAY_VALID", "mean"),
                avg_arrival_delay=("ARRIVAL_DELAY_VALID", "mean"),
            )
            .reset_index()
        )

        flights_monthly = (
            df.groupby(["MONTH", "AIRLINE"], dropna=False)
            .agg(
                total_flights=("AIRLINE", "size"),
                total_delayed=("IS_DELAYED", "sum"),
                total_cancelled=("IS_CANCELLED", "sum"),
                avg_arrival_delay=("ARRIVAL_DELAY_VALID", "mean"),
                on_time_pct=("IS_ON_TIME_ARRIVAL", "mean"),
            )
            .reset_index()
        )
        flights_monthly["on_time_pct"] = flights_monthly["on_time_pct"] * 100.0

        flights_by_airport = (
            df.groupby(["ORIGIN_AIRPORT"], dropna=False)
            .agg(
                total_departures=("ORIGIN_AIRPORT", "size"),
                total_delayed=("IS_DELAYED", "sum"),
                total_cancelled=("IS_CANCELLED", "sum"),
                avg_departure_delay=("DEPARTURE_DELAY_VALID", "mean"),
                weather_delay_minutes=("WEATHER_DELAY", "sum"),
                total_delay_minutes=("TOTAL_DELAY_MINUTES_POSITIVE", "sum"),
            )
            .reset_index()
        )
        flights_by_airport["pct_weather_delay"] = (
            flights_by_airport["weather_delay_minutes"] / flights_by_airport["total_delay_minutes"]
        ) * 100.0
        flights_by_airport["pct_weather_delay"] = flights_by_airport["pct_weather_delay"].fillna(0)
        flights_by_airport = flights_by_airport[
            [
                "ORIGIN_AIRPORT",
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "avg_departure_delay",
                "pct_weather_delay",
            ]
        ]

        return {
            "flights_daily": flights_daily,
            "flights_monthly": flights_monthly,
            "flights_by_airport": flights_by_airport,
        }

    except AssertionError as e:
        logger.exception("Validación fallida en transform(): %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error transformando Bronze a Silver")
        sys.exit(1)


def validate_output(df: pd.DataFrame, table_name: str) -> None:
    try:
        assert df is not None, f"{table_name}: DataFrame es None"
        assert not df.empty, f"{table_name}: DataFrame está vacío"

        expected_columns = {
            "flights_daily": [
                "YEAR",
                "MONTH",
                "DAY",
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "avg_departure_delay",
                "avg_arrival_delay",
            ],
            "flights_monthly": [
                "MONTH",
                "AIRLINE",
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "avg_arrival_delay",
                "on_time_pct",
            ],
            "flights_by_airport": [
                "ORIGIN_AIRPORT",
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "avg_departure_delay",
                "pct_weather_delay",
            ],
        }

        for col in expected_columns[table_name]:
            assert col in df.columns, f"{table_name}: falta la columna '{col}'"

    except AssertionError as e:
        logger.exception("Validación fallida antes de escribir %s: %s", table_name, e)
        sys.exit(1)
    except Exception:
        logger.exception("Error validando output %s", table_name)
        sys.exit(1)


def load(outputs: dict, bucket: str) -> None:
    try:
        logger.info("Creando base de datos %s en Glue...", SILVER_DB)
        wr.catalog.create_database(SILVER_DB, exist_ok=True)
    except Exception:
        logger.exception("Error creando la base de datos flights_silver")
        sys.exit(1)

    try:
        df = outputs["flights_daily"]
        validate_output(df, "flights_daily")
        ruta_s3 = f"s3://{bucket}/flights/silver/flights_daily/"
        wr.s3.to_parquet(
            df=df,
            path=ruta_s3,
            dataset=True,
            database=SILVER_DB,
            table="flights_daily",
            mode="overwrite_partitions",
            partition_cols=["MONTH"],
            compression="snappy",
        )
        logger.info("flights_daily: subido exitosamente — %d filas — %s", len(df), ruta_s3)
    except Exception:
        logger.exception("Error escribiendo flights_daily")
        sys.exit(1)

    try:
        df = outputs["flights_monthly"]
        validate_output(df, "flights_monthly")
        ruta_s3 = f"s3://{bucket}/flights/silver/flights_monthly/"
        wr.s3.to_parquet(
            df=df,
            path=ruta_s3,
            dataset=True,
            database=SILVER_DB,
            table="flights_monthly",
            mode="overwrite",
            compression="snappy",
        )
        logger.info("flights_monthly: subido exitosamente — %d filas — %s", len(df), ruta_s3)
    except Exception:
        logger.exception("Error escribiendo flights_monthly")
        sys.exit(1)

    try:
        df = outputs["flights_by_airport"]
        validate_output(df, "flights_by_airport")
        ruta_s3 = f"s3://{bucket}/flights/silver/flights_by_airport/"
        wr.s3.to_parquet(
            df=df,
            path=ruta_s3,
            dataset=True,
            database=SILVER_DB,
            table="flights_by_airport",
            mode="overwrite",
            compression="snappy",
        )
        logger.info("flights_by_airport: subido exitosamente — %d filas — %s", len(df), ruta_s3)
    except Exception:
        logger.exception("Error escribiendo flights_by_airport")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    flights = extract(args.bucket)
    outputs = transform(flights)
    load(outputs, args.bucket)


if __name__ == "__main__":
    main()