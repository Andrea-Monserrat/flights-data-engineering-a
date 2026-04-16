import sys
import gc
import argparse
import logging

import pandas as pd
import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

SILVER_DB = "flights_silver"


# Nombres reales del schema Bronze en S3
REQUIRED_COLUMNS = [
    "year",
    "month",
    "day",
    "airline",
    "origin_airport",
    "departure_delay",
    "arrival_delay",
    "cancelled",
    "weather_delay",
]


def extract(bucket: str):
    ruta_s3 = f"s3://{bucket}/flights/bronze/flights/"

    try:
        logger.info("Leyendo flights Bronze desde %s por chunks...", ruta_s3)

        return wr.s3.read_parquet(
            path=ruta_s3,
            dataset=True,
            chunked=True,
            columns=REQUIRED_COLUMNS,
            use_threads=False,
        )

    except Exception:
        logger.exception("Error leyendo flights Bronze desde S3")
        sys.exit(1)


def validate_chunk(df: pd.DataFrame) -> None:
    try:
        assert df is not None, "Chunk es None"
        assert not df.empty, "Chunk está vacío"

        # validar contra schema real en minúsculas
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f"Falta la columna requerida '{col}'"

        for col in ["year", "month", "day", "airline", "origin_airport", "cancelled"]:
            assert df[col].notna().all(), f"La columna clave '{col}' contiene nulos"

    except AssertionError as e:
        logger.exception("Validación fallida en chunk: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error validando chunk")
        sys.exit(1)


def prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()

        # normalizar a mayúsculas para el resto del pipeline
        df.columns = [c.upper() for c in df.columns]

        df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int16")
        df["MONTH"] = pd.to_numeric(df["MONTH"], errors="coerce").astype("Int8")
        df["DAY"] = pd.to_numeric(df["DAY"], errors="coerce").astype("Int8")
        df["CANCELLED"] = pd.to_numeric(df["CANCELLED"], errors="coerce").fillna(0).astype("Int8")

        df["DEPARTURE_DELAY"] = pd.to_numeric(df["DEPARTURE_DELAY"], errors="coerce").astype("float32")
        df["ARRIVAL_DELAY"] = pd.to_numeric(df["ARRIVAL_DELAY"], errors="coerce").astype("float32")
        df["WEATHER_DELAY"] = pd.to_numeric(df["WEATHER_DELAY"], errors="coerce").fillna(0).astype("float32")

        df["AIRLINE"] = df["AIRLINE"].astype("string")
        df["ORIGIN_AIRPORT"] = df["ORIGIN_AIRPORT"].astype("string")

        return df

    except Exception:
        logger.exception("Error preparando chunk")
        sys.exit(1)


def build_daily_partial(df: pd.DataFrame) -> pd.DataFrame:
    try:
        tmp = pd.DataFrame({
            "YEAR": df["YEAR"],
            "MONTH": df["MONTH"],
            "DAY": df["DAY"],
            "total_flights": 1,
            "total_delayed": (df["DEPARTURE_DELAY"] > 0).astype("int32"),
            "total_cancelled": (df["CANCELLED"] == 1).astype("int32"),
            "dep_delay_sum": df["DEPARTURE_DELAY"].where(df["CANCELLED"] != 1, 0).fillna(0),
            "dep_delay_count": df["DEPARTURE_DELAY"].where(df["CANCELLED"] != 1).notna().astype("int32"),
            "arr_delay_sum": df["ARRIVAL_DELAY"].where(df["CANCELLED"] != 1, 0).fillna(0),
            "arr_delay_count": df["ARRIVAL_DELAY"].where(df["CANCELLED"] != 1).notna().astype("int32"),
        })

        return (
            tmp.groupby(["YEAR", "MONTH", "DAY"], dropna=False, as_index=False)
            .agg({
                "total_flights": "sum",
                "total_delayed": "sum",
                "total_cancelled": "sum",
                "dep_delay_sum": "sum",
                "dep_delay_count": "sum",
                "arr_delay_sum": "sum",
                "arr_delay_count": "sum",
            })
        )

    except Exception:
        logger.exception("Error construyendo agregado parcial flights_daily")
        sys.exit(1)


def build_monthly_partial(df: pd.DataFrame) -> pd.DataFrame:
    try:
        tmp = pd.DataFrame({
            "MONTH": df["MONTH"],
            "AIRLINE": df["AIRLINE"],
            "total_flights": 1,
            "total_delayed": (df["DEPARTURE_DELAY"] > 0).astype("int32"),
            "total_cancelled": (df["CANCELLED"] == 1).astype("int32"),
            "arr_delay_sum": df["ARRIVAL_DELAY"].fillna(0),
            "arr_delay_count": df["ARRIVAL_DELAY"].notna().astype("int32"),
            "on_time_count": (df["ARRIVAL_DELAY"] <= 15).fillna(False).astype("int32"),
        })

        return (
            tmp.groupby(["MONTH", "AIRLINE"], dropna=False, as_index=False)
            .agg({
                "total_flights": "sum",
                "total_delayed": "sum",
                "total_cancelled": "sum",
                "arr_delay_sum": "sum",
                "arr_delay_count": "sum",
                "on_time_count": "sum",
            })
        )

    except Exception:
        logger.exception("Error construyendo agregado parcial flights_monthly")
        sys.exit(1)


def build_airport_partial(df: pd.DataFrame) -> pd.DataFrame:
    try:
        total_delay_positive = df["DEPARTURE_DELAY"].clip(lower=0).fillna(0)

        tmp = pd.DataFrame({
            "ORIGIN_AIRPORT": df["ORIGIN_AIRPORT"],
            "total_departures": 1,
            "total_delayed": (df["DEPARTURE_DELAY"] > 0).astype("int32"),
            "total_cancelled": (df["CANCELLED"] == 1).astype("int32"),
            "dep_delay_sum": df["DEPARTURE_DELAY"].where(df["CANCELLED"] != 1, 0).fillna(0),
            "dep_delay_count": df["DEPARTURE_DELAY"].where(df["CANCELLED"] != 1).notna().astype("int32"),
            "weather_delay_minutes": df["WEATHER_DELAY"].fillna(0),
            "total_delay_minutes": total_delay_positive,
        })

        return (
            tmp.groupby(["ORIGIN_AIRPORT"], dropna=False, as_index=False)
            .agg({
                "total_departures": "sum",
                "total_delayed": "sum",
                "total_cancelled": "sum",
                "dep_delay_sum": "sum",
                "dep_delay_count": "sum",
                "weather_delay_minutes": "sum",
                "total_delay_minutes": "sum",
            })
        )

    except Exception:
        logger.exception("Error construyendo agregado parcial flights_by_airport")
        sys.exit(1)


def reduce_partials(partials: list[pd.DataFrame], group_cols: list[str], sum_cols: list[str]) -> pd.DataFrame:
    try:
        assert partials, "No se generaron agregados parciales"

        df = pd.concat(partials, ignore_index=True)

        return (
            df.groupby(group_cols, dropna=False, as_index=False)[sum_cols]
            .sum()
        )

    except AssertionError as e:
        logger.exception("Validación fallida reduciendo parciales: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error reduciendo agregados parciales")
        sys.exit(1)


def finalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()
        df["avg_departure_delay"] = df["dep_delay_sum"] / df["dep_delay_count"].replace(0, pd.NA)
        df["avg_arrival_delay"] = df["arr_delay_sum"] / df["arr_delay_count"].replace(0, pd.NA)

        return df[
            [
                "YEAR",
                "MONTH",
                "DAY",
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "avg_departure_delay",
                "avg_arrival_delay",
            ]
        ]

    except Exception:
        logger.exception("Error finalizando flights_daily")
        sys.exit(1)


def finalize_monthly(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()
        df["avg_arrival_delay"] = df["arr_delay_sum"] / df["arr_delay_count"].replace(0, pd.NA)
        df["on_time_pct"] = (df["on_time_count"] / df["total_flights"].replace(0, pd.NA)) * 100.0

        return df[
            [
                "MONTH",
                "AIRLINE",
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "avg_arrival_delay",
                "on_time_pct",
            ]
        ]

    except Exception:
        logger.exception("Error finalizando flights_monthly")
        sys.exit(1)


def finalize_airport(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()
        df["avg_departure_delay"] = df["dep_delay_sum"] / df["dep_delay_count"].replace(0, pd.NA)
        df["pct_weather_delay"] = (
            df["weather_delay_minutes"] / df["total_delay_minutes"].replace(0, pd.NA)
        ) * 100.0
        df["pct_weather_delay"] = df["pct_weather_delay"].fillna(0)

        return df[
            [
                "ORIGIN_AIRPORT",
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "avg_departure_delay",
                "pct_weather_delay",
            ]
        ]

    except Exception:
        logger.exception("Error finalizando flights_by_airport")
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


def write_daily(df: pd.DataFrame, bucket: str) -> None:
    ruta_s3 = f"s3://{bucket}/flights/silver/flights_daily/"

    try:
        validate_output(df, "flights_daily")

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


def write_monthly(df: pd.DataFrame, bucket: str) -> None:
    ruta_s3 = f"s3://{bucket}/flights/silver/flights_monthly/"

    try:
        validate_output(df, "flights_monthly")

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


def write_airport(df: pd.DataFrame, bucket: str) -> None:
    ruta_s3 = f"s3://{bucket}/flights/silver/flights_by_airport/"

    try:
        validate_output(df, "flights_by_airport")

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


def load(bucket: str, daily: pd.DataFrame, monthly: pd.DataFrame, airport: pd.DataFrame) -> None:
    try:
        logger.info("Creando base de datos %s en Glue...", SILVER_DB)
        wr.catalog.create_database(SILVER_DB, exist_ok=True)
    except Exception:
        logger.exception("Error creando la base de datos flights_silver")
        sys.exit(1)

    write_daily(daily, bucket)
    write_monthly(monthly, bucket)
    write_airport(airport, bucket)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    flights_iter = extract(args.bucket)

    daily_partials = []
    monthly_partials = []
    airport_partials = []
    total_rows = 0

    for i, chunk in enumerate(flights_iter, start=1):
        validate_chunk(chunk)
        chunk = prepare_chunk(chunk)

        daily_partials.append(build_daily_partial(chunk))
        monthly_partials.append(build_monthly_partial(chunk))
        airport_partials.append(build_airport_partial(chunk))

        total_rows += len(chunk)
        logger.info("Chunk %d procesado — %d filas acumuladas", i, total_rows)

        del chunk
        gc.collect()

    daily = reduce_partials(
        daily_partials,
        ["YEAR", "MONTH", "DAY"],
        [
            "total_flights",
            "total_delayed",
            "total_cancelled",
            "dep_delay_sum",
            "dep_delay_count",
            "arr_delay_sum",
            "arr_delay_count",
        ],
    )
    del daily_partials
    gc.collect()
    daily = finalize_daily(daily)

    monthly = reduce_partials(
        monthly_partials,
        ["MONTH", "AIRLINE"],
        [
            "total_flights",
            "total_delayed",
            "total_cancelled",
            "arr_delay_sum",
            "arr_delay_count",
            "on_time_count",
        ],
    )
    del monthly_partials
    gc.collect()
    monthly = finalize_monthly(monthly)

    airport = reduce_partials(
        airport_partials,
        ["ORIGIN_AIRPORT"],
        [
            "total_departures",
            "total_delayed",
            "total_cancelled",
            "dep_delay_sum",
            "dep_delay_count",
            "weather_delay_minutes",
            "total_delay_minutes",
        ],
    )
    del airport_partials
    gc.collect()
    airport = finalize_airport(airport)

    load(args.bucket, daily, monthly, airport)


if __name__ == "__main__":
    main()