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
    """
    Lee flights_bronze.flights desde S3 en formato Parquet.
    """
    ruta_s3 = f"s3://{bucket}/flights/bronze/flights/"

    try:
        logger.info("Leyendo Bronze flights desde %s...", ruta_s3)

        flights = wr.s3.read_parquet(
            path=ruta_s3,
            dataset=True
        )

        assert flights is not None, "flights es None"
        assert not flights.empty, "flights Bronze está vacío"

        logger.info(
            "Bronze flights leído exitosamente — %d filas — %s",
            len(flights),
            ruta_s3,
        )

        return flights

    except AssertionError as e:
        logger.exception("Validación fallida en extract(): %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error leyendo flights Bronze desde S3")
        sys.exit(1)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas a mayúsculas.
    """
    try:
        df = df.copy()
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception:
        logger.exception("Error normalizando nombres de columnas")
        sys.exit(1)


def validate_input(df: pd.DataFrame) -> None:
    """
    Valida columnas mínimas requeridas para construir Silver.
    """
    try:
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

    except AssertionError as e:
        logger.exception("Validación fallida en input Silver: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error validando input Silver")
        sys.exit(1)


def prepare_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas relevantes a numéricas para agregación.
    """
    try:
        df = df.copy()

        numeric_cols = [
            "YEAR",
            "MONTH",
            "DAY",
            "DEPARTURE_DELAY",
            "ARRIVAL_DELAY",
            "CANCELLED",
            "WEATHER_DELAY",
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # flags auxiliares
        df["IS_DELAYED"] = (df["DEPARTURE_DELAY"] > 0).astype(int)
        df["IS_CANCELLED"] = (df["CANCELLED"] == 1).astype(int)
        df["IS_ON_TIME_ARRIVAL"] = (df["ARRIVAL_DELAY"] <= 15).astype(int)

        # para excluir cancelados de promedios
        df["DEPARTURE_DELAY_VALID"] = df["DEPARTURE_DELAY"].where(df["CANCELLED"] != 1)
        df["ARRIVAL_DELAY_VALID"] = df["ARRIVAL_DELAY"].where(df["CANCELLED"] != 1)

        # minutos de retraso atribuibles a clima respecto al total de minutos de salida retrasados
        df["TOTAL_DELAY_MINUTES_POSITIVE"] = df["DEPARTURE_DELAY"].clip(lower=0)

        return df

    except Exception:
        logger.exception("Error preparando columnas numéricas")
        sys.exit(1)


def build_flights_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye flights_daily agregando por YEAR, MONTH, DAY.
    """
    try:
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

        return flights_daily

    except Exception:
        logger.exception("Error construyendo flights_daily")
        sys.exit(1)


def build_flights_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye flights_monthly agregando por MONTH y AIRLINE.
    """
    try:
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

        return flights_monthly

    except Exception:
        logger.exception("Error construyendo flights_monthly")
        sys.exit(1)


def build_flights_by_airport(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye flights_by_airport agregando por ORIGIN_AIRPORT.
    """
    try:
        agg = (
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

        agg["pct_weather_delay"] = (
            agg["weather_delay_minutes"] / agg["total_delay_minutes"]
        ) * 100.0

        agg["pct_weather_delay"] = agg["pct_weather_delay"].fillna(0)

        flights_by_airport = agg[
            [
                "ORIGIN_AIRPORT",
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "avg_departure_delay",
                "pct_weather_delay",
            ]
        ]

        return flights_by_airport

    except Exception:
        logger.exception("Error construyendo flights_by_airport")
        sys.exit(1)


def validate_output(df: pd.DataFrame, table_name: str) -> None:
    """
    Aplica validaciones con assert antes de escribir cada tabla a S3.
    """
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

        key_columns = {
            "flights_daily": ["YEAR", "MONTH", "DAY", "total_flights"],
            "flights_monthly": ["MONTH", "AIRLINE", "total_flights"],
            "flights_by_airport": ["ORIGIN_AIRPORT", "total_departures"],
        }

        for col in key_columns[table_name]:
            assert df[col].notna().all(), f"{table_name}: '{col}' contiene nulos"

        numeric_columns = {
            "flights_daily": [
                "YEAR",
                "MONTH",
                "DAY",
                "total_flights",
                "total_delayed",
                "total_cancelled",
            ],
            "flights_monthly": [
                "MONTH",
                "total_flights",
                "total_delayed",
                "total_cancelled",
                "on_time_pct",
            ],
            "flights_by_airport": [
                "total_departures",
                "total_delayed",
                "total_cancelled",
                "pct_weather_delay",
            ],
        }

        for col in numeric_columns[table_name]:
            assert pd.api.types.is_numeric_dtype(df[col]), (
                f"{table_name}: '{col}' debe ser numérica"
            )

    except AssertionError as e:
        logger.exception("Validación fallida antes de escribir %s: %s", table_name, e)
        sys.exit(1)
    except Exception:
        logger.exception("Error validando output %s", table_name)
        sys.exit(1)


def write_flights_daily(df: pd.DataFrame, bucket: str) -> None:
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

        logger.info(
            "flights_daily: subido exitosamente — %d filas — %s",
            len(df),
            ruta_s3,
        )

    except Exception:
        logger.exception("Error escribiendo flights_daily")
        sys.exit(1)


def write_flights_monthly(df: pd.DataFrame, bucket: str) -> None:
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

        logger.info(
            "flights_monthly: subido exitosamente — %d filas — %s",
            len(df),
            ruta_s3,
        )

    except Exception:
        logger.exception("Error escribiendo flights_monthly")
        sys.exit(1)


def write_flights_by_airport(df: pd.DataFrame, bucket: str) -> None:
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

        logger.info(
            "flights_by_airport: subido exitosamente — %d filas — %s",
            len(df),
            ruta_s3,
        )

    except Exception:
        logger.exception("Error escribiendo flights_by_airport")
        sys.exit(1)


def load(outputs: dict, bucket: str) -> None:
    """
    Crea flights_silver y escribe las tres tablas.
    """
    try:
        logger.info("Creando base de datos %s en Glue...", SILVER_DB)
        wr.catalog.create_database(SILVER_DB, exist_ok=True)
    except Exception:
        logger.exception("Error creando la base de datos flights_silver")
        sys.exit(1)

    write_flights_daily(outputs["flights_daily"], bucket)
    write_flights_monthly(outputs["flights_monthly"], bucket)
    write_flights_by_airport(outputs["flights_by_airport"], bucket)


def transform(flights: pd.DataFrame) -> dict:
    """
    Transforma Bronze y construye las tres tablas Silver.
    """
    try:
        flights = normalize_columns(flights)
        validate_input(flights)
        flights = prepare_numeric_columns(flights)

        flights_daily = build_flights_daily(flights)
        flights_monthly = build_flights_monthly(flights)
        flights_by_airport = build_flights_by_airport(flights)

        return {
            "flights_daily": flights_daily,
            "flights_monthly": flights_monthly,
            "flights_by_airport": flights_by_airport,
        }

    except Exception:
        logger.exception("Error transformando Bronze a Silver")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transforma flights Bronze a Silver en Parquet + Snappy y registra tablas en Glue."
    )
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3")
    args = parser.parse_args()

    flights = extract(args.bucket)
    outputs = transform(flights)
    load(outputs, args.bucket)


if __name__ == "__main__":
    main()