import sys
import argparse
import logging

import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

GOLD_DB = "flights_gold"
GOLD_TABLE = "vuelos_analitica"

CTAS_SQL = """
CREATE TABLE flights_gold.vuelos_analitica AS
SELECT
    f.year,
    f.month,
    f.day,
    f.origin_airport,
    ap_orig.airport AS origin_airport_name,
    ap_orig.city AS origin_city,
    ap_orig.state AS origin_state,
    f.destination_airport,
    ap_dest.airport AS destination_airport_name,
    al.airline AS airline_name,
    f.departure_delay,
    f.arrival_delay,
    f.cancelled,
    f.cancellation_reason,
    f.distance,
    f.air_system_delay,
    f.airline_delay,
    f.weather_delay,
    f.late_aircraft_delay,
    f.security_delay
FROM flights_bronze.flights f
LEFT JOIN flights_bronze.airlines al
    ON f.airline = al.iata_code
LEFT JOIN flights_bronze.airports ap_orig
    ON f.origin_airport = ap_orig.iata_code
LEFT JOIN flights_bronze.airports ap_dest
    ON f.destination_airport = ap_dest.iata_code
"""


def setup(bucket: str) -> None:
    try:
        logger.info("Creando base de datos %s en Glue (si no existe)...", GOLD_DB)
        wr.catalog.create_database(GOLD_DB, exist_ok=True)
        logger.info("Base de datos %s lista.", GOLD_DB)
    except Exception:
        logger.exception("Error creando la base de datos %s", GOLD_DB)
        sys.exit(1)

    try:
        logger.info("Eliminando tabla %s.%s si existe...", GOLD_DB, GOLD_TABLE)
        wr.catalog.delete_table_if_exists(database=GOLD_DB, table=GOLD_TABLE)
        logger.info("Tabla %s.%s eliminada (o no existía).", GOLD_DB, GOLD_TABLE)
    except Exception:
        logger.exception("Error eliminando la tabla %s.%s", GOLD_DB, GOLD_TABLE)
        sys.exit(1)


def build_gold(bucket: str) -> None:
    s3_output = f"s3://{bucket}/athena-results/"

    try:
        logger.info("Ejecutando CTAS en Athena — destino: %s...", s3_output)

        wr.athena.read_sql_query(
            sql=CTAS_SQL,
            database=GOLD_DB,
            s3_output=s3_output,
            ctas_approach=False,
        )

        logger.info(
            "CTAS ejecutado exitosamente — tabla %s.%s creada.",
            GOLD_DB,
            GOLD_TABLE,
        )

    except Exception:
        logger.exception("Error ejecutando el CTAS en Athena")
        sys.exit(1)


def verify(bucket: str) -> None:
    s3_output = f"s3://{bucket}/athena-results/"

    try:
        logger.info("Verificando tabla %s.%s con SELECT LIMIT 5...", GOLD_DB, GOLD_TABLE)

        df = wr.athena.read_sql_query(
            sql=f"SELECT * FROM {GOLD_DB}.{GOLD_TABLE} LIMIT 5",
            database=GOLD_DB,
            s3_output=s3_output,
            ctas_approach=False,
        )

        assert df is not None, f"{GOLD_TABLE}: el resultado es None"
        assert not df.empty, f"{GOLD_TABLE}: la tabla está vacía"
        assert "airline_name" in df.columns, "Falta la columna airline_name"
        assert "origin_airport_name" in df.columns, "Falta la columna origin_airport_name"
        assert "destination_airport_name" in df.columns, "Falta la columna destination_airport_name"

        logger.info(
            "Verificación exitosa — %d filas de muestra obtenidas — columnas: %s",
            len(df),
            list(df.columns),
        )

    except AssertionError as e:
        logger.exception("Validación fallida en verify(): %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error verificando la tabla Gold")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Construye la tabla Gold vuelos_analitica via CTAS en Athena."
    )
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3")
    args = parser.parse_args()

    setup(args.bucket)
    build_gold(args.bucket)
    verify(args.bucket)


if __name__ == "__main__":
    main()