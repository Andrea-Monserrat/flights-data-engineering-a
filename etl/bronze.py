import sys
import argparse
import logging
import os

import pandas as pd
import awswrangler as wr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


GLUE_DATABASE = "flights_bronze"


def extract(data_dir: str) -> dict:
    """
    Valida que los tres archivos fuente existen y no están vacíos.
    Devuelve un diccionario con las rutas validadas.
    """
    archivos = {
        "flights": "flights.csv",
        "airlines": "airlines.csv",
        "airports": "airports.csv",
    }
    rutas = {}

    try:
        for nombre, archivo in archivos.items():
            ruta = os.path.join(data_dir, archivo)

            assert os.path.exists(ruta), f"No se encontró el archivo {ruta}"
            assert os.path.getsize(ruta) > 0, f"El archivo {ruta} está vacío"

            rutas[nombre] = ruta
            logger.info(
                "%s: archivo encontrado — %.1f MB — %s",
                nombre,
                os.path.getsize(ruta) / 1e6,
                ruta,
            )

        return rutas

    except AssertionError as e:
        logger.exception("Validación fallida en extract(): %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error inesperado durante extract()")
        sys.exit(1)


def validate_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """
    Valida expectativas mínimas antes de escribir a S3.
    Usa assert porque así lo exige la rúbrica.
    """
    try:
        assert df is not None, f"{table_name}: el DataFrame es None"
        assert not df.empty, f"{table_name}: el DataFrame está vacío"

        # Validación mínima de columnas esperadas por tabla
        expected_columns = {
            "flights": ["YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"],
            "airlines": ["IATA_CODE", "AIRLINE"],
            "airports": ["IATA_CODE", "AIRPORT", "CITY", "STATE", "COUNTRY"],
        }

        columnas_esperadas = expected_columns.get(table_name, [])
        for col in columnas_esperadas:
            assert col in df.columns, f"{table_name}: falta la columna requerida '{col}'"

        # Validación mínima de nulos en columnas clave
        key_columns = {
            "flights": ["YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"],
            "airlines": ["IATA_CODE", "AIRLINE"],
            "airports": ["IATA_CODE", "AIRPORT"],
        }

        columnas_clave = key_columns.get(table_name, [])
        for col in columnas_clave:
            assert df[col].notna().all(), f"{table_name}: la columna clave '{col}' contiene nulos"

        # Validación mínima de tipos en columnas críticas
        if table_name == "flights":
            assert pd.api.types.is_numeric_dtype(df["YEAR"]), "flights: YEAR debe ser numérica"
            assert pd.api.types.is_numeric_dtype(df["MONTH"]), "flights: MONTH debe ser numérica"
            assert pd.api.types.is_numeric_dtype(df["DAY"]), "flights: DAY debe ser numérica"


        if table_name in {"airlines", "airports"}:
            assert pd.api.types.is_object_dtype(df["IATA_CODE"]), f"{table_name}: IATA_CODE debe ser texto"

    except AssertionError as e:
        logger.exception("Validación fallida antes de escribir %s: %s", table_name, e)
        sys.exit(1)
    except Exception:
        logger.exception("Error inesperado validando %s", table_name)
        sys.exit(1)


def load_flights_in_chunks(ruta_csv: str, bucket: str, chunksize: int = 100_000) -> None:
    """
    Carga flights.csv en chunks a S3 como Parquet y lo registra en Glue.
    Mantiene idempotencia usando overwrite en el primer chunk y append en los siguientes.
    """
    ruta_s3 = f"s3://{bucket}/flights/bronze/flights/"
    total_filas = 0
    primer_chunk = True

    try:
        logger.info("Subiendo flights a %s en chunks de %d filas...", ruta_s3, chunksize)

        for chunk in pd.read_csv(ruta_csv, chunksize=chunksize):
            validate_dataframe(chunk, "flights")

            wr.s3.to_parquet(
                df=chunk,
                path=ruta_s3,
                dataset=True,
                database=GLUE_DATABASE,
                table="flights",
                mode="overwrite" if primer_chunk else "append",
            )

            total_filas += len(chunk)
            primer_chunk = False
            logger.info("flights: %d filas procesadas...", total_filas)

        assert total_filas > 0, "flights: no se cargó ninguna fila"

        logger.info(
            "flights: subido exitosamente — %d filas totales — %s",
            total_filas,
            ruta_s3,
        )

    except AssertionError as e:
        logger.exception("Validación fallida cargando flights: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error subiendo flights a S3")
        sys.exit(1)


def load_small_table(nombre: str, ruta_csv: str, bucket: str) -> None:
    """
    Carga una tabla pequeña completa en memoria y la escribe a S3 como Parquet.
    """
    ruta_s3 = f"s3://{bucket}/flights/bronze/{nombre}/"

    try:
        logger.info("Subiendo %s a %s...", nombre, ruta_s3)

        df = pd.read_csv(ruta_csv)
        validate_dataframe(df, nombre)

        wr.s3.to_parquet(
            df=df,
            path=ruta_s3,
            dataset=True,
            database=GLUE_DATABASE,
            table=nombre,
            mode="overwrite",
        )

        logger.info(
            "%s: subido exitosamente — %d filas — %s",
            nombre,
            len(df),
            ruta_s3,
        )


    except Exception:
        logger.exception("Error subiendo %s a S3", nombre)
        sys.exit(1)


def load(rutas: dict, bucket: str) -> None:
    """
    Crea la base de datos Glue y carga las tres tablas en Bronze.
    """
    try:
        logger.info("Creando base de datos %s en Glue...", GLUE_DATABASE)
        wr.catalog.create_database(GLUE_DATABASE, exist_ok=True)
    except Exception:
        logger.exception("Error creando la base de datos en Glue")
        sys.exit(1)

    load_flights_in_chunks(rutas["flights"], bucket)
    load_small_table("airlines", rutas["airlines"], bucket)
    load_small_table("airports", rutas["airports"], bucket)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carga archivos CSV locales a Bronze en S3 y registra tablas en Glue."
    )
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3 de destino")
    parser.add_argument(
        "--data-dir",
        required=False,
        default="data/",
        help="Directorio local que contiene flights.csv, airlines.csv y airports.csv",
    )
    args = parser.parse_args()

    rutas = extract(args.data_dir)
    load(rutas, args.bucket)


if __name__ == "__main__":
    main()