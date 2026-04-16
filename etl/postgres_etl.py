import sys
import json
import argparse
import logging

import boto3
import pandas as pd
from sqlalchemy import create_engine, insert, BigInteger, Integer, Float, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped, Session
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA — SQLAlchemy 2.0 Declarative Base
# ─────────────────────────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    iata_code: Mapped[str]           = mapped_column(String(10),  primary_key=True)
    airline:   Mapped[str]           = mapped_column(String(100), nullable=False)


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str]           = mapped_column(String(10),  primary_key=True)
    airport:   Mapped[Optional[str]] = mapped_column(String(200))
    city:      Mapped[Optional[str]] = mapped_column(String(100))
    state:     Mapped[Optional[str]] = mapped_column(String(50))
    country:   Mapped[Optional[str]] = mapped_column(String(50))
    latitude:  Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)


class Flight(Base):
    __tablename__ = "flights"

    id:                   Mapped[int]            = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    year:                 Mapped[Optional[int]]  = mapped_column(Integer)
    month:                Mapped[Optional[int]]  = mapped_column(Integer)
    day:                  Mapped[Optional[int]]  = mapped_column(Integer)
    day_of_week:          Mapped[Optional[int]]  = mapped_column(Integer)
    airline:              Mapped[Optional[str]]  = mapped_column(String(10), ForeignKey("airlines.iata_code"))
    flight_number:        Mapped[Optional[str]]  = mapped_column(String(10))
    tail_number:          Mapped[Optional[str]]  = mapped_column(String(10))
    origin_airport:       Mapped[Optional[str]]  = mapped_column(String(10), ForeignKey("airports.iata_code"))
    destination_airport:  Mapped[Optional[str]]  = mapped_column(String(10), ForeignKey("airports.iata_code"))
    scheduled_departure:  Mapped[Optional[str]]  = mapped_column(String(10))
    departure_time:       Mapped[Optional[float]] = mapped_column(Float)
    departure_delay:      Mapped[Optional[float]] = mapped_column(Float)
    taxi_out:             Mapped[Optional[float]] = mapped_column(Float)
    wheels_off:           Mapped[Optional[float]] = mapped_column(Float)
    scheduled_time:       Mapped[Optional[float]] = mapped_column(Float)
    elapsed_time:         Mapped[Optional[float]] = mapped_column(Float)
    air_time:             Mapped[Optional[float]] = mapped_column(Float)
    distance:             Mapped[Optional[float]] = mapped_column(Float)
    wheels_on:            Mapped[Optional[float]] = mapped_column(Float)
    taxi_in:              Mapped[Optional[float]] = mapped_column(Float)
    scheduled_arrival:    Mapped[Optional[str]]   = mapped_column(String(10))
    arrival_time:         Mapped[Optional[float]] = mapped_column(Float)
    arrival_delay:        Mapped[Optional[float]] = mapped_column(Float)
    cancelled:            Mapped[Optional[int]]   = mapped_column(Integer)
    cancellation_reason:  Mapped[Optional[str]]   = mapped_column(String(5))
    air_system_delay:     Mapped[Optional[float]] = mapped_column(Float)
    security_delay:       Mapped[Optional[float]] = mapped_column(Float)
    airline_delay:        Mapped[Optional[float]] = mapped_column(Float)
    late_aircraft_delay:  Mapped[Optional[float]] = mapped_column(Float)
    weather_delay:        Mapped[Optional[float]] = mapped_column(Float)


# ─────────────────────────────────────────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────────────────────────────────────────

def get_credentials(secret_name: str, region: str) -> dict:
    """Recupera credenciales desde AWS Secrets Manager."""
    try:
        client = boto3.client("secretsmanager", region_name=region)
        secret = client.get_secret_value(SecretId=secret_name)
        creds = json.loads(secret["SecretString"])
        logger.info("Credenciales obtenidas desde Secrets Manager: %s", secret_name)
        return creds
    except Exception:
        logger.exception("Error obteniendo credenciales de Secrets Manager")
        sys.exit(1)


def build_engine(endpoint: str, creds: dict):
    """Construye el engine de SQLAlchemy para PostgreSQL."""
    try:
        url = (
            f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
            f"@{endpoint}:{creds['port']}/{creds['dbname']}"
        )
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        logger.info("Conexión exitosa a PostgreSQL: %s", endpoint)
        return engine
    except Exception:
        logger.exception("Error conectando a PostgreSQL en %s", endpoint)
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

def create_schema(engine) -> None:
    """Crea las tablas en PostgreSQL. Idempotente: drop + create en orden FK."""
    try:
        logger.info("Creando schema en PostgreSQL (drop + create)...")
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        logger.info("Schema creado: airlines, airports, flights")
    except Exception:
        logger.exception("Error creando el schema")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────────────────────────────────────────

def _records_from_df(df: pd.DataFrame) -> list[dict]:
    """Convierte DataFrame a lista de dicts convirtiendo NaN/NaT a None."""
    return [
        {k: None if pd.isnull(v) else v for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


def load_airlines(session: Session, data_dir: str) -> None:
    try:
        df = pd.read_csv(f"{data_dir}/airlines.csv")
        df.columns = [c.strip().lower() for c in df.columns]

        assert not df.empty, "airlines.csv está vacío"
        assert "iata_code" in df.columns, "airlines.csv: falta columna iata_code"

        records = _records_from_df(df)
        session.execute(insert(Airline), records)
        logger.info("airlines: %d filas cargadas", len(records))

    except AssertionError as e:
        logger.exception("Validación fallida en airlines: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error cargando airlines")
        sys.exit(1)


def load_airports(session: Session, data_dir: str) -> None:
    try:
        df = pd.read_csv(f"{data_dir}/airports.csv")
        df.columns = [c.strip().lower() for c in df.columns]

        assert not df.empty, "airports.csv está vacío"
        assert "iata_code" in df.columns, "airports.csv: falta columna iata_code"

        records = _records_from_df(df)
        session.execute(insert(Airport), records)
        logger.info("airports: %d filas cargadas", len(records))

    except AssertionError as e:
        logger.exception("Validación fallida en airports: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error cargando airports")
        sys.exit(1)


def load_flights(session: Session, data_dir: str) -> None:
    try:
        logger.info("Leyendo primeros 500,000 registros de flights.csv...")
        df = pd.read_csv(f"{data_dir}/flights.csv", nrows=500_000)
        df.columns = [c.strip().lower() for c in df.columns]

        assert not df.empty, "flights.csv está vacío"
        assert "airline" in df.columns, "flights.csv: falta columna airline"

        # Convertir columnas numéricas
        float_cols = [
            "departure_delay", "arrival_delay", "taxi_out", "wheels_off",
            "scheduled_time", "elapsed_time", "air_time", "distance",
            "wheels_on", "taxi_in", "arrival_time", "departure_time",
            "air_system_delay", "security_delay", "airline_delay",
            "late_aircraft_delay", "weather_delay",
        ]
        int_cols = ["year", "month", "day", "day_of_week", "cancelled"]

        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Conservar solo columnas del modelo
        model_cols = [c.name for c in Flight.__table__.columns if c.name != "id"]
        df = df[[c for c in model_cols if c in df.columns]]

        records = _records_from_df(df)
        session.execute(insert(Flight), records)
        logger.info("flights: %d filas cargadas", len(records))

    except AssertionError as e:
        logger.exception("Validación fallida en flights: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Error cargando flights")
        sys.exit(1)


def load_all(engine, data_dir: str) -> None:
    """Carga las tres tablas en orden respetando dependencias FK."""
    try:
        with Session(engine) as session:
            load_airlines(session, data_dir)
            load_airports(session, data_dir)
            load_flights(session, data_dir)
            session.commit()
            logger.info("Commit exitoso — todas las tablas cargadas")
    except Exception:
        logger.exception("Error durante la carga — se hizo rollback")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# VERIFICACIÓN
# ─────────────────────────────────────────────────────────────────────────────

def verify(engine) -> None:
    """Imprime el COUNT(*) de cada tabla para confirmar la carga."""
    try:
        import sqlalchemy as sa
        with engine.connect() as conn:
            for table in ["airlines", "airports", "flights"]:
                result = conn.execute(sa.text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                logger.info("SELECT COUNT(*) FROM %s → %d filas", table, count)
    except Exception:
        logger.exception("Error verificando tablas")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crea el schema de flights en PostgreSQL y carga los datos desde CSV."
    )
    parser.add_argument("--endpoint",   required=True,  help="Endpoint de la instancia primaria RDS")
    parser.add_argument("--secret",     required=False, default="itam/rds/flights/credentials",
                        help="Nombre del secret en Secrets Manager")
    parser.add_argument("--region",     required=False, default="us-east-1",
                        help="Región AWS donde está el secret")
    parser.add_argument("--data-dir",   required=False, default="data/",
                        help="Directorio con airlines.csv, airports.csv y flights.csv")
    args = parser.parse_args()

    creds = get_credentials(args.secret, args.region)
    engine = build_engine(args.endpoint, creds)
    create_schema(engine)
    load_all(engine, args.data_dir)
    verify(engine)


if __name__ == "__main__":
    main()
