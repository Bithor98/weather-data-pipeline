
import sqlite3
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
CSV_FILE = DATA_DIR / "weather_clean.csv"
DB_FILE = DATA_DIR / "weather.db"


def create_connection(db_path: Path):
    return sqlite3.connect(db_path)

# Creamos una tabla si no la hay, con una restricción UNIQUE(city, timestamp_utc) para evitar duplicados.
def create_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,
            temperature_c REAL,
            wind_speed_kmh REAL,
            source TEXT,
            UNIQUE(city, timestamp_utc)
        );
    """)
    conn.commit()

# Cargamos incremental: no borramos histórico, inserta y evita duplicados por UNIQUE. Retorna conteo de insertados y ignorados.
def load_data_incremental(conn: sqlite3.Connection, df: pd.DataFrame):
    # Normaliza NaN -> None (para SQLite)
    df = df.where(pd.notnull(df), None)

    # Filtra campos clave (evita NOT NULL errors)
    df = df[df["city"].notna()]
    df = df[df["timestamp_utc"].notna()]
    df = df[df["city"].astype(str).str.strip() != ""]
    df = df[df["timestamp_utc"].astype(str).str.strip() != ""]

    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        cur.execute("""
            INSERT OR IGNORE INTO weather_observations
            (city, timestamp_utc, temperature_c, wind_speed_kmh, source)
            VALUES (?, ?, ?, ?, ?);
        """, (
            row["city"],
            row["timestamp_utc"],
            row["temperature_c"],
            row["wind_speed_kmh"],
            row["source"],
        ))

        # rowcount: 1 si insertó, 0 si ignoró por UNIQUE
        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    return inserted, skipped


def count_rows(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM weather_observations;")
    return cur.fetchone()[0]

# Hacemos flujo completo: leer CSV, conectar, crear tabla, cargar incremental, imprimir resultados. Si no hay CSV, error claro.
if __name__ == "__main__":
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"No existe el CSV: {CSV_FILE}. Ejecuta transform.py primero.")

    df = pd.read_csv(CSV_FILE)

    conn = create_connection(DB_FILE)
    create_table(conn)

    inserted, skipped = load_data_incremental(conn, df)
    total = count_rows(conn)

    conn.close()

    print(f"Insertadas: {inserted}")
    print(f"Ignoradas por duplicado: {skipped}")
    print(f"Total en BD: {total}")