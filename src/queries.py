
import sqlite3
from pathlib import Path

DB_FILE = Path("data") / "weather.db"

def run_query(query: str, params: tuple = ()):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows

def print_rows(title: str, rows, limit: int = 50):
    print("\n" + title)
    print("-" * len(title))
    for r in rows[:limit]:
        print(r)
    if len(rows) > limit:
        print(f"... ({len(rows)} filas, mostrando {limit})")

if __name__ == "__main__":

    # 1) Total de registros por ciudad
    q_count_city = """
    SELECT city, COUNT(*) AS total
    FROM weather_observations
    GROUP BY city
    ORDER BY total DESC;
    """
    print_rows("Total de registros por ciudad", run_query(q_count_city))

    # 2) Última lectura por ciudad (patrón MAX + JOIN)
    q_latest = """
    SELECT w.city, w.timestamp_utc, w.temperature_c, w.wind_speed_kmh
    FROM weather_observations w
    JOIN (
        SELECT city, MAX(timestamp_utc) AS max_ts
        FROM weather_observations
        GROUP BY city
    ) m
    ON w.city = m.city AND w.timestamp_utc = m.max_ts
    ORDER BY w.city;
    """
    print_rows("Última lectura por ciudad", run_query(q_latest))

    # 3) Ranking por temperatura de la última lectura (más útil en portfolio)
    q_rank_latest_temp = """
    SELECT w.city, w.timestamp_utc, w.temperature_c
    FROM weather_observations w
    JOIN (
        SELECT city, MAX(timestamp_utc) AS max_ts
        FROM weather_observations
        GROUP BY city
    ) m
    ON w.city = m.city AND w.timestamp_utc = m.max_ts
    WHERE w.temperature_c IS NOT NULL
    ORDER BY w.temperature_c DESC;
    """
    print_rows("Ranking por temperatura (última lectura)", run_query(q_rank_latest_temp))
