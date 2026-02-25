
import json
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
OUT_CSV = DATA_DIR / "weather_clean.csv"

# Con parse_raw_file consigo extraes los datos relevantes del JSON para devolverlos en datos analizables y estructurados.
def parse_raw_file(path: Path) -> dict:
    # ciudad = parte del nombre del archivo antes del primer "_"
    safe_city = path.stem.split("_")[0]  # madrid, barcelona...
    city = safe_city.replace("-", " ").replace("__", "_")  # por si acaso

    with open(path, encoding="utf-8") as f:
        payload = json.load(f)

    current = payload.get("current_weather", {})

    return {
        "city": safe_city,  # guardamos en formato consistente: madrid, barcelona...
        "timestamp_utc": current.get("time"),
        "temperature_c": current.get("temperature"),
        "wind_speed_kmh": current.get("windspeed"),
        "source": "open-meteo"
    }

# Permite que se pueda ejecutar repetidamente sin elimitar datos anteriores, pudiendo comparar los datos nuevos con los anteriores.
def load_existing() -> pd.DataFrame:
    if OUT_CSV.exists():
        return pd.read_csv(OUT_CSV)
    return pd.DataFrame()

def transform():
    rows = []
    for file in RAW_DIR.glob("*.json"):
        rows.append(parse_raw_file(file))

    df_new = pd.DataFrame(rows)

    df_existing = load_existing()
    df_all = pd.concat([df_existing, df_new], ignore_index=True)

    #Evita duplicar datos duplicados siempre que se hagan ejecuciones repetidas del pipeline, dejando solamente una fila de cada dato.
    df_all.drop_duplicates(subset=["city", "timestamp_utc"], inplace=True)

    df_all.to_csv(OUT_CSV, index=False)
    return len(df_new), len(df_all)

# Muestra el resultado final, despues de ejecuta, procesar el JSON y actualizar el CSV limpio para mostrar el resultado en la consola.
if __name__ == "__main__":
    new_rows, total_rows = transform()
    print(f"Nuevas filas procesadas: {new_rows}")
    print(f"Total filas en CSV: {total_rows}")