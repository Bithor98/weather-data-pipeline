
import json
from pathlib import Path
from datetime import datetime, timezone
import requests

DATA_DIR = Path("data")
CITIES_FILE = DATA_DIR / "cities.json"
RAW_DIR = DATA_DIR / "raw"
BASE_URL = "https://api.open-meteo.com/v1/forecast"

def load_cities():
    with open(CITIES_FILE, encoding="utf-8") as f:
        return json.load(f)

def fetch_weather(lat: float, lon: float):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true",
        "timezone": "UTC",
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def save_raw(city: str, payload: dict):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_city = city.lower().replace(" ", "_")
    out = RAW_DIR / f"{safe_city}_{ts}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out

if __name__ == "__main__":
    cities = load_cities()
    for c in cities:
        payload = fetch_weather(c["latitude"], c["longitude"])
        out = save_raw(c["city"], payload)
        print(f"[OK] {c['city']} -> {out}")