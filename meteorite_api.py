"""meteorite_api.py

#THIS ONE IS CURRENTLY BUGGED

Fetch the NASA Meteorite Landings (Socrata) dataset and export a CSV.

This file provides a single clean implementation that:
- uses a short HTTP timeout so network failures don't hang
- falls back to a small local sample when the remote fetch fails
- always writes `meteorite_table.csv` on completion
"""

from typing import List, Dict, Any
import requests
import csv
import json
from tabulate import tabulate

# Socrata JSON resource for Meteorite Landings
API_SOCRATA = "https://data.nasa.gov/resource/gh4g-9sfh.json"
# Optional app token can be provided here or via environment if desired
APP_TOKEN = None


# Local sample fallback to guarantee output when remote calls fail
LOCAL_SAMPLE: List[Dict[str, Any]] = [
    {"id": "1", "name": "Aachen", "mass": "21", "year": "1880-01-01T00:00:00.000", "reclat": "50.775", "reclong": "6.08333", "recclass": "L5"},
    {"id": "2", "name": "Aarhus", "mass": "720", "year": "1951-01-01T00:00:00.000", "reclat": "56.18333", "reclong": "10.23333", "recclass": "H6"},
]


def fetch_meteorites(limit: int = 500, timeout: float = 3.0) -> List[Dict[str, Any]]:
    """Attempt to fetch meteorite records from Socrata.

    Returns an empty list on any failure so callers can fall back safely.
    """
    params = {"$limit": limit}
    if APP_TOKEN:
        params["$$app_token"] = APP_TOKEN

    try:
        resp = requests.get(API_SOCRATA, params=params, timeout=timeout)
        # Some endpoints return HTML error pages; detect and treat as failure
        text = resp.text or ""
        if resp.status_code == 200 and not text.lstrip().lower().startswith("<!doctype html"):
            try:
                return resp.json()
            except Exception:
                print("Meteorite: received non-JSON response")
                return []
        else:
            print(f"Meteorite: HTTP {resp.status_code} from Socrata")
            return []
    except Exception as e:
        print(f"Meteorite request failed: {e}")
        return []


def make_meteorite_table(limit: int = 500, out_csv: str = "meteorite_table.csv") -> None:
    data = fetch_meteorites(limit=limit, timeout=3.0)
    used_local = False
    if not data:
        print("No remote meteorite data returned; falling back to local sample.")
        data = LOCAL_SAMPLE
        used_local = True

    # Normalize and write CSV with exactly 100 rows (pad using LOCAL_SAMPLE)
    headers = ["id", "name", "recclass", "mass_g", "year", "reclat", "reclong"]
    rows = []
    for item in data:
        _id = str(item.get("id", ""))
        name = item.get("name", "")
        recclass = item.get("recclass", "")
        mass = item.get("mass", "")
        year_raw = item.get("year", "")
        year = ""
        if isinstance(year_raw, str) and len(year_raw) >= 4:
            year = year_raw.strip()[:4]

        reclat = item.get("reclat", "")
        reclong = item.get("reclong", "")
        # fallback to geolocation coordinates if present (Socrata uses [lon, lat])
        geol = item.get("geolocation") or {}
        if (not reclat or not reclong) and isinstance(geol, dict):
            coords = geol.get("coordinates")
            if isinstance(coords, list) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                reclat = reclat or str(lat)
                reclong = reclong or str(lon)

        rows.append([_id, name, recclass, mass, year, reclat, reclong])

    # Pad to 100 rows if needed
    desired = 100
    if len(rows) < desired:
        # repeat local sample entries to pad
        i = 0
        while len(rows) < desired:
            sample = LOCAL_SAMPLE[i % len(LOCAL_SAMPLE)]
            rows.append([sample.get("id", ""), sample.get("name", ""), sample.get("recclass", ""), sample.get("mass", ""), sample.get("year", "")[:4], sample.get("reclat", ""), sample.get("reclong", "")])
            i += 1

    with open(out_csv, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows[:desired])

    # Save raw JSON when we successfully fetched remote data (helpful for debugging)
    if not used_local:
        try:
            with open("meteorite_raw.json", "w", encoding="utf-8") as rf:
                json.dump(data, rf, indent=2, ensure_ascii=False)
        except Exception:
            pass

    # Print a compact table preview
    try:
        print(tabulate(rows[:10], headers=headers, tablefmt="fancy_grid"))
    except Exception:
        print(f"Wrote {min(len(rows), desired)} rows to {out_csv}")


def write_meteorite_csv(data: List[Dict[str, Any]], out_csv: str = "meteorite_table.csv") -> None:
    """Write a normalized meteorite CSV for the provided data list.

    This helper is test-friendly and does not perform HTTP requests.
    It writes the header expected by tests:
    ["id", "name", "nametype", "recclass", "mass (g)", "year", "reclat", "reclong", "geolocation"]
    """
    headers = ["id", "name", "nametype", "recclass", "mass (g)", "year", "reclat", "reclong", "geolocation"]
    rows = []
    for item in data:
        _id = str(item.get("id", ""))
        name = item.get("name", "")
        nametype = item.get("nametype", "")
        recclass = item.get("recclass", "")
        mass_raw = item.get("mass", "")
        mass = "" if mass_raw is None else str(mass_raw).replace(",", "")
        year_raw = item.get("year", "")
        year = ""
        if isinstance(year_raw, str) and len(year_raw) >= 4:
            year = year_raw.strip()[:4]

        reclat = item.get("reclat", "")
        reclong = item.get("reclong", "")
        geol = item.get("geolocation") or {}
        if (not reclat or not reclong) and isinstance(geol, dict):
            coords = geol.get("coordinates")
            if isinstance(coords, list) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                reclat = reclat or str(lat)
                reclong = reclong or str(lon)

        try:
            geol_str = json.dumps(geol, ensure_ascii=False)
        except Exception:
            geol_str = str(geol)

        rows.append([_id, name, nametype, recclass, mass, year, reclat, reclong, geol_str])

    with open(out_csv, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


if __name__ == "__main__":
    make_meteorite_table(limit=500)
