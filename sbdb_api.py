#Fetches Small-Body Database (SBDB) data from JPL SBDB API and exports to CSV table.

import requests

SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"
def fetch_sbdb(asteroid_id):
    params = {"sstr": asteroid_id}
    try:
        response = requests.get(SBDB_URL, params=params, timeout=10, headers={'User-Agent':'meteor-madness/1.0'})
    except Exception as e:
        print(f"SBDB request error for {asteroid_id}: {e}")
        return None
    if response.status_code == 200:
        return response.json()
    else:
        print(f"SBDB Error: {response.status_code} - {response.text}")
        return None


if __name__ == "__main__":
    # Generate and export SBDB table
    import csv
    from tabulate import tabulate
    # helper: safely extract a value from elements which can be dict or list
    def safe_elem_get(elems, key, fallback="N/A"):
        if isinstance(elems, dict):
            return elems.get(key, fallback)
        if isinstance(elems, list):
            # try to find dict entries with matching name or containing the key
            for it in elems:
                if isinstance(it, dict):
                    # common SBDB element formats: {'name': 'e', 'value': 0.1} or {'e': 0.1}
                    if it.get('name') == key:
                        return it.get('value', it.get('val', fallback))
                    if key in it:
                        return it.get(key, fallback)
            return fallback
        return fallback
    # List of first 100 numbered asteroids (as strings)
    asteroid_ids = [str(i) for i in range(1, 101)]
    headers = ["Name", "Epoch", "Eccentricity", "Inclination", "Longitude Asc Node", "Arg Perihelion", "Perihelion Dist", "Semi-major Axis", "Mean Anomaly"]
    out_rows = []
    for aid in asteroid_ids:
        sbdb_data = fetch_sbdb(aid)
        if not sbdb_data or "object" not in sbdb_data:
            out_rows.append([f"N/A ({aid})"] + ["N/A"] * (len(headers)-1))
            continue
        obj = sbdb_data.get("object", {})
        orbit = obj.get("orbit") or sbdb_data.get('orbit') or {}
        if isinstance(orbit, dict):
            elems = orbit.get('elements') or orbit
        else:
            elems = {}
        row = [
            obj.get("fullname", f"N/A ({aid})"),
            safe_elem_get(elems, "epoch", orbit.get("epoch", "N/A")),
            safe_elem_get(elems, "e", orbit.get("e", "N/A")),
            safe_elem_get(elems, "i", orbit.get("i", "N/A")),
            safe_elem_get(elems, "om", orbit.get("om", "N/A")),
            safe_elem_get(elems, "w", orbit.get("w", "N/A")),
            safe_elem_get(elems, "q", orbit.get("q", "N/A")),
            safe_elem_get(elems, "a", orbit.get("a", "N/A")),
            safe_elem_get(elems, "ma", orbit.get("ma", "N/A"))
        ]
        out_rows.append(row)

    with open("sbdb_table.csv", "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(out_rows)
    try:
        print(tabulate(out_rows[:10], headers=headers, tablefmt="fancy_grid"))
    except Exception:
        print(f"Wrote {len(out_rows)} rows to sbdb_table.csv")
    print("Table exported to sbdb_table.csv")


# (No extra helper script to run.).  
