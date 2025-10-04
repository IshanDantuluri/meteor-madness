#Fetches Small-Body Database (SBDB) data from JPL SBDB API and exports to CSV table.

import requests

SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"
def fetch_sbdb(asteroid_id):
    params = {"sstr": asteroid_id}
    response = requests.get(SBDB_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"SBDB Error: {response.status_code} - {response.text}")
        return None


if __name__ == "__main__":
    # Generate and export SBDB table
    import csv
    from tabulate import tabulate
    # List of first 100 numbered asteroids (as strings)
    asteroid_ids = [str(i) for i in range(1, 101)]
    headers = ["Name", "Epoch", "Eccentricity", "Inclination", "Longitude Asc Node", "Arg Perihelion", "Perihelion Dist", "Semi-major Axis", "Mean Anomaly"]
    out_rows = []
    for aid in asteroid_ids:
        sbdb_data = fetch_sbdb(aid)
        if not sbdb_data or "object" not in sbdb_data:
            out_rows.append([f"N/A ({aid})"] + ["N/A"] * (len(headers)-1))
            continue
        obj = sbdb_data["object"]
        orbit = obj.get("orbit", {})
        row = [
            obj.get("fullname", f"N/A ({aid})"),
            orbit.get("epoch", "N/A"),
            orbit.get("e", "N/A"),
            orbit.get("i", "N/A"),
            orbit.get("om", "N/A"),
            orbit.get("w", "N/A"),
            orbit.get("q", "N/A"),
            orbit.get("a", "N/A"),
            orbit.get("ma", "N/A")
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


# (No extra helper script to run.)
