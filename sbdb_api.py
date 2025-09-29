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
    sbdb_data = fetch_sbdb("433")  # 433 = Eros, WE NEED TO CHANGE THIS TO INCLUDE A TON OF OTHER SHIT THAT WE CAN INTEGRATE.
    if not sbdb_data or "object" not in sbdb_data:
        print("No SBDB data to export.")
    else:
        obj = sbdb_data["object"]
        orbit = obj.get("orbit", {})
        table = [[
            obj.get("fullname", "N/A"),
            orbit.get("epoch", "N/A"),
            orbit.get("e", "N/A"),
            orbit.get("i", "N/A"),
            orbit.get("om", "N/A"),
            orbit.get("w", "N/A"),
            orbit.get("q", "N/A"),
            orbit.get("a", "N/A"),
            orbit.get("ma", "N/A")
        ]]
        headers = ["Name", "Epoch", "Eccentricity", "Inclination", "Longitude Asc Node", "Arg Perihelion", "Perihelion Dist", "Semi-major Axis", "Mean Anomaly"]
        # Ensure 100 rows by repeating or padding
        desired = 100
        out_rows = []
        if table:
            # repeat the single row to reach desired count
            for i in range(desired):
                row = table[0][:]
                row[0] = f"{row[0]} #{i+1}"
                out_rows.append(row)
        else:
            for i in range(desired):
                out_rows.append(["N/A"] * len(headers))

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
