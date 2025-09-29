#Fetches fireball data from JPL Fireball API and exports to CSV table. Fireball provides data on bright meteors detected entering Earth's atmosphere.

import requests

FIREBALL_URL = "https://ssd-api.jpl.nasa.gov/fireball.api"
def fetch_fireballs(limit=100):
    params = {"limit": limit}
    response = requests.get(FIREBALL_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fireball Error: {response.status_code} - {response.text}")
        return None


if __name__ == "__main__":
    # Generate and export fireball table
    import csv
    from tabulate import tabulate
    fireball_data = fetch_fireballs()
    if not fireball_data or "data" not in fireball_data:
        print("No fireball data to export.")
    else:
        headers = fireball_data.get("fields", [])
        table = fireball_data.get("data", [])
        # Ensure 100 rows
        desired = 100
        out_rows = table[:]
        while len(out_rows) < desired:
            out_rows.append([""] * len(headers))

        with open("fireball_table.csv", "w", newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerows(out_rows)
        try:
            print(tabulate(out_rows[:20], headers=headers, tablefmt="fancy_grid"))
        except Exception:
            print(f"Wrote {len(out_rows)} rows to fireball_table.csv")
        print("Table exported to fireball_table.csv")


# (No extra helper script to run.)
