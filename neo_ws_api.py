#Fetches Near Earth Object data from NASA's Near Earth Object Web Service (NeoWs) API and exports to CSV table.

import requests

API_KEY = "GjbT7BRsxhQQaJ5kJTYcAk7u0IaRgWAAMaS4dg9y"  # Replace with your NASA API key for production use
BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"

def fetch_neo_data(start_date, end_date):
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "api_key": API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None


if __name__ == "__main__":
    # When run directly, generate and export the NEO table
    from datetime import date
    from tabulate import tabulate
    import csv
    today = date.today().isoformat()
    data = fetch_neo_data(today, today)
    table = []
    headers = [
        "#",
        "Name",
        "Date",
        "Diameter (m)",
        "Velocity (km/s)",
        "Miss Dist. (km)",
        "Orbiting Body",
        "Inclination (deg)",
        "Eccentricity",
        "Hazardous"
    ]
    neos = data.get("near_earth_objects", {}).get(today, [])
    idx = 1
    for neo in neos:
        name = neo.get("name")
        diameter = neo.get("estimated_diameter", {}).get("meters", {})
        min_d = diameter.get("min")
        max_d = diameter.get("max")
        if min_d is not None and max_d is not None:
            diameter_avg = (min_d + max_d) / 2
            diameter_str = f"{diameter_avg:,.1f}"
        else:
            diameter_str = "N/A"
        hazardous = "!DANGER!" if neo.get("is_potentially_hazardous_asteroid") else "FINE"
        orbital_data = neo.get("orbital_data", {})
        inclination = orbital_data.get("inclination", "N/A")
        eccentricity = orbital_data.get("eccentricity", "N/A")
        for approach in neo.get("close_approach_data", []):
            approach_date = approach.get("close_approach_date")
            velocity = float(approach.get("relative_velocity", {}).get("kilometers_per_second", 0))
            miss_distance = float(approach.get("miss_distance", {}).get("kilometers", 0))
            orbiting_body = approach.get("orbiting_body", "N/A")
            table.append([
                idx,
                name,
                approach_date,
                diameter_str,
                f"{velocity:,.2f}",
                f"{miss_distance:,.0f}",
                orbiting_body,
                inclination,
                eccentricity,
                hazardous
            ])
            idx += 1
    # Ensure 100 rows: pad with blank rows if needed
    desired = 100
    while len(table) < desired:
        table.append([len(table)+1, "", "", "N/A", "", "", "", "", "", ""])

    # Save table to CSV
    with open("neo_table.csv", "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(table)
    try:
        print(tabulate(table[:50], headers=headers, tablefmt="fancy_grid"))
    except Exception:
        print(f"Wrote {len(table)} rows to neo_table.csv")
    print("Table exported to neo_table.csv")
