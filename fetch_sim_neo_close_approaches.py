import requests
import csv
from datetime import datetime, timedelta
import math

# -----------------------------
# CONFIGURATION
# -----------------------------
sim_neo_days_back = 30          # Number of days back to fetch
sim_neo_end_date = datetime.today()
sim_neo_start_date = sim_neo_end_date - timedelta(days=sim_neo_days_back)
sim_neo_output_csv = "neo_impact_ready.csv"
sim_neo_api_key = "GjbT7BRsxhQQaJ5kJTYcAk7u0IaRgWAAMaS4dg9y"    # Replace with your NASA API key
sim_neo_chunk_days = 7          # Max 7 days per request
sim_neo_max_miss_distance_km = 1000000
sim_neo_density_kg_m3 = 3000   # meteor density
sim_neo_target_density_kg_m3 = 2700  # Earth surface density

# -----------------------------
# FETCH NEO DATA FOR A DATE RANGE
# -----------------------------
def sim_neo_fetch_chunk(start_date, end_date):
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "api_key": sim_neo_api_key
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error fetching NEO data ({start_date} → {end_date}): {response.status_code}")
        return {}
    return response.json()

# -----------------------------
# CALCULATE IMPACT PROPERTIES
# -----------------------------
def sim_neo_calculate_properties(diameter_m, velocity_km_s):
    radius_m = diameter_m / 2
    mass_kg = (4/3) * math.pi * radius_m**3 * sim_neo_density_kg_m3
    velocity_m_s = velocity_km_s * 1000
    kinetic_energy_j = 0.5 * mass_kg * velocity_m_s**2
    crater_diameter_m = 20 * (diameter_m ** 0.78) * (velocity_km_s ** 0.44)
    # Impact Severity Index (normalized)
    impact_severity_index = kinetic_energy_j / 1e15 + crater_diameter_m / 1000
    return mass_kg, kinetic_energy_j, crater_diameter_m, impact_severity_index

# -----------------------------
# MAIN PROCESS
# -----------------------------
def sim_neo_main():
    print(f"Fetching NEO data from {sim_neo_start_date.strftime('%Y-%m-%d')} to {sim_neo_end_date.strftime('%Y-%m-%d')}...")
    sim_neo_list = []

    sim_neo_current_start = sim_neo_start_date
    while sim_neo_current_start < sim_neo_end_date:
        sim_neo_current_end = min(sim_neo_current_start + timedelta(days=sim_neo_chunk_days-1), sim_neo_end_date)
        sim_neo_data = sim_neo_fetch_chunk(sim_neo_current_start, sim_neo_current_end)

        for sim_neo_date_str, sim_neo_day_list in sim_neo_data.get("near_earth_objects", {}).items():
            for sim_neo_item in sim_neo_day_list:
                sim_neo_close_approach = sim_neo_item.get("close_approach_data", [])
                if sim_neo_close_approach:
                    sim_neo_approach = sim_neo_close_approach[0]
                    sim_neo_miss_distance_km = float(sim_neo_approach.get("miss_distance", {}).get("kilometers", "inf"))

                    if sim_neo_miss_distance_km <= sim_neo_max_miss_distance_km:
                        sim_neo_name = sim_neo_item.get("name")
                        sim_neo_est_diameter = sim_neo_item.get("estimated_diameter", {}).get("meters", {})
                        sim_neo_diameter_min = sim_neo_est_diameter.get("estimated_diameter_min", 0)
                        sim_neo_diameter_max = sim_neo_est_diameter.get("estimated_diameter_max", 0)
                        sim_neo_diameter_avg = (sim_neo_diameter_min + sim_neo_diameter_max) / 2
                        sim_neo_velocity = float(sim_neo_approach.get("relative_velocity", {}).get("kilometers_per_second", 0))
                        sim_neo_orbiting_body = sim_neo_approach.get("orbiting_body", "")
                        sim_neo_orbital_data = sim_neo_item.get("orbital_data", {})
                        sim_neo_inclination = sim_neo_orbital_data.get("inclination", "")
                        sim_neo_eccentricity = sim_neo_orbital_data.get("eccentricity", "")
                        sim_neo_hazardous = sim_neo_item.get("is_potentially_hazardous_asteroid", False)

                        sim_neo_mass_kg, sim_neo_KE_j, sim_neo_crater_m, sim_neo_ISI = sim_neo_calculate_properties(sim_neo_diameter_avg, sim_neo_velocity)

                        sim_neo_list.append({
                            "sim_neo_Name": sim_neo_name,
                            "sim_neo_Date": sim_neo_date_str,
                            "sim_neo_Diameter_min_m": sim_neo_diameter_min,
                            "sim_neo_Diameter_max_m": sim_neo_diameter_max,
                            "sim_neo_Diameter_avg_m": sim_neo_diameter_avg,
                            "sim_neo_Velocity_km_s": sim_neo_velocity,
                            "sim_neo_Mass_kg": sim_neo_mass_kg,
                            "sim_neo_Kinetic_Energy_J": sim_neo_KE_j,
                            "sim_neo_Crater_Diameter_m": sim_neo_crater_m,
                            "sim_neo_Impact_Severity_Index": sim_neo_ISI,
                            "sim_neo_Miss_Distance_km": sim_neo_miss_distance_km,
                            "sim_neo_Orbiting_Body": sim_neo_orbiting_body,
                            "sim_neo_Inclination_deg": sim_neo_inclination,
                            "sim_neo_Eccentricity": sim_neo_eccentricity,
                            "sim_neo_Hazardous": sim_neo_hazardous
                        })

        sim_neo_current_start = sim_neo_current_end + timedelta(days=1)

    if sim_neo_list:
        print(f"Writing {len(sim_neo_list)} NEOs to {sim_neo_output_csv}...")
        sim_neo_headers = ["sim_neo_Name","sim_neo_Date","sim_neo_Diameter_min_m","sim_neo_Diameter_max_m",
                           "sim_neo_Diameter_avg_m","sim_neo_Velocity_km_s","sim_neo_Mass_kg","sim_neo_Kinetic_Energy_J",
                           "sim_neo_Crater_Diameter_m","sim_neo_Impact_Severity_Index","sim_neo_Miss_Distance_km",
                           "sim_neo_Orbiting_Body","sim_neo_Inclination_deg","sim_neo_Eccentricity","sim_neo_Hazardous"]
        with open(sim_neo_output_csv, mode='w', newline='', encoding='utf-8') as sim_neo_f:
            sim_neo_writer = csv.DictWriter(sim_neo_f, fieldnames=sim_neo_headers)
            sim_neo_writer.writeheader()
            for sim_neo_row in sim_neo_list:
                sim_neo_writer.writerow(sim_neo_row)
        print("Done!")
    else:
        print("No close-approach NEOs found in this period.")

# -----------------------------
# RUN SCRIPT
# -----------------------------
if __name__ == "__main__":
    sim_neo_main()
