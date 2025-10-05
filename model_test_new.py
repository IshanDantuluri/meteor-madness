import math
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# =====================================================
# 🌍 ADVANCED IMPACT MODEL — Holsapple–Housen (1993)
# =====================================================
def simulate_meteor_impact(diameter_m, velocity_km_s, density_kg_m3=3000, target_density_kg_m3=2700, gravity=9.81):
    """
    Advanced meteor impact simulator:
    - Includes atmospheric breakup and airburst detection
    - Uses Holsapple–Housen crater scaling
    - Estimates damage radius and effects
    """
    radius_m = diameter_m / 2
    velocity_m_s = velocity_km_s * 1000
    mass_kg = (4/3) * math.pi * radius_m**3 * density_kg_m3
    energy_j = 0.5 * mass_kg * velocity_m_s**2
    energy_mt = energy_j / 4.184e15  # Joules → megatons TNT

    # ------------------------
    # Atmospheric entry effects
    # ------------------------
    atmosphere_density = 1.2  # kg/m³
    dynamic_pressure = 0.5 * atmosphere_density * velocity_m_s**2  # N/m²

    # Simplified breakup threshold (Pa)
    breakup_strength = 1e7  # 10 MPa typical stony asteroid
    will_airburst = dynamic_pressure > breakup_strength and diameter_m < 60

    if will_airburst:
        crater_diameter_m = 0
        damage_radius_km = (energy_mt ** 0.33) * 2  # shockwave radius
        damage_description = "Airburst – no crater, strong blast wave"
    else:
        # Holsapple–Housen gravity scaling
        crater_diameter_m = 1.161 * ((gravity**(-0.22)) *
                                    (velocity_m_s**0.44) *
                                    ((mass_kg / target_density_kg_m3)**0.333))
        damage_radius_km = crater_diameter_m / 500
        damage_description = "Ground impact – crater formation"

    # ------------------------
    # Estimate damage severity
    # ------------------------
    if energy_mt < 0.1:
        damage_level = "Minor flash / negligible"
    elif energy_mt < 10:
        damage_level = "Local damage (few km)"
    elif energy_mt < 1000:
        damage_level = "Regional devastation"
    else:
        damage_level = "Global catastrophic impact"

    return {
        "energy_mt": energy_mt,
        "crater_diameter_m": crater_diameter_m,
        "damage_radius_km": damage_radius_km,
        "damage_level": damage_level,
        "airburst": will_airburst,
        "damage_description": damage_description
    }


# =====================================================
# 🧪 MODEL TESTING AGAINST REAL EVENTS
# =====================================================
def test_model():
    test_events = [
        {"name": "Barringer Crater", "diameter_m": 50, "velocity_km_s": 12.8, "observed_crater_m": 1200},
        {"name": "Tunguska Event", "diameter_m": 50, "velocity_km_s": 17.0, "observed_crater_m": 0},
        {"name": "Chelyabinsk", "diameter_m": 20, "velocity_km_s": 19.0, "observed_crater_m": 0},
        {"name": "Chicxulub", "diameter_m": 10000, "velocity_km_s": 20.0, "observed_crater_m": 180000},
        {"name": "Meteor Crater Small Test", "diameter_m": 100, "velocity_km_s": 20.0, "observed_crater_m": 2000},
    ]

    predicted = []
    observed = []

    print("\n🧪 ADVANCED MODEL VALIDATION 🧪")
    print("---------------------------------------------------")

    for e in test_events:
        result = simulate_meteor_impact(e["diameter_m"], e["velocity_km_s"])
        predicted.append(result["crater_diameter_m"])
        observed.append(e["observed_crater_m"])

        print(f"{e['name']:<28} | D={e['diameter_m']:>5.0f} m | V={e['velocity_km_s']:>5.1f} km/s | "
              f"Pred Crater={result['crater_diameter_m']:>9.1f} m | Obs={e['observed_crater_m']:>6} m | "
              f"{result['damage_level']}")

    mae = mean_absolute_error(observed, predicted)
    rmse = math.sqrt(mean_squared_error(observed, predicted))
    r2 = r2_score(observed, predicted)

    print("---------------------------------------------------")
    print(f"Mean Absolute Error (MAE):   {mae:,.2f} m")
    print(f"Root Mean Square Error (RMSE): {rmse:,.2f} m")
    print(f"R² Score: {r2:.3f}")
    print("---------------------------------------------------\n")


# =====================================================
# 🧮 INTERACTIVE SIMULATION MODE
# =====================================================
def interactive_simulator():
    print("\n🌠 METEOR IMPACT SIMULATOR 🌠")
    print("---------------------------------------------------")

    try:
        diameter = float(input("Enter meteor diameter (m): "))
        velocity = float(input("Enter impact velocity (km/s): "))
        density = float(input("Enter meteor density (kg/m³, typical=3000): ") or 3000)
    except ValueError:
        print("⚠️ Invalid input. Please enter numeric values.")
        return

    result = simulate_meteor_impact(diameter, velocity, density)

    print("\n🧾 IMPACT RESULTS")
    print("---------------------------------------------------")
    print(f"Meteor Diameter:     {diameter:,.1f} m")
    print(f"Impact Velocity:     {velocity:,.1f} km/s")
    print(f"Meteor Density:      {density:,.0f} kg/m³")
    print(f"Impact Energy:       {result['energy_mt']:,.2f} Mt TNT")
    print(f"Crater Diameter:     {result['crater_diameter_m']:,.1f} m")
    print(f"Damage Radius:       {result['damage_radius_km']:,.2f} km")
    print(f"Damage Level:        {result['damage_level']}")
    print(f"Event Type:          {result['damage_description']}")
    print("---------------------------------------------------\n")


# =====================================================
# 🚀 MAIN EXECUTION
# =====================================================
if __name__ == "__main__":
    test_model()            # Run scientific validation
    interactive_simulator() # Allow user to test scenarios interactively

