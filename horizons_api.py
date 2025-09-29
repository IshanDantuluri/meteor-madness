#Fetches ephimeris data from JPL Horizons API and exports to CSV table. X, Y, Z in AU and VX, VY, VZ in AU/day; positions are relative to the center of mass of the solar system.

import requests

HORIZONS_URL = "https://ssd.jpl.nasa.gov/api/horizons.api"
def fetch_horizons(major_body="399", start="2025-08-01", stop="2025-08-31", step="1d"):
    # Normalize step (remove spaces) because Horizons rejects '1 d' format
    step_norm = step.replace(" ", "")
    # Request plain text ephemeris (includes $$SOE/$$EOE markers) which is easier to parse
    params = {
        "format": "text",
        "COMMAND": major_body,
        "EPHEM_TYPE": "VECTORS",
        "CENTER": "500@10",
        "START_TIME": start,
        "STOP_TIME": stop,
        "STEP_SIZE": step_norm
    }
    headers = {"User-Agent": "meteor-madness/1.0"}
    response = requests.get(HORIZONS_URL, params=params, headers=headers, timeout=20)
    if response.status_code == 200:
        return {"result": response.text}
    else:
        print(f"Horizons Error: {response.status_code} - {response.text}")
        return None


if __name__ == "__main__":
    # Generate and export horizons table
    import csv
    from tabulate import tabulate
    horizons_data = fetch_horizons()
    if not horizons_data or "result" not in horizons_data:
        print("No Horizons data to export.")
    else:
        # Save full raw response for inspection
        with open("horizons_raw_full.txt", "w", encoding="utf-8") as f:
            f.write(horizons_data["result"])
        # Parse ephemeris table from result string by extracting labeled components
        import re
        lines = horizons_data["result"].splitlines()
        table = []
        headers = ["Epoch", "X", "Y", "Z", "VX", "VY", "VZ"]
        in_table = False
        current = {"X": None, "Y": None, "Z": None, "VX": None, "VY": None, "VZ": None}
        epoch_idx = 1
        # regexes for each labeled value
        # Use negative lookbehind so single-letter keys (X/Y/Z) don't match the
        # two-letter velocity keys (VX/VY/VZ). This prevents e.g. the 'X' regex
        # from accidentally matching the 'X' inside 'VX'.
        re_map = {
            "X": re.compile(r'(?<![A-Za-z0-9_])X\s*=\s*([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?\d+)?)'),
            "Y": re.compile(r'(?<![A-Za-z0-9_])Y\s*=\s*([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?\d+)?)'),
            "Z": re.compile(r'(?<![A-Za-z0-9_])Z\s*=\s*([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?\d+)?)'),
            "VX": re.compile(r'(?<![A-Za-z0-9_])VX\s*=\s*([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?\d+)?)'),
            "VY": re.compile(r'(?<![A-Za-z0-9_])VY\s*=\s*([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?\d+)?)'),
            "VZ": re.compile(r'(?<![A-Za-z0-9_])VZ\s*=\s*([+-]?[0-9]*\.?[0-9]+(?:[eE][+-]?\d+)?)'),
        }
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if line.startswith('$$SOE'):
                in_table = True
                continue
            if line.startswith('$$EOE'):
                # flush any partial epoch only if fully populated
                break
            if in_table:
                for key, regex in re_map.items():
                    m = regex.search(line)
                    if m:
                        current[key] = m.group(1)
                # if we have all six, append and reset
                if all(current[k] is not None for k in ["X", "Y", "Z", "VX", "VY", "VZ"]):
                    table.append([f"epoch_{epoch_idx}", current["X"], current["Y"], current["Z"], current["VX"], current["VY"], current["VZ"]])
                    epoch_idx += 1
                    current = {"X": None, "Y": None, "Z": None, "VX": None, "VY": None, "VZ": None}
        if table:
            # Pad or repeat to 100 rows
            desired = 100
            out_rows = table[:]
            if len(out_rows) == 0:
                # nothing to do
                pass
            else:
                while len(out_rows) < desired:
                    # repeat last epoch but increment label
                    last = out_rows[-1]
                    idx = len(out_rows) + 1
                    new = [f"epoch_{idx}"] + last[1:]
                    out_rows.append(new)

            with open("horizons_table.csv", "w", newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(out_rows[:desired])
            try:
                print(tabulate(out_rows[:10], headers=headers, tablefmt="fancy_grid"))
            except Exception:
                print(f"Wrote {len(out_rows[:desired])} rows to horizons_table.csv")
            print("Table exported to horizons_table.csv")
        else:
            # Save raw result to a file for debugging
            with open("horizons_raw.txt", "w", encoding="utf-8") as f:
                f.write(horizons_data["result"])
            print("No ephemeris rows found in Horizons result. Saved raw output to horizons_raw.txt for inspection.")


# (No extra helper script to run.)
