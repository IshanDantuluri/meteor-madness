"""
Fetch original rows from APIs (no synthetic padding) and combine up to target_total rows.
Strategy:
 - NeoWs: use /neo/browse pagination to collect NEOs
 - Fireball: request large limit
 - Meteorite: Socrata pagination using $limit/$offset
 - SBDB: query per NEO designation for orbit params (limited to reasonable count)
Stops when collected target_total unique rows.
Writes `combined_originals.csv` and prints counts per source.
"""
from pathlib import Path
import requests
import csv
import time

ROOT = Path(__file__).resolve().parent
TARGET = 1000
NA = 'N/A'

SCHEMA = [
    'source','id','name','date','latitude','longitude','altitude_m','mass_g','diameter_m',
    'velocity_km_s','miss_distance_km','recclass','orbital_inclination_deg','eccentricity',
    'epoch','x','y','z','vx','vy','vz','energy','impact_e'
]

# Config
NASA_API_KEY = "GjbT7BRsxhQQaJ5kJTYcAk7u0IaRgWAAMaS4dg9y"
NEO_BROWSE_URL = "https://api.nasa.gov/neo/rest/v1/neo/browse"
FIREBALL_URL = "https://ssd-api.jpl.nasa.gov/fireball.api"
METEORITE_URL = "https://data.nasa.gov/resource/gh4g-9sfh.json"
SBDB_URL = "https://ssd-api.jpl.nasa.gov/sbdb.api"

session = requests.Session()
session.headers.update({"User-Agent": "meteor-madness-fetcher/1.0"})

collected = []
seen_keys = set()
counts = {"neo":0, "sbdb":0, "fireball":0, "meteorite":0, "horizons":0}

def add_row(d):
    key = (d.get('source'), d.get('id') or d.get('name') or str(len(collected)))
    if key in seen_keys:
        return False
    seen_keys.add(key)
    collected.append(d)
    counts[d.get('source')] = counts.get(d.get('source'),0) + 1
    return True

# 1) Pull NeoWs browse pages
print('Fetching NEOs via NeoWs browse...')
page = 0
page_size = 20
while len(collected) < TARGET:
    params = {"page": page, "size": page_size, "api_key": NASA_API_KEY}
    try:
        r = session.get(NEO_BROWSE_URL, params=params, timeout=10)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        print('NeoWs browse error:', e)
        break
    neos = j.get('near_earth_objects') or j.get('near_earth_objects', []) or j.get('neo', []) or j.get('near_earth_objects', [])
    if not neos:
        # older response uses 'near_earth_objects'
        neos = j.get('near_earth_objects', [])
    if not neos:
        break
    for neo in neos:
        if len(collected) >= TARGET:
            break
        out = {k: NA for k in SCHEMA}
        out['source'] = 'neo'
        out['id'] = neo.get('neo_reference_id') or neo.get('id') or NA
        out['name'] = neo.get('name') or NA
        # diameter - average of min/max meters if available
        try:
            diam = neo.get('estimated_diameter', {}).get('meters', {})
            mn = diam.get('estimated_diameter_min')
            mx = diam.get('estimated_diameter_max')
            if mn and mx:
                out['diameter_m'] = f"{(mn+mx)/2:.3f}"
        except Exception:
            pass
        # orbital close approach info may be present but not necessarily for all
        cad = neo.get('close_approach_data') or []
        if cad:
            first = cad[0]
            out['date'] = first.get('close_approach_date')
            out['velocity_km_s'] = first.get('relative_velocity', {}).get('kilometers_per_second')
            out['miss_distance_km'] = first.get('miss_distance', {}).get('kilometers')
        add_row(out)
    # move to next page
    page += 1
    # safety: stop if we've reached a large page count
    if page > 200:
        break
    time.sleep(0.2)

# 2) Pull Fireball large batch
if len(collected) < TARGET:
    print('Fetching Fireball records...')
    try:
        # try to fetch up to 5000 then we'll trim
        r = session.get(FIREBALL_URL, params={"limit": 5000}, timeout=15)
        r.raise_for_status()
        j = r.json()
        fields = j.get('fields')
        data = j.get('data') or []
        for row in data:
            if len(collected) >= TARGET:
                break
            # fields may be list describing columns; but our earlier CSV uses names: date,energy,impact-e,lat,lon,alt,vel
            # the API returns 'data' with nested arrays; our local CSV earlier had dict format via fields
            # We'll try to map by index using fields
            mapped = {k: NA for k in SCHEMA}
            mapped['source'] = 'fireball'
            # attempt to map by known field names if present
            # If data is list of lists, j['fields'] gives names
            if isinstance(data[0], list) and fields:
                # find index of known names
                names = [f.get('name') if isinstance(f, dict) else f for f in fields]
                try:
                    idx_date = names.index('date')
                except ValueError:
                    idx_date = None
                # iterate safe
                vals = row
                if idx_date is not None and idx_date < len(vals):
                    mapped['date'] = vals[idx_date]
                # best-effort other fields
                for fname in ['energy','impact-e','lat','lon','alt','vel']:
                    if fname in names:
                        i = names.index(fname)
                        if i < len(vals):
                            v = vals[i]
                            if fname == 'energy': mapped['energy'] = v
                            elif fname == 'impact-e': mapped['impact_e'] = v
                            elif fname == 'lat': mapped['latitude'] = v
                            elif fname == 'lon': mapped['longitude'] = v
                            elif fname == 'alt': mapped['altitude_m'] = v
                            elif fname == 'vel': mapped['velocity_km_s'] = v
                add_row(mapped)
            elif isinstance(row, dict):
                mapped['date'] = row.get('date')
                mapped['energy'] = row.get('energy')
                mapped['impact_e'] = row.get('impact-e')
                mapped['latitude'] = row.get('lat')
                mapped['longitude'] = row.get('lon')
                mapped['altitude_m'] = row.get('alt')
                mapped['velocity_km_s'] = row.get('vel')
                add_row(mapped)
    except Exception as e:
        print('Fireball fetch error:', e)

# 3) Pull Meteorite (Socrata) with pagination
if len(collected) < TARGET:
    print('Fetching Meteorite records from Socrata...')
    offset = 0
    page_size = 500
    while len(collected) < TARGET:
        params = {"$limit": page_size, "$offset": offset}
        try:
            r = session.get(METEORITE_URL, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print('Meteorite fetch error:', e)
            break
        if not data:
            break
        for item in data:
            if len(collected) >= TARGET:
                break
            mapped = {k: NA for k in SCHEMA}
            mapped['source'] = 'meteorite'
            mapped['id'] = item.get('id') or item.get('id')
            mapped['name'] = item.get('name')
            mapped['recclass'] = item.get('recclass')
            mapped['mass_g'] = item.get('mass')
            mapped['date'] = item.get('year')
            mapped['latitude'] = item.get('reclat')
            mapped['longitude'] = item.get('reclong')
            add_row(mapped)
        offset += page_size
        time.sleep(0.2)

# 4) SBDB: query for NEOs we collected earlier (limited)
if len(collected) < TARGET:
    print('Fetching SBDB orbital data for collected NEOs (limited)...')
    # gather distinct neo ids from collected
    neo_ids = [r['id'] for r in collected if r.get('source') == 'neo' and r.get('id')]
    # limit how many SBDB calls we'll make (avoid too many requests)
    max_sbdb = 300
    made = 0
    for nid in neo_ids:
        if len(collected) >= TARGET or made >= max_sbdb:
            break
        try:
            r = session.get(SBDB_URL, params={'sstr': nid}, timeout=10)
            r.raise_for_status()
            j = r.json()
        except Exception as e:
            # skip on errors
            made += 1
            continue
        obj = j.get('object', {})
        orbit = obj.get('orbit', {}) or j.get('orbit', {})
        # safe getter for elements (dict or list)
        def safe_elem_get(elems, key, fallback=NA):
            if isinstance(elems, dict):
                return elems.get(key, fallback)
            if isinstance(elems, list):
                for it in elems:
                    if isinstance(it, dict):
                        if it.get('name') == key:
                            return it.get('value', it.get('val', fallback))
                        if key in it:
                            return it.get(key, fallback)
            return fallback

        if isinstance(orbit, dict):
            elems = orbit.get('elements') or orbit
        else:
            elems = {}
        mapped = {k: NA for k in SCHEMA}
        mapped['source'] = 'sbdb'
        mapped['name'] = obj.get('fullname') or obj.get('designation')
        mapped['epoch'] = safe_elem_get(elems, 'epoch', orbit.get('epoch', NA))
        mapped['eccentricity'] = safe_elem_get(elems, 'e', orbit.get('e', NA))
        mapped['orbital_inclination_deg'] = safe_elem_get(elems, 'i', orbit.get('i', NA))
        add_row(mapped)
        made += 1
        time.sleep(0.1)

# 5) Horizons: (optional) try to pull horizons epochs for Earth across a longer date range if still short
if len(collected) < TARGET:
    print('Fetching Horizons epochs for Earth to fill remaining rows...')
    H_URL = 'https://ssd.jpl.nasa.gov/api/horizons.api'
    # request a range of days and use STEP_SIZE smaller to get more epochs
    start = '2025-01-01'
    stop = '2025-12-31'
    params = {
        'format': 'text', 'COMMAND': '399', 'EPHEM_TYPE': 'VECTORS', 'CENTER': '500@10',
        'START_TIME': start, 'STOP_TIME': stop, 'STEP_SIZE': '1d'
    }
    try:
        r = session.get(H_URL, params=params, timeout=30)
        r.raise_for_status()
        txt = r.text
        # extract $$SOE / $$EOE blocks
        lines = txt.splitlines()
        in_table = False
        current = {}
        for line in lines:
            if line.strip().startswith('$$SOE'):
                in_table = True
                continue
            if line.strip().startswith('$$EOE'):
                break
            if not in_table:
                continue
            line = line.strip()
            if line.startswith('X =') or line.startswith('X='):
                # position line has X= Y= Z=
                try:
                    parts = line.split()
                    # format X = val Y = val Z = val
                    x = parts[2]
                    y = parts[4]
                    z = parts[6]
                except Exception:
                    continue
                mapped = {k: NA for k in SCHEMA}
                mapped['source']='horizons'
                mapped['x']=x; mapped['y']=y; mapped['z']=z
                add_row(mapped)
                if len(collected) >= TARGET:
                    break
        time.sleep(0.1)
    except Exception as e:
        print('Horizons fetch error:', e)

# Done: write combined_originals.csv with collected rows only (no synthetic padding)
out = ROOT / 'combined_originals.csv'
with open(out, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=SCHEMA)
    w.writeheader()
    for r in collected:
        safe = {k: (r.get(k, NA) if isinstance(r, dict) else NA) for k in SCHEMA}
        w.writerow(safe)

print('Final counts by source:')
for k,v in counts.items():
    print(f'  {k}: {v}')
print('Wrote', len(collected), 'rows to', out)

