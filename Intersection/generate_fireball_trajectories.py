#!/usr/bin/env python3
"""
Normalize local Fireball data and emit event files and a GeoJSON for mapping.

Outputs:
 - Intersection/fireball_events.csv (cleaned table)
 - Intersection/fireball_events.geojson (Point features: lat/lon + properties)
 - Intersection/trajectories/fireball_<index>.json (per-event metadata)

This uses only local file `fireball_table.csv` found in the repo root.
"""
from pathlib import Path
import csv, json
from datetime import datetime

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
SRC = REPO_ROOT / 'fireball_table.csv'
OUT_CSV = ROOT / 'fireball_events.csv'
OUT_GEO = ROOT / 'fireball_events.geojson'
TRAJ_DIR = ROOT / 'trajectories'

def parse_float(s):
    try:
        return None if s is None or s == '' else float(str(s).strip())
    except Exception:
        return None

def normalize_row(row):
    # expected columns: date,energy,impact-e,lat,lat-dir,lon,lon-dir,alt,vel
    out = {}
    out['date_raw'] = row.get('date','')
    # try to parse as ISO-like
    try:
        out['date'] = datetime.fromisoformat(out['date_raw']).isoformat()
    except Exception:
        out['date'] = out['date_raw']
    out['energy_kilotons'] = parse_float(row.get('energy'))
    out['impact_e'] = parse_float(row.get('impact-e'))
    lat = parse_float(row.get('lat'))
    lon = parse_float(row.get('lon'))
    # lat-dir / lon-dir may be N/S E/W
    lat_dir = (row.get('lat-dir') or '').strip().upper()
    lon_dir = (row.get('lon-dir') or '').strip().upper()
    if lat is not None and lat_dir in ('S',):
        lat = -abs(lat)
    if lon is not None and lon_dir in ('W',):
        lon = -abs(lon)
    out['latitude'] = lat
    out['longitude'] = lon
    out['altitude_m'] = parse_float(row.get('alt'))
    out['velocity_km_s'] = parse_float(row.get('vel'))
    # derived fields will be added later
    return out

def compute_derived_metrics(norm):
    """Compute derived impact metrics using local fields only.
    - energy_kilotons -> energy_joules
    - if energy and velocity available -> estimate mass (kg) via E=1/2 m v^2
    - estimate diameter from mass assuming spherical and density
    - crude crater diameter estimate (simple empirical scaling)
    """
    out = {}
    # energy: kilotons of TNT -> joules (1 kiloton = 4.184e12 J)
    ek = norm.get('energy_kilotons')
    if ek is not None:
        out['energy_joules'] = ek * 4.184e12
    else:
        out['energy_joules'] = None

    v = norm.get('velocity_km_s')
    if v is not None:
        v_m_s = v * 1000.0
    else:
        v_m_s = None

    # estimate mass from energy (if energy_joules present) using E=1/2 m v^2
    if out['energy_joules'] is not None and v_m_s is not None and v_m_s > 0:
        m_est = (2.0 * out['energy_joules']) / (v_m_s**2)
        out['mass_kg_est'] = m_est
    else:
        out['mass_kg_est'] = None

    # estimate diameter assuming spherical density (default 3000 kg/m3)
    density = 3000.0
    if out['mass_kg_est'] is not None and out['mass_kg_est'] > 0:
        volume = out['mass_kg_est'] / density
        # volume sphere = 4/3 pi r^3
        import math
        r = ((3.0*volume)/(4.0*math.pi))**(1.0/3.0)
        out['impactor_diameter_m_est'] = 2.0 * r
    else:
        out['impactor_diameter_m_est'] = None

    # crude crater diameter estimate using simple pi-scaling: D_c ~ k * (E_joules)^(1/4)
    # This is a rough approximation and only indicative. Choose k ~ 1e-3 to get meters scale.
    if out['energy_joules'] is not None:
        out['crater_diameter_m_est'] = (out['energy_joules']**0.25) * 1e-3
    else:
        out['crater_diameter_m_est'] = None

    # classify as airburst if altitude_m > 0 and energy moderate (heuristic)
    alt = norm.get('altitude_m')
    if out['energy_joules'] is not None and alt is not None:
        if alt > 20000 or out['energy_joules'] < 1e12:
            out['impact_type'] = 'airburst'
        else:
            out['impact_type'] = 'surface/ground'
    else:
        out['impact_type'] = None

    return out

def main():
    if not SRC.exists():
        print('Missing', SRC)
        raise SystemExit(1)
    TRAJ_DIR.mkdir(parents=True, exist_ok=True)

    rows = []
    with SRC.open(encoding='utf-8') as f:
        r = csv.DictReader(f)
        for i,row in enumerate(r):
            norm = normalize_row(row)
            norm['source_row_index'] = i
            # compute derived metrics
            derived = compute_derived_metrics(norm)
            norm.update(derived)
            rows.append(norm)
            # write per-event json
            jj = TRAJ_DIR / f'fireball_{i:04d}.json'
            with jj.open('w', encoding='utf-8') as jf:
                json.dump({'index': i, 'raw': row, 'normalized': norm}, jf, indent=2)

    # write cleaned CSV (with derived metrics)
    fieldnames = [
        'source_row_index','date','date_raw','latitude','longitude','altitude_m','velocity_km_s','energy_kilotons','impact_e',
        'energy_joules','mass_kg_est','impactor_diameter_m_est','crater_diameter_m_est','impact_type'
    ]
    with OUT_CSV.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in fieldnames})

    # create GeoJSON points
    features = []
    for r in rows:
        lat = r.get('latitude')
        lon = r.get('longitude')
        props = {k:v for k,v in r.items() if k not in ('latitude','longitude')}
        if lat is None or lon is None:
            # skip events without coords
            continue
        features.append({
            'type':'Feature',
            'geometry':{'type':'Point','coordinates':[lon, lat]},
            'properties':props
        })

    geo = {'type':'FeatureCollection','features':features}
    with OUT_GEO.open('w', encoding='utf-8') as f:
        json.dump(geo, f, indent=2)

    print('Wrote', OUT_CSV, OUT_GEO, 'and', len(rows), 'per-event JSON files into', TRAJ_DIR)

if __name__ == '__main__':
    main()
