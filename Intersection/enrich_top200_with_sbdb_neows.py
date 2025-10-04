#!/usr/bin/env python3
"""
Enrich `top200_intersections.csv` with SBDB and NeoWs fields (H, orbit elements, diameter estimates, PHA flag, solution date).

Writes `top200_intersections_enriched.csv`.

Note: This is a best-effort, uses short HTTP timeouts and records errors per row.
"""
from pathlib import Path
import csv
import requests
import time
import math

ROOT = Path(__file__).resolve().parent
IN = ROOT / 'top200_intersections.csv'
OUT = ROOT / 'top200_intersections_enriched.csv'
SBDB_URL = 'https://ssd-api.jpl.nasa.gov/sbdb.api'
NEOWS_LOOKUP = 'https://api.nasa.gov/neo/rest/v1/neo/'
NASA_API_KEY = 'GjbT7BRsxhQQaJ5kJTYcAk7u0IaRgWAAMaS4dg9y'

session = requests.Session()
session.headers.update({'User-Agent': 'meteor-madness-enricher/1.0'})


def query_sbdb(id_str):
    try:
        r = session.get(SBDB_URL, params={'sstr': id_str, 'phys': 'true'}, timeout=10)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)


def query_neows(id_str):
    try:
        url = NEOWS_LOOKUP + str(id_str)
        r = session.get(url, params={'api_key': NASA_API_KEY}, timeout=10)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def enrich_row(row):
    # choose identifier to query SBDB: prefer tried_id, then orig_id
    tried = (row.get('tried_id') or '').strip()
    orig = (row.get('orig_id') or '').strip()
    candidates = []
    if tried:
        candidates.append(tried)
    if orig and orig not in candidates:
        candidates.append(orig)
    # also try numeric token from name
    name = row.get('name') or ''
    if name:
        parts = name.split()
        if parts and parts[0].isdigit() and parts[0] not in candidates:
            candidates.insert(0, parts[0])

    sbdb = None
    sbdb_err = None
    for c in candidates:
        j, err = query_sbdb(c)
        time.sleep(0.08)
        if j and not err:
            sbdb = j
            sbdb_err = None
            used = c
            break
        else:
            sbdb_err = err

    # Prepare enriched fields default
    enrich = {
        'sbdb_spkid': '', 'H': '', 'diameter_m': '', 'diameter_min_m': '', 'diameter_max_m': '',
        'orbital_a_au': '', 'eccentricity': '', 'inclination_deg': '', 'perihelion_q_au': '',
        'asc_node_deg': '', 'arg_peri_deg': '', 'epoch_jd': '', 'solution_date': '', 'pha': '', 'sbdb_error': ''
    }

    if sbdb is None:
        enrich['sbdb_error'] = sbdb_err or 'not_found'
    else:
        # extract
        obj = sbdb.get('object', {})
        orbit = sbdb.get('orbit') or sbdb.get('object', {}).get('orbit') or {}
        phys = sbdb.get('phys') or {}
        enrich['sbdb_spkid'] = obj.get('spkid') or obj.get('des') or ''
        enrich['H'] = obj.get('H') or phys.get('H') or ''
        # orbit elements
        enrich['orbital_a_au'] = orbit.get('a') or orbit.get('semi_major_axis') or orbit.get('elements', {}).get('a') or ''
        enrich['eccentricity'] = orbit.get('e') or ''
        enrich['inclination_deg'] = orbit.get('i') or orbit.get('inclination') or ''
        enrich['perihelion_q_au'] = orbit.get('q') or ''
        enrich['asc_node_deg'] = orbit.get('node') or orbit.get('om') or ''
        enrich['arg_peri_deg'] = orbit.get('peri') or orbit.get('w') or ''
        enrich['epoch_jd'] = orbit.get('epoch') or ''
        enrich['solution_date'] = orbit.get('soln_date') or orbit.get('soln_date') or ''
        enrich['pha'] = 'True' if obj.get('pha') in (True, 'true', 'True', 'Y', '1') else 'False'
        enrich['sbdb_error'] = ''
        # physical diameters
        try:
            # try to get from phys or from 'estimated_diameter' location
            if phys:
                if phys.get('diameter'):
                    d = safe_float(phys.get('diameter'))
                    if d:
                        enrich['diameter_m'] = d
            # There may not be diameter in SBDB; we'll try NeoWs below
        except Exception:
            pass

    # Try NeoWs to get diameter estimate if missing
    if not enrich['diameter_m']:
        # try to resolve NeoWs id: use sbdb_spkid or orig id
        neows_id = enrich.get('sbdb_spkid') or orig
        if neows_id:
            jn, errn = query_neows(neows_id)
            time.sleep(0.08)
            if jn and not errn:
                try:
                    diam = jn.get('estimated_diameter', {}).get('meters', {})
                    mn = diam.get('estimated_diameter_min')
                    mx = diam.get('estimated_diameter_max')
                    if mn and mx:
                        enrich['diameter_min_m'] = mn
                        enrich['diameter_max_m'] = mx
                        enrich['diameter_m'] = (mn + mx) / 2.0
                except Exception:
                    pass

    return enrich


def main():
    if not IN.exists():
        print('Input not found:', IN)
        return
    rows = []
    with IN.open(encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)

    out_rows = []
    for i, row in enumerate(rows, start=1):
        print(f'[{i}/{len(rows)}] Enriching {row.get("tried_id") or row.get("orig_id")}')
        e = enrich_row(row)
        new = dict(row)
        new.update(e)
        out_rows.append(new)

    # write output
    fieldnames = list(out_rows[0].keys()) if out_rows else []
    with OUT.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print('Wrote', OUT)


if __name__ == '__main__':
    main()
