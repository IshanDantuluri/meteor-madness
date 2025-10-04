#!/usr/bin/env python3
"""
find_earth_intersections.py

Resolve NeoWs numeric ids to canonical designations, query JPL SBDB for MOID, and
write `earth_intersections.csv` listing MOID (AU and km) and a boolean `intersects`
when MOID <= threshold.

Usage example:
  python find_earth_intersections.py --input combined_originals.csv --out earth_intersections.csv --threshold-au 0.001 --max-calls 500
"""
from pathlib import Path
import csv
import requests
import time
import argparse
import re

ROOT = Path(__file__).resolve().parent
SBDB_URL = 'https://ssd-api.jpl.nasa.gov/sbdb.api'
NEOWS_LOOKUP = 'https://api.nasa.gov/neo/rest/v1/neo/'
NASA_API_KEY = 'GjbT7BRsxhQQaJ5kJTYcAk7u0IaRgWAAMaS4dg9y'
AU_KM = 149597870.7


def sanitize_id(s):
    if not s:
        return None
    s = str(s).strip()
    s = re.sub(r'^\(|\)$', '', s)
    return s.strip()


def resolve_with_neows(neo_id):
    try:
        url = NEOWS_LOOKUP + str(neo_id)
        r = requests.get(url, params={'api_key': NASA_API_KEY}, timeout=8, headers={'User-Agent':'meteor-madness/1.0'})
        r.raise_for_status()
        j = r.json()
        name = j.get('name', '')
        m = re.match(r'^\s*(\d+)', name)
        if m:
            return m.group(1)
        designation = j.get('designation') or j.get('neo_reference_id')
        if designation:
            return str(designation)
    except Exception:
        return None


def query_sbdb_once(id_str):
    try:
        r = requests.get(SBDB_URL, params={'sstr': id_str}, timeout=10, headers={'User-Agent':'meteor-madness/1.0'})
        r.raise_for_status()
        return r.json(), None
    except requests.HTTPError as he:
        status = None
        try:
            status = he.response.status_code
        except Exception:
            pass
        return None, f'HTTP {status}'
    except Exception as e:
        return None, str(e)


def extract_moid_from_sbdb(json_obj):
    if not isinstance(json_obj, dict):
        return None
    # SBDB sometimes returns 'orbit' as a top-level key, or nested under 'object'.
    orbit = json_obj.get('orbit') or json_obj.get('object', {}).get('orbit') or {}
    # try common keys
    for key in ['moid', 'MOID', 'closest_approach_distance']:
        if key in orbit:
            try:
                return float(orbit[key])
            except Exception:
                pass
    # fallback: search for any key name containing 'moid'
    for k, v in orbit.items():
        if 'moid' in k.lower():
            try:
                return float(v)
            except Exception:
                pass
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', default=str(ROOT / 'combined_originals.csv'))
    p.add_argument('--out', default=str(ROOT / 'earth_intersections.csv'))
    p.add_argument('--threshold-au', type=float, default=0.001)
    p.add_argument('--max-calls', type=int, default=500)
    p.add_argument('--verbose', action='store_true', help='Print per-object progress')
    args = p.parse_args()

    input_path = Path(args.input)
    out_path = Path(args.out)

    rows = []
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    results = []
    calls = 0
    for i, r in enumerate(rows):
        if args.max_calls and calls >= args.max_calls:
            break
        src = (r.get('source') or r.get('Source') or '').lower()
        if src and src != 'neo':
            continue
        orig_id = sanitize_id(r.get('id') or r.get('Id') or r.get('neo_reference_id') or r.get('name'))
        name = r.get('name') or r.get('Name') or ''
        if not orig_id:
            continue

        # build variants to try; prefer numeric token from name (e.g., '433' from '433 Eros')
        variants = []
        # if name begins with number (e.g., '433 Eros'), try that first
        name_token = None
        m_name = re.match(r"^\s*(\d+)", str(name))
        if m_name:
            name_token = m_name.group(1)
            variants.append(name_token)
        # then original id
        variants.append(orig_id)
        if orig_id.startswith('(') and orig_id.endswith(')'):
            variants.append(orig_id.strip('()'))
        # if long numeric NeoWs id, try resolve via NeoWs and also try short numeric
        if orig_id.isdigit() and len(orig_id) > 6:
            resolved = resolve_with_neows(orig_id)
            if resolved:
                variants.insert(0, resolved)
            try:
                short = str(int(orig_id) % 1000000)
                if short not in variants:
                    variants.append(short)
            except Exception:
                pass
        # also ensure any numeric token from name is included
        if name_token and name_token not in variants:
            variants.append(name_token)

        found = False
        last_error = None
        for v in variants:
            if args.max_calls and calls >= args.max_calls:
                break
            calls += 1
            if args.verbose:
                print(f'[{calls}] trying {v} for row {i} ({orig_id})')
            j, err = query_sbdb_once(v)
            if err:
                last_error = err
                if args.verbose:
                    print('  error:', err)
                # if 400, try next variant
                continue
            moid = extract_moid_from_sbdb(j)
            if moid is not None:
                results.append({'row_index': i, 'orig_id': orig_id, 'tried_id': v, 'name': name, 'moid_au': moid, 'moid_km': moid * AU_KM, 'intersects': moid <= args.threshold_au, 'error': ''})
                found = True
                break
            else:
                last_error = 'no_moid'
            time.sleep(0.08)

        if not found:
            results.append({'row_index': i, 'orig_id': orig_id, 'tried_id': variants[0] if variants else orig_id, 'name': name, 'moid_au': '', 'moid_km': '', 'intersects': False, 'error': last_error or 'not_found'})

    # write output CSV
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        fields = ['row_index','orig_id','tried_id','name','moid_au','moid_km','intersects','error']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            # format moid_km nicely
            if isinstance(r.get('moid_km'), float):
                r['moid_km'] = f"{r['moid_km']:.1f}"
            w.writerow(r)

    print(f'Finished. SBDB calls: {calls}. Wrote {len(results)} rows to {out_path}')


if __name__ == '__main__':
    main()
