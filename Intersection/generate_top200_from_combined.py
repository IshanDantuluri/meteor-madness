#!/usr/bin/env python3
"""
Generate a top-200 intersections CSV from `combined_originals.csv`.

This script reads the combined originals (from repo root or Intersection folder),
sorts by numeric miss distance (closest first), and writes up to 200 rows to
`Intersection/top200_intersections.csv` with available fields.

No network calls. Intended to be run from the workspace root (or anywhere).
"""
from pathlib import Path
import csv

INTER = Path(__file__).resolve().parent
ROOT = INTER.parent
COMB_ROOT = ROOT / 'combined_originals.csv'
COMB_INTER = INTER / 'combined_originals.csv'
OUT = INTER / 'top200_intersections.csv'

def parse_float(s):
    if s is None:
        return None
    s = str(s).strip()
    if s in ('', 'N/A', 'NA', 'None'):
        return None
    try:
        # remove commas
        return float(s.replace(',', ''))
    except Exception:
        return None

def main():
    if COMB_INTER.exists():
        comb = COMB_INTER
    else:
        comb = COMB_ROOT
    if not comb.exists():
        print('Missing combined_originals.csv in repo root or Intersection folder')
        raise SystemExit(1)

    with comb.open(encoding='utf-8') as f:
        r = list(csv.DictReader(f))

    # parse numeric miss_distance and velocity
    for row in r:
        row['_miss_km'] = parse_float(row.get('miss_distance_km') or row.get('miss_distance (km)') or row.get('miss_distance'))
        row['_vel_kms'] = parse_float(row.get('velocity_km_s') or row.get('velocity (km/s)') or row.get('velocity'))
        row['_diam_m'] = parse_float(row.get('diameter_m') or row.get('Diameter (m)') or row.get('diameter'))

    # sort by miss distance (None => infinity)
    r_sorted = sorted(r, key=lambda x: (x['_miss_km'] if x['_miss_km'] is not None else float('inf'),
                                        x['_vel_kms'] if x['_vel_kms'] is not None else float('inf')))

    top = r_sorted[:200]

    # output fields (keep simple and available)
    fieldnames = ['source','id','name','date','diameter_m','velocity_km_s','miss_distance_km']
    with OUT.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in top:
            out = {
                'source': row.get('source',''),
                'id': row.get('id',''),
                'name': row.get('name',''),
                'date': row.get('date',''),
                'diameter_m': (row.get('diameter_m') or row.get('Diameter (m)') or '') ,
                'velocity_km_s': (row.get('velocity_km_s') or row.get('velocity (km/s)') or row.get('velocity') or ''),
                'miss_distance_km': (row.get('miss_distance_km') or row.get('miss_distance (km)') or row.get('miss_distance') or '')
            }
            w.writerow(out)

    print('Wrote', OUT, 'with', len(top), 'rows')

if __name__ == '__main__':
    main()
