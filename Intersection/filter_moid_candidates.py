#!/usr/bin/env python3
from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parent
TOP = ROOT / 'top200_intersections.csv'
OUT = ROOT / 'sme_intersections_impact_candidates.csv'

# Earth radius in AU
EARTH_RADIUS_KM = 6371.0
KM_PER_AU = 149597870.7
EARTH_RADIUS_AU = EARTH_RADIUS_KM / KM_PER_AU

if not TOP.exists():
    print('Missing', TOP)
    raise SystemExit(1)

rows = []
with TOP.open(encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        moid = row.get('moid_au','')
        try:
            m = float(moid)
        except Exception:
            continue
        if m <= EARTH_RADIUS_AU:
            rows.append(row)

fieldnames = r.fieldnames if 'r' in locals() and r.fieldnames else ['row_index','orig_id','tried_id','name','moid_au','moid_km','intersects','error']
with OUT.open('w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for rr in rows:
        w.writerow(rr)

print('Earth radius (AU)=', EARTH_RADIUS_AU)
print('Found', len(rows), 'candidates (written to', OUT, ')')
