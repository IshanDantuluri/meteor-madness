#!/usr/bin/env python3
"""
Produce a compact CSV `sme_intersections.csv` by joining
`Intersection/top200_intersections.csv` with repo `combined_originals.csv`.

Fields: id,tried_id,name,source,moid_au,moid_km,intersects,date,velocity_km_s,miss_distance_km,diameter_m
"""
from pathlib import Path
import csv

ROOT = Path(__file__).resolve().parent
TOP = ROOT / 'top200_intersections.csv'
COMB = ROOT.parent / 'combined_originals.csv'
OUT = ROOT / 'sme_intersections.csv'

if not TOP.exists():
    print('Missing', TOP)
    raise SystemExit(1)
if not COMB.exists():
    print('Missing', COMB)
    raise SystemExit(1)

# build lookup by id and name token
comb_map = {}
with COMB.open(encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        k = (row.get('id') or '').strip()
        if k:
            comb_map[k] = row
        name = (row.get('name') or '').strip()
        if name:
            tok = name.split()[0]
            comb_map.setdefault(tok, row)

out_rows = []
with TOP.open(encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        tried = (row.get('tried_id') or '').strip()
        orig = (row.get('orig_id') or '').strip()
        name = row.get('name') or ''
        cand = None
        for k in (tried, orig, name.split()[0] if name else ''):
            if k and k in comb_map:
                cand = comb_map[k]
                break
        out = {
            'id': orig,
            'tried_id': tried,
            'name': name,
            'source': cand.get('source') if cand else '',
            'moid_au': row.get('moid_au',''),
            'moid_km': row.get('moid_km',''),
            'intersects': row.get('intersects',''),
            'date': (cand.get('date') if cand else ''),
            'velocity_km_s': (cand.get('velocity_km_s') if cand else ''),
            'miss_distance_km': (cand.get('miss_distance_km') if cand else ''),
            'diameter_m': (cand.get('diameter_m') if cand else '')
        }
        out_rows.append(out)

fieldnames = ['id','tried_id','name','source','moid_au','moid_km','intersects','date','velocity_km_s','miss_distance_km','diameter_m']
with OUT.open('w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in out_rows:
        w.writerow(r)

print('Wrote', OUT, 'rows=', len(out_rows))
