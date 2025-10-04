#!/usr/bin/env python3
"""
Join `top200_intersections.csv` with `combined_originals.csv` to produce
`top200_intersections_final.csv` containing key fields (velocity, miss_distance, diameter, date).

Runs locally without external network calls.
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TOP = ROOT / 'top200_intersections.csv'
COMB = ROOT / 'combined_originals.csv'
OUT = ROOT / 'top200_intersections_final.csv'

if not TOP.exists():
    print('Missing', TOP)
    raise SystemExit(1)
if not COMB.exists():
    print('Missing', COMB)
    raise SystemExit(1)

# Build lookup from combined_originals by id and by numeric designation token
comb_map = {}
with COMB.open(encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        key = (row.get('id') or '').strip()
        if key:
            comb_map[key] = row
        # also map by first token of name if numeric
        name = (row.get('name') or '').strip()
        if name:
            tok = name.split()[0]
            if tok.isdigit():
                comb_map.setdefault(tok, row)

out_rows = []
with TOP.open(encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        orig = (row.get('orig_id') or '').strip()
        tried = (row.get('tried_id') or '').strip()
        name = row.get('name') or ''
        cand = None
        # prefer tried id, then orig, then numeric name token
        for k in (tried, orig, name.split()[0] if name else ''):
            if k and k in comb_map:
                cand = comb_map[k]
                break
        out = {
            'id': orig,
            'tried_id': tried,
            'name': name,
            'date': '',
            'moid_au': row.get('moid_au',''),
            'moid_km': row.get('moid_km',''),
            'intersects': row.get('intersects',''),
            'velocity_km_s': '',
            'miss_distance_km': '',
            'diameter_m': ''
        }
        if cand:
            out['velocity_km_s'] = cand.get('velocity_km_s','') or cand.get('velocity (km/s)','') or cand.get('velocity','')
            out['miss_distance_km'] = cand.get('miss_distance_km','') or cand.get('miss_distance (km)','') or cand.get('miss_distance','')
            out['diameter_m'] = cand.get('diameter_m','') or cand.get('Diameter (m)','')
            out['date'] = cand.get('date','')
        out_rows.append(out)

fieldnames = ['id','tried_id','name','date','moid_au','moid_km','intersects','velocity_km_s','miss_distance_km','diameter_m']
with OUT.open('w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in out_rows:
        w.writerow(r)

print('Wrote', OUT)
