#!/usr/bin/env python3
"""
Create `top200_intersections.csv` by selecting rows with numeric MOID from
`earth_intersections.csv`, sorting by MOID ascending and taking the top 200.
Print a 20-row preview to stdout.
"""
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent
in_path = ROOT / 'earth_intersections.csv'
out_path = ROOT / 'top200_intersections.csv'

if not in_path.exists():
    print('Input file not found:', in_path)
    raise SystemExit(1)

rows = []
with in_path.open(encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for r in reader:
        moid = r.get('moid_au') or r.get('MOID') or ''
        try:
            moid_f = float(moid)
        except Exception:
            continue
        r['_moid_f'] = moid_f
        rows.append(r)

if not rows:
    print('No numeric MOID rows found in', in_path)
    raise SystemExit(0)

rows_sorted = sorted(rows, key=lambda x: x['_moid_f'])
top = rows_sorted[:200]

fieldnames = list(top[0].keys())
if '_moid_f' in fieldnames:
    fieldnames.remove('_moid_f')

with out_path.open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in top:
        out = {k: v for k, v in r.items() if k != '_moid_f'}
        writer.writerow(out)

print('Wrote', out_path)
print('\nPreview (first 20 rows):')
with out_path.open(encoding='utf-8') as f:
    for i, line in enumerate(f):
        print(line.strip())
        if i >= 20:
            break
