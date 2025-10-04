from pathlib import Path
import csv

p = Path('meteorite_table.csv')
if not p.exists():
    print('meteorite_table.csv not found')
    raise SystemExit(1)

rows = []
with p.open(encoding='utf-8') as f:
    # read raw lines, split on commas or tabs, fix stray spaces
    for i, line in enumerate(f):
        line = line.strip()
        if not line:
            continue
        # replace sequences of whitespace (including tabs) with a single comma for rows that don't contain commas
        if ',' not in line:
            parts = [p.strip() for p in line.split()]
        else:
            # some files have mixed tabs and commas
            parts = [p.strip() for p in line.replace('\t', ',').split(',')]
        rows.append(parts)

# determine max columns
maxcols = max(len(r) for r in rows)
# pad rows
norm = []
for r in rows:
    if len(r) < maxcols:
        r = r + [''] * (maxcols - len(r))
    norm.append(r)

outp = Path('meteorite_table.csv')
with outp.open('w', encoding='utf-8', newline='') as f:
    w = csv.writer(f)
    for r in norm:
        w.writerow(r)

print('Rewrote meteorite_table.csv with', len(norm), 'rows')
