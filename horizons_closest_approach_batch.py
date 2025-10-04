#!/usr/bin/env python3
"""
Compute closest approach to Earth using JPL Horizons VECTORS for NEOs listed in a CSV.

This script reads `combined_originals.csv` (or custom input), resolves an identifier for
Horizons, requests VECTORS relative to Earth geocenter (500@0) over a date range, parses
the output, computes the minimum distance (km) and writes `horizons_intersections.csv`
for objects whose min distance <= threshold (AU).

Defaults are conservative: window=2025-01-01..2025-12-31, step=1d, threshold=0.001 AU (~150,000 km).
Run with --max to limit how many objects to check in one run.
"""
from pathlib import Path
import csv
import requests
import re
import time
import argparse
from datetime import datetime

ROOT = Path(__file__).resolve().parent
H_URL = 'https://ssd.jpl.nasa.gov/api/horizons.api'
AU_KM = 149597870.7


def parse_vectors_text(txt):
    lines = txt.splitlines()
    in_table = False
    rows = []
    epoch = None
    x=y=z=vx=vy=vz = None
    for ln in lines:
        s = ln.strip()
        if s.startswith('$$SOE'):
            in_table = True
            continue
        if s.startswith('$$EOE'):
            if epoch is not None and x is not None:
                rows.append({'epoch': epoch, 'X': x, 'Y': y, 'Z': z, 'VX': vx, 'VY': vy, 'VZ': vz})
            break
        if not in_table:
            continue
        m = re.match(r'^(\d+\.\d+) =', s)
        if m:
            if epoch is not None and x is not None:
                rows.append({'epoch': epoch, 'X': x, 'Y': y, 'Z': z, 'VX': vx, 'VY': vy, 'VZ': vz})
            epoch = float(m.group(1))
            x=y=z=vx=vy=vz = None
            continue
        if s.startswith('X') and 'Y' in s and 'Z' in s:
            vals = re.findall(r'([+-]?\d+\.\d+E[+-]\d+|[+-]?\d+\.\d+)', s)
            if len(vals) >= 3:
                try:
                    x = float(vals[0]); y = float(vals[1]); z = float(vals[2])
                except Exception:
                    pass
            continue
        if 'VX' in s and 'VY' in s and 'VZ' in s:
            vals = re.findall(r'([+-]?\d+\.\d+E[+-]\d+|[+-]?\d+\.\d+)', s)
            if len(vals) >= 3:
                try:
                    vx = float(vals[0]); vy = float(vals[1]); vz = float(vals[2])
                except Exception:
                    pass
            continue
    return rows


def query_horizons(command, start, stop, step, center='500@0'):
    params = {
        'format': 'text','COMMAND': str(command),'EPHEM_TYPE':'VECTORS','CENTER':center,
        'START_TIME': start,'STOP_TIME': stop,'STEP_SIZE': step
    }
    r = requests.get(H_URL, params=params, timeout=30, headers={'User-Agent':'meteor-madness/1.0'})
    r.raise_for_status()
    return r.text


def jd_to_datetime(jd):
    from datetime import timedelta
    ref_jd = 2451545.0
    ref_dt = datetime(2000,1,1,12,0,0)
    return ref_dt + timedelta(seconds=(jd-ref_jd)*86400.0)


def process_object(identifier, start, stop, step, verbose=False):
    try:
        txt = query_horizons(identifier, start, stop, step)
    except Exception as e:
        if verbose:
            print('Horizons error for', identifier, e)
        return None, str(e)
    rows = parse_vectors_text(txt)
    if not rows:
        return None, 'no_vectors_parsed'
    min_dist = None
    min_epoch = None
    for r in rows:
        x = r['X'] * AU_KM; y = r['Y'] * AU_KM; z = r['Z'] * AU_KM
        d = (x*x + y*y + z*z) ** 0.5
        if min_dist is None or d < min_dist:
            min_dist = d; min_epoch = r['epoch']
    return {'min_dist_km': min_dist, 'min_epoch_jd': min_epoch, 'points': len(rows)}, None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', default=str(ROOT / 'combined_originals.csv'))
    p.add_argument('--out', default=str(ROOT / 'horizons_intersections.csv'))
    p.add_argument('--start', default='2025-01-01')
    p.add_argument('--stop', default='2025-12-31')
    p.add_argument('--step', default='1d')
    p.add_argument('--threshold-au', type=float, default=0.001)
    p.add_argument('--max', type=int, default=50)
    p.add_argument('--verbose', action='store_true')
    args = p.parse_args()

    input_path = Path(args.input)
    out_path = Path(args.out)
    thr_km = args.threshold_au * AU_KM

    rows = []
    with open(input_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    results = []
    checked = 0
    for r in rows:
        if args.max and checked >= args.max:
            break
        src = (r.get('source') or r.get('Source') or '').lower()
        if src and src != 'neo':
            continue
        ident = r.get('id') or r.get('Id') or r.get('neo_reference_id') or r.get('name')
        name = r.get('name') or r.get('Name') or ''
        if not ident:
            continue
        ident = str(ident).strip()
        checked += 1
        if args.verbose:
            print(f'[{checked}] querying Horizons for {ident} ({name})')
        res, err = process_object(ident, args.start, args.stop, args.step, verbose=args.verbose)
        if err:
            results.append({'id': ident, 'name': name, 'min_dist_km': '', 'min_epoch_jd': '', 'error': err})
        else:
            intersects = res['min_dist_km'] <= thr_km
            results.append({'id': ident, 'name': name, 'min_dist_km': f"{res['min_dist_km']:.1f}", 'min_epoch_jd': res['min_epoch_jd'], 'intersects': intersects, 'points': res['points'], 'error': ''})
        # polite pause
        time.sleep(0.25)

    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['id','name','min_dist_km','min_epoch_jd','intersects','points','error']
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rr in results:
            w.writerow(rr)

    print(f'Checked {checked} objects; wrote {out_path}')


if __name__ == '__main__':
    main()
