#!/usr/bin/env python3
"""
Query JPL Horizons for trajectory VECTORS for a given NEO/SBDB id or designation.

Usage examples:
  python horizons_trajectory.py --id 433
  python horizons_trajectory.py --id "433 Eros" --start 2025-01-01 --stop 2025-12-31 --step 1d

The script writes a CSV named `trajectory_<id>.csv` with columns:
  epoch,x_km,y_km,z_km,vx_km_s,vy_km_s,vz_km_s,distance_km

Notes:
 - Uses Horizons API (text VECTORS). Converts AU -> km and AU/day -> km/s.
 - Default CENTER is Earth geocenter (500@0). If you want Earth as target set CENTER=500@0.
"""
from pathlib import Path
import argparse
import requests
import re
import csv
from datetime import datetime

ROOT = Path(__file__).resolve().parent
H_URL = 'https://ssd.jpl.nasa.gov/api/horizons.api'
AU_KM = 149597870.7
DAY_SECS = 86400.0


def parse_vectors_text(txt):
    """Parse Horizons text VECTORS output and return list of dicts with epoch, X,Y,Z,VX,VY,VZ (as floats).
    Returns [] if parse fails.
    """
    lines = txt.splitlines()
    in_table = False
    rows = []
    # VECTORS output sometimes has lines like:
    # 2451545.000000000 = A.D. 2000-Jan-01 12:00:00.0000 TDB
    # X =  -0.1771591450E+01 Y =  0.9672600357E+00 Z =  0.2494103733E-01
    # VX=  0.4679889914E-03 VY=  0.7712431205E-03 VZ= -0.1873412341E-04
    epoch = None
    x=y=z=vx=vy=vz = None
    for ln in lines:
        s = ln.strip()
        if s.startswith('$$SOE'):
            in_table = True
            continue
        if s.startswith('$$EOE'):
            # flush last if present
            if epoch is not None and x is not None:
                rows.append({'epoch': epoch, 'X': x, 'Y': y, 'Z': z, 'VX': vx, 'VY': vy, 'VZ': vz})
            break
        if not in_table:
            continue
        # match epoch lines (Julian date = A.D. ...)
        m = re.match(r'^(\d+\.\d+) =', s)
        if m:
            # flush previous
            if epoch is not None and x is not None:
                rows.append({'epoch': epoch, 'X': x, 'Y': y, 'Z': z, 'VX': vx, 'VY': vy, 'VZ': vz})
            epoch = float(m.group(1))
            x=y=z=vx=vy=vz = None
            continue
        # position line
        # allow both 'X =  val Y =  val Z = val' and 'X= val Y= val Z= val'
        if s.startswith('X') and 'Y' in s and 'Z' in s:
            # find floats
            vals = re.findall(r'([+-]?\d+\.\d+E[+-]\d+|[+-]?\d+\.\d+)', s)
            if len(vals) >= 3:
                try:
                    x = float(vals[0])
                    y = float(vals[1])
                    z = float(vals[2])
                except Exception:
                    pass
            continue
        # velocity line
        if s.startswith('VX') or s.startswith('VX=') or s.startswith('VX =') or ('VX' in s and 'VY' in s and 'VZ' in s):
            vals = re.findall(r'([+-]?\d+\.\d+E[+-]\d+|[+-]?\d+\.\d+)', s)
            if len(vals) >= 3:
                try:
                    vx = float(vals[0])
                    vy = float(vals[1])
                    vz = float(vals[2])
                except Exception:
                    pass
            continue

    return rows


def jd_to_iso(jd):
    # convert Julian Date to ISO using algorithm (approx) via datetime from JD 2451545.0 = 2000-01-01 12:00:00
    # For reporting we'll use a simple conversion by computing seconds offset from JD 2451545.0
    # This is approximate but acceptable for labeling; if high precision is needed use astropy.
    ref_jd = 2451545.0
    ref_dt = datetime(2000,1,1,12,0,0)
    seconds = (jd - ref_jd) * DAY_SECS
    return (ref_dt +  timedelta(seconds=seconds)).isoformat()


def jd_to_datetime(jd):
    from datetime import timedelta
    ref_jd = 2451545.0
    ref_dt = datetime(2000,1,1,12,0,0)
    return ref_dt + timedelta(seconds=(jd-ref_jd)*DAY_SECS)


def query_horizons(command, start, stop, step, center='500@0'):
    params = {
        'format': 'text',
        'COMMAND': str(command),
        'EPHEM_TYPE': 'VECTORS',
        'CENTER': center,
        'START_TIME': start,
        'STOP_TIME': stop,
        'STEP_SIZE': step,
    }
    r = requests.get(H_URL, params=params, timeout=30, headers={'User-Agent':'meteor-madness/1.0'})
    r.raise_for_status()
    return r.text


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--id', required=True, help='NEO id, designation, or SBDB id (e.g. 433 or "433 Eros")')
    p.add_argument('--start', default='2025-01-01', help='Start time (YYYY-MM-DD)')
    p.add_argument('--stop', default='2025-12-31', help='Stop time (YYYY-MM-DD)')
    p.add_argument('--step', default='1d', help='STEP_SIZE for Horizons (e.g. 1d, 1h)')
    p.add_argument('--center', default='500@0', help='Center for vectors (default Earth geocenter 500@0)')
    args = p.parse_args()

    txt = query_horizons(args.id, args.start, args.stop, args.step, center=args.center)
    rows = parse_vectors_text(txt)
    if not rows:
        print('No vector rows parsed from Horizons response. Raw response saved to horizons_<id>.txt')
        (ROOT / f'horizons_{args.id}.txt').write_text(txt, encoding='utf-8')
        return

    out_rows = []
    for r in rows:
        jd = r['epoch']
        x_au = r['X']; y_au = r['Y']; z_au = r['Z']
        vx_au_d = r['VX']; vy_au_d = r['VY']; vz_au_d = r['VZ']
        x_km = x_au * AU_KM
        y_km = y_au * AU_KM
        z_km = z_au * AU_KM
        vx_km_s = vx_au_d * AU_KM / DAY_SECS
        vy_km_s = vy_au_d * AU_KM / DAY_SECS
        vz_km_s = vz_au_d * AU_KM / DAY_SECS
        dist = (x_km**2 + y_km**2 + z_km**2) ** 0.5
        out_rows.append({'jd': jd, 'epoch': jd_to_datetime(jd).isoformat(), 'x_km': x_km, 'y_km': y_km, 'z_km': z_km,
                         'vx_km_s': vx_km_s, 'vy_km_s': vy_km_s, 'vz_km_s': vz_km_s, 'distance_km': dist})

    # find min distance
    min_row = min(out_rows, key=lambda r: r['distance_km'])
    print(f"Points: {len(out_rows)}; closest approach {min_row['distance_km']:.0f} km at {min_row['epoch']}")

    out_path = ROOT / f'trajectory_{args.id.replace(" ","_")}.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['jd','epoch','x_km','y_km','z_km','vx_km_s','vy_km_s','vz_km_s','distance_km'])
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print('Wrote', out_path)


if __name__ == '__main__':
    main()
