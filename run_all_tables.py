"""
Run all table scripts sequentially.
"""
import subprocess, sys

scripts = [
    "neo_ws_api.py",
    "sbdb_api.py",
    "fireball_api.py",
    "horizons_api.py",
]

for s in scripts:
    print(f"\nRunning {s}...")
    subprocess.run([sys.executable, s])
