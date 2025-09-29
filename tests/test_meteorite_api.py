import os
import csv
import tempfile

from meteorite_api import write_meteorite_csv


def make_sample():
    return [
        {
            "id": "1",
            "name": "TestRock",
            "nametype": "Valid",
            "recclass": "L5",
            "mass": "1234",
            "year": "1990-01-01T00:00:00.000",
            "reclat": "10.0",
            "reclong": "20.0",
            "geolocation": {"type": "Point", "coordinates": [20.0, 10.0]},
        },
        {
            "id": "2",
            "name": "NoGeo",
            "nametype": "Valid",
            "recclass": "H6",
            "mass": "5,000",
            "year": "2001-05-05T00:00:00.000",
            # missing reclat/reclong but geolocation present
            "geolocation": {"type": "Point", "coordinates": [-45.5, 60.25]},
        },
    ]


def test_write_meteorite_csv_creates_file_and_rows(tmp_path):
    sample = make_sample()
    out_csv = tmp_path / "test_meteorite_table.csv"
    write_meteorite_csv(sample, out_csv=str(out_csv))

    assert out_csv.exists()

    with open(out_csv, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    # header + 2 rows
    assert len(rows) == 1 + 2
    header = rows[0]
    assert header == ["id", "name", "nametype", "recclass", "mass (g)", "year", "reclat", "reclong", "geolocation"]
