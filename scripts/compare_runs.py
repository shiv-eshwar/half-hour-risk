"""Compare a rebuilt experiment with the published results and print a JSON summary."""
import argparse
import csv
import gzip
import json
import math
from itertools import zip_longest
from pathlib import Path
from statistics import median


def read(path):
    return json.loads(path.read_text())


def equivalent(a, b, key):
    if isinstance(a, dict) and isinstance(b, dict) and a.keys() == b.keys():
        for k in a:
            equivalent(a[k], b[k], f"{key}.{k}")
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            equivalent(x, y, f"{key}[{i}]")
    elif isinstance(a, float) and isinstance(b, (float, int)) and math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-12):
        return
    elif a != b:
        raise ValueError(f"Result differs at {key}")


def compare(reference, rebuilt):
    if reference.resolve() == rebuilt.resolve():
        raise ValueError("Supply two separate result directories")
    for file in ("data_quality.json", "validation.json", "holdout.json", "case_studies.json", "streaming.json"):
        equivalent(read(reference / file), read(rebuilt / file), file)
    selection = read(reference / "validation.json")
    cutoff = selection["validation"]
    max_delta = 0.0
    n = 0
    with gzip.open(reference / "forecasts.csv.gz", "rt") as left, gzip.open(rebuilt / "forecasts.csv.gz", "rt") as right:
        a, b = csv.DictReader(left), csv.DictReader(right)
        equivalent(a.fieldnames, b.fieldnames, "forecast columns")
        for x, y in zip_longest(a, b):
            if x is None or y is None or x["issue_utc"] != y["issue_utc"]:
                raise ValueError("Forecast clocks differ")
            for name in x.keys() - {"issue_utc"}:
                v, w = float(x[name]), float(y[name])
                equivalent(v, w, f"{x['issue_utc']}.{name}")
                if name in cutoff:
                    threshold = cutoff[name]["alert_cutoff"]
                    equivalent(v > threshold, w > threshold, f"{x['issue_utc']}.{name}.alert")
                    max_delta = max(max_delta, abs(v - w))
            n += 1
    metrics = read(reference / "holdout.json")["metrics"]
    chosen = selection["selected_tape"]
    equivalent(metrics[chosen]["windows"], n, "forecast count")

    left = {p.name for p in (reference / "scaling").glob("*.json")}
    right = {p.name for p in (rebuilt / "scaling").glob("*.json")}
    equivalent(left, right, "scaling configurations")
    timings = {}
    for file in sorted(left):
        a, b = read(reference / "scaling" / file), read(rebuilt / "scaling" / file)
        for key in ("months", "master", "shuffle_partitions", "minutes", "prints", "native_fills",
                    "last_price", "sum_sq", "sum_signed_sq", "tape_quote", "tape_buy_quote"):
            equivalent(a[key], b[key], f"{file}.{key}")
        samples = b["seconds"]
        if len(samples) != 3 or min(samples) <= 0 or median(samples) != b["median_seconds"]:
            raise ValueError(f"Invalid timing record in {file}")
        if b["master"].startswith("local-cluster"):
            workers = {e["id"] for e in b["executors"] if e["id"] != "driver" and e["completed_tasks"] > 0}
            equivalent(2, len(workers), "executors with completed tasks")
        timings[file] = b["median_seconds"]
    return {"matched_forecast_windows": n, "selected_model": chosen,
            "matched_average_precision": metrics[chosen]["average_precision"],
            "largest_absolute_forecast_score_difference": max_delta,
            "rebuild_median_seconds_by_configuration": timings}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("rebuilt", type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.reference, args.rebuilt), indent=2))
