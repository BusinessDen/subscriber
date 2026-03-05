#!/usr/bin/env python3
"""
Bootstrap .scraper_state.json from snapshots.json if it doesn't exist.
Run this before scraper.py in GitHub Actions to ensure incremental runs.
"""
import json
from pathlib import Path

state_file = Path("data/.scraper_state.json")
snaps_file = Path("data/snapshots.json")

if state_file.exists():
    state = json.loads(state_file.read_text())
    print(f"State file exists — last run: {state.get('last_run_date')} (ts={state.get('last_run_ts')})")
else:
    if snaps_file.exists():
        snaps = json.loads(snaps_file.read_text()).get("snapshots", [])
        if snaps:
            last = sorted(snaps, key=lambda x: x["date"])[-1]
            ts   = last["timestamp"]
            state_file.write_text(json.dumps({"last_run_ts": ts, "last_run_date": last["date"]}, indent=2))
            print(f"Bootstrapped state from snapshot: {last['date']} (ts={ts})")
        else:
            print("No snapshots found — scraper will do full fetch")
    else:
        print("No snapshots.json — scraper will do full fetch")
