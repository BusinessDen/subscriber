#!/usr/bin/env python3
"""
BusinessDen Revenue Tracker — Local Scraper
============================================
Writes three JSON files to ./data/:

  subscribers.json  — ALL subscribers (active + canceled), no PII, geocoded
  payments.json     — Every successful charge, accumulated over time
  snapshots.json    — Daily aggregate snapshot, one record per run

First run: fetches everything from Stripe. Charges are fetched in pages and
  saved to disk every 1,000 — if interrupted, re-running resumes from the
  last saved checkpoint automatically.

Subsequent runs: incremental — only fetches changes since the last run.

Usage:
  pip3 install requests
  python3 scraper.py --key rk_live_...

  Or: export STRIPE_KEY=rk_live_... && python3 scraper.py

Geocoding: US Census Bureau API — free, no key required.
Non-US addresses stored with lat/lng = null.
"""

import argparse
import json
import os
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

DATA_DIR            = Path(__file__).parent / "data"
SUBSCRIBERS_FILE    = DATA_DIR / "subscribers.json"
PAYMENTS_FILE       = DATA_DIR / "payments.json"
SNAPSHOTS_FILE      = DATA_DIR / "snapshots.json"
GEOCODE_CACHE_FILE  = DATA_DIR / ".geocode_cache.json"
STATE_FILE          = DATA_DIR / ".scraper_state.json"
CHECKPOINT_FILE     = DATA_DIR / ".charges_checkpoint.json"

CHECKPOINT_EVERY    = 1000   # save payments.json every N payments accumulated

# ── Plan classification ────────────────────────────────────────────────────────

def classify_plan(nickname, plan_id, interval):
    name = (nickname or plan_id or "").lower()
    if "corp" in name or "corporate" in name:
        return "corporate"
    if interval == "year" or "annual" in name or "yearly" in name:
        return "annual"
    if interval == "month" or "month" in name:
        return "monthly"
    return "other"

# ── Stripe API ─────────────────────────────────────────────────────────────────

STRIPE_BASE = "https://api.stripe.com/v1"

def stripe_get(path, key):
    r = requests.get(f"{STRIPE_BASE}/{path}",
                     headers={"Authorization": f"Bearer {key}"},
                     timeout=30)
    r.raise_for_status()
    return r.json()

def stripe_page(endpoint, key, params, retries=5):
    """Fetch a single page from Stripe with exponential backoff on errors."""
    for attempt in range(retries):
        try:
            r = requests.get(f"{STRIPE_BASE}/{endpoint}",
                             headers={"Authorization": f"Bearer {key}"},
                             params=params,
                             timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"\n  Connection error, retrying in {wait}s... ({e})")
            time.sleep(wait)

def stripe_list(endpoint, key, params=None, label=None, max_items=100000):
    """Paginate a Stripe list endpoint fully into memory."""
    params = dict(params or {})
    params["limit"] = 100
    items, last_id = [], None
    while True:
        if last_id:
            params["starting_after"] = last_id
        data  = stripe_page(endpoint, key, params)
        batch = data.get("data", [])
        items.extend(batch)
        print(f"  {label or endpoint}: {len(items)} fetched...", end="\r")
        if not data.get("has_more") or len(items) >= max_items:
            break
        last_id = batch[-1]["id"]
        time.sleep(0.08)
    print(f"  {label or endpoint}: {len(items)} total          ")
    return items

# ── State ──────────────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ── Geocoding ──────────────────────────────────────────────────────────────────

def load_geocache():
    if GEOCODE_CACHE_FILE.exists():
        with open(GEOCODE_CACHE_FILE) as f:
            return json.load(f)
    return {}

def save_geocache(cache):
    with open(GEOCODE_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def geocode(street, city, state, zip_code, cache):
    """Geocode via US Census Bureau (free, no key). Returns (lat, lng) or (None, None)."""
    cache_key = f"{street}|{city}|{state}|{zip_code}".lower().strip()
    if cache_key in cache:
        hit = cache[cache_key]
        return hit.get("lat"), hit.get("lng")
    try:
        r = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/address",
            params={"street": street or "", "city": city or "", "state": state or "",
                    "zip": zip_code or "", "benchmark": "Public_AR_Current", "format": "json"},
            timeout=12
        )
        matches = r.json().get("result", {}).get("addressMatches", [])
        if matches:
            c = matches[0]["coordinates"]
            lat, lng = c["y"], c["x"]
            cache[cache_key] = {"lat": lat, "lng": lng}
            return lat, lng
    except Exception:
        pass
    cache[cache_key] = {"lat": None, "lng": None}
    return None, None

# ── Subscriber builder ─────────────────────────────────────────────────────────

def build_subscriber(sub, cust, geocache, index, total):
    plan     = sub.get("plan") or (sub.get("items", {}).get("data") or [{}])[0].get("plan") or {}
    interval = plan.get("interval", "")
    nickname = plan.get("nickname") or plan.get("id") or ""
    amount   = plan.get("amount", 0) / 100
    sub_type = classify_plan(nickname, plan.get("id"), interval)

    shipping = (cust or {}).get("shipping") or {}
    if isinstance(shipping, dict) and "line1" in shipping:
        addr = shipping          # shipping IS the address
    elif isinstance(shipping, dict):
        addr = shipping.get("address") or {}
    else:
        addr = {}
    addr = (cust or {}).get("address") or addr

    country  = addr.get("country", "")
    street   = addr.get("line1", "")
    city     = addr.get("city", "")
    state    = addr.get("state", "")
    zip_code = addr.get("postal_code", "")

    lat, lng = None, None
    if country == "US" and (street or city or zip_code):
        print(f"  Geocoding {index}/{total}: {city or zip_code}, {state}...          ", end="\r")
        lat, lng = geocode(street, city, state, zip_code, geocache)
        time.sleep(0.12)

    started_at  = sub.get("start_date") or sub.get("created")
    canceled_at = sub.get("canceled_at")
    status      = sub.get("status", "")
    now_ts      = int(time.time())
    end_ts      = canceled_at if canceled_at else now_ts
    tenure_days = max(0, round((end_ts - started_at) / 86400)) if started_at else None

    return {
        "customer_id":          sub.get("customer"),
        "subscription_id":      sub["id"],
        "status":               status,
        "type":                 sub_type,
        "plan_name":            nickname,
        "amount":               round(amount, 2),
        "interval":             interval,
        "started_at":           started_at,
        "canceled_at":          canceled_at,
        "tenure_days":          tenure_days,
        "current_period_start": sub.get("current_period_start"),
        "current_period_end":   sub.get("current_period_end"),
        "lat":                  lat,
        "lng":                  lng,
        "state":                state if country == "US" else None,
        "country":              country or None,
    }

# ── Charge classifier ──────────────────────────────────────────────────────────

def classify_charge(charge, subs_by_customer):
    if not charge.get("invoice"):
        return "event"
    subs = subs_by_customer.get(charge.get("customer"), [])
    if not subs:
        return "monthly"
    charge_ts = charge.get("created", 0)
    for s in subs:
        start = s.get("started_at") or 0
        end   = s.get("canceled_at") or int(time.time()) + 86400
        if start <= charge_ts <= end:
            return s["type"]
    return subs[0]["type"]

def charge_to_payment(c, subs_by_customer):
    created = c.get("created")
    return {
        "charge_id":   c["id"],
        "customer_id": c.get("customer"),
        "amount":      round(c.get("amount", 0) / 100, 2),
        "date":        datetime.fromtimestamp(created, tz=timezone.utc).strftime("%Y-%m-%d"),
        "timestamp":   created,
        "type":        classify_charge(c, subs_by_customer),
        "description": c.get("description") or "",
        "invoice_id":  c.get("invoice"),
    }

# ── Payments I/O ───────────────────────────────────────────────────────────────

def load_payments():
    if PAYMENTS_FILE.exists():
        with open(PAYMENTS_FILE) as f:
            return json.load(f).get("payments", [])
    return []

def save_payments(payments):
    sorted_pays = sorted(payments, key=lambda x: x["timestamp"], reverse=True)
    with open(PAYMENTS_FILE, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "count":        len(sorted_pays),
            "payments":     sorted_pays,
        }, f, indent=2)
    return sorted_pays

# ── Main scrape ────────────────────────────────────────────────────────────────

def scrape(stripe_key):
    DATA_DIR.mkdir(exist_ok=True)
    geocache = load_geocache()
    state    = load_state()
    now_ts   = int(time.time())
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    is_first = "last_run_ts" not in state

    if is_first:
        print("\n  First run — fetching complete history.")
    else:
        last = datetime.fromtimestamp(state["last_run_ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"\n  Incremental run — fetching changes since {last}")

    since = state.get("last_run_ts", 0)

    # ── 1. Active subscriptions ───────────────────────────────────────────────
    print("\n[1/5] Fetching active subscriptions...")
    active_subs = stripe_list("subscriptions", stripe_key, {"status": "active"}, "active subs")

    # ── 2. Canceled subscriptions ─────────────────────────────────────────────
    print("\n[2/5] Fetching canceled subscriptions...")
    if is_first:
        canceled_subs = stripe_list("subscriptions", stripe_key, {"status": "canceled"}, "canceled subs")
    else:
        # Stripe doesn't support filtering canceled subs by canceled_at,
        # so fetch all and filter in Python
        all_canceled = stripe_list("subscriptions", stripe_key,
                                   {"status": "canceled"}, "canceled subs")
        canceled_subs = [s for s in all_canceled if (s.get("canceled_at") or 0) >= since]
        print(f"  Filtered to {len(canceled_subs)} canceled since last run")
    all_new_subs = active_subs + canceled_subs

    # ── 3. Customer details ───────────────────────────────────────────────────
    print("\n[3/5] Fetching customer details...")
    existing_subs_map = {}
    if SUBSCRIBERS_FILE.exists():
        with open(SUBSCRIBERS_FILE) as f:
            for s in json.load(f).get("subscribers", []):
                existing_subs_map[s["subscription_id"]] = s

    need_customer = set()
    for sub in all_new_subs:
        existing = existing_subs_map.get(sub["id"])
        if not existing or (existing.get("lat") is None and existing.get("country") == "US"):
            if sub.get("customer"):
                need_customer.add(sub["customer"])

    customers = {}
    cust_list = list(need_customer)
    for i, cid in enumerate(cust_list):
        print(f"  Customer {i+1}/{len(cust_list)}...", end="\r")
        try:
            customers[cid] = stripe_get(f"customers/{cid}", stripe_key)
        except Exception as e:
            print(f"\n  Warning: could not fetch {cid}: {e}")
        time.sleep(0.06)
    print(f"  Fetched {len(customers)} customers          ")

    # ── 4. Build subscriber records ───────────────────────────────────────────
    print(f"\n[4/5] Building {len(all_new_subs)} subscriber records...")
    geocoded = skipped = 0
    new_sub_records = []

    for i, sub in enumerate(all_new_subs):
        sid      = sub["id"]
        existing = existing_subs_map.get(sid)
        cust     = customers.get(sub.get("customer"))

        if existing and existing.get("lat") is not None:
            existing["status"]               = sub.get("status", existing["status"])
            existing["canceled_at"]          = sub.get("canceled_at")
            existing["current_period_start"] = sub.get("current_period_start")
            existing["current_period_end"]   = sub.get("current_period_end")
            start = existing.get("started_at") or 0
            end   = existing["canceled_at"] or now_ts
            existing["tenure_days"] = max(0, round((end - start) / 86400)) if start else None
            new_sub_records.append(existing)
            skipped += 1
            continue

        rec = build_subscriber(sub, cust, geocache, i+1, len(all_new_subs))
        if rec["lat"]:
            geocoded += 1
        else:
            skipped += 1
        new_sub_records.append(rec)

    new_ids  = {r["subscription_id"] for r in new_sub_records}
    retained = [s for s in existing_subs_map.values() if s["subscription_id"] not in new_ids]
    all_sub_records = retained + new_sub_records

    save_geocache(geocache)
    print(f"\n  Geocoded: {geocoded} new · {skipped} from cache/skipped")
    n_active   = sum(1 for s in all_sub_records if s["status"] == "active")
    n_canceled = sum(1 for s in all_sub_records if s["status"] == "canceled")
    print(f"  Total: {len(all_sub_records)} ({n_active} active, {n_canceled} canceled)")

    # Customer->subs lookup for charge classification
    subs_by_customer = {}
    for s in all_sub_records:
        subs_by_customer.setdefault(s["customer_id"], []).append(s)

    # ── 5. Charges (chunked, checkpoint every 1,000 payments) ────────────────
    existing_payments = load_payments()
    existing_ids      = {p["charge_id"] for p in existing_payments}

    # A charge checkpoint means a full history fetch was interrupted — resume it
    # even if state file now exists (state is saved before charges on first run)
    has_checkpoint = CHECKPOINT_FILE.exists()

    if not is_first and not has_checkpoint:
        # Incremental — just fetch charges since last run
        print("\n[5/5] Fetching new charges since last run...")
        new_charges  = stripe_list("charges", stripe_key, {"created[gte]": since}, "new charges")
        new_pays     = [charge_to_payment(c, subs_by_customer)
                        for c in new_charges if c.get("status") == "succeeded"]
        added        = [p for p in new_pays if p["charge_id"] not in existing_ids]
        all_payments = save_payments(existing_payments + added)
        print(f"  Added {len(added)} new payments ({len(all_payments)} total)")

    else:
        # Full history — paginate manually and checkpoint every 1,000 payments
        checkpoint    = {}
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE) as f:
                checkpoint = json.load(f)

        resume_after  = checkpoint.get("last_charge_id")
        total_fetched = checkpoint.get("total_fetched", 0)
        total_saved   = checkpoint.get("total_saved", 0)

        if resume_after:
            print(f"\n[5/5] Resuming charges from checkpoint "
                  f"({total_fetched} fetched, {total_saved} payments saved so far)...")
        else:
            print(f"\n[5/5] Fetching all charges — saving every {CHECKPOINT_EVERY} payments...")

        params   = {"limit": 100}
        if resume_after:
            params["starting_after"] = resume_after

        buf      = []      # payments accumulated since last checkpoint
        last_id  = None
        has_more = True

        while has_more:
            if last_id:
                params["starting_after"] = last_id

            data     = stripe_page("charges", stripe_key, params)
            batch    = data.get("data", [])
            has_more = data.get("has_more", False)

            for c in batch:
                total_fetched += 1
                last_id = c["id"]
                if c.get("status") == "succeeded" and c["id"] not in existing_ids:
                    buf.append(charge_to_payment(c, subs_by_customer))

            print(f"  Charges: {total_fetched} fetched, "
                  f"{total_saved + len(buf)} payments saved...", end="\r")

            # Checkpoint every 1,000 payments
            if len(buf) >= CHECKPOINT_EVERY:
                existing_payments = save_payments(existing_payments + buf)
                existing_ids     |= {p["charge_id"] for p in buf}
                total_saved      += len(buf)
                buf               = []
                with open(CHECKPOINT_FILE, "w") as f:
                    json.dump({
                        "last_charge_id": last_id,
                        "total_fetched":  total_fetched,
                        "total_saved":    total_saved,
                    }, f)
                print(f"\n  ✓ Checkpoint — {total_fetched} charges fetched, "
                      f"{total_saved} payments saved")

            time.sleep(0.08)

        # Flush remaining buffer
        if buf:
            existing_payments = save_payments(existing_payments + buf)
            total_saved      += len(buf)

        # Clear checkpoint — full fetch is done
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()

        all_payments = existing_payments
        print(f"\n  Done — {total_fetched} charges fetched, {total_saved} payments saved")

    # ── Snapshot ──────────────────────────────────────────────────────────────
    active_records = [s for s in all_sub_records if s["status"] == "active"]
    counts = {"monthly": 0, "annual": 0, "corporate": 0, "other": 0}
    mrr    = 0.0
    for s in active_records:
        counts[s["type"]] = counts.get(s["type"], 0) + 1
        mrr += s["amount"] / 12 if s["interval"] == "year" else s["amount"]

    thirty_ago   = now_ts - 30 * 86400
    new_30d      = sum(1 for s in all_sub_records if (s.get("started_at") or 0) >= thirty_ago)
    canceled_30d = sum(1 for s in all_sub_records
                       if s["status"] == "canceled" and (s.get("canceled_at") or 0) >= thirty_ago)

    def avg_tenure(t):
        tenures = [s["tenure_days"] for s in all_sub_records
                   if s["status"] == "canceled" and s["type"] == t
                   and s.get("tenure_days") is not None]
        return round(sum(tenures) / len(tenures), 1) if tenures else None

    snapshot = {
        "date":                 today,
        "timestamp":            now_ts,
        "active_total":         len(active_records),
        "active_monthly":       counts.get("monthly", 0),
        "active_annual":        counts.get("annual", 0),
        "active_corporate":     counts.get("corporate", 0),
        "active_other":         counts.get("other", 0),
        "mrr":                  round(mrr, 2),
        "arr":                  round(mrr * 12, 2),
        "new_30d":              new_30d,
        "canceled_30d":         canceled_30d,
        "avg_tenure_monthly":   avg_tenure("monthly"),
        "avg_tenure_annual":    avg_tenure("annual"),
        "avg_tenure_corporate": avg_tenure("corporate"),
    }

    # ── Write subscribers + snapshots ─────────────────────────────────────────
    # Save state NOW so steps 1-4 are skipped if charges crash and we resume
    save_state({"last_run_ts": now_ts, "last_run_date": today})

    print("\nWriting subscribers.json...")
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump({
            "generated_at":   datetime.now(timezone.utc).isoformat(),
            "count_active":   len(active_records),
            "count_canceled": len(all_sub_records) - len(active_records),
            "count_total":    len(all_sub_records),
            "subscribers":    all_sub_records,
        }, f, indent=2)

    print("Writing snapshots.json...")
    existing_snaps = []
    if SNAPSHOTS_FILE.exists():
        with open(SNAPSHOTS_FILE) as f:
            existing_snaps = json.load(f).get("snapshots", [])
    existing_snaps = [s for s in existing_snaps if s["date"] != today]
    existing_snaps.append(snapshot)
    existing_snaps.sort(key=lambda x: x["date"])
    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "snapshots":    existing_snaps,
        }, f, indent=2)


    # ── Summary ───────────────────────────────────────────────────────────────
    canceled_total = len(all_sub_records) - len(active_records)
    print(f"""
╔══════════════════════════════════════════════╗
║           Scrape Complete                    ║
╠══════════════════════════════════════════════╣
║  Active subscribers  : {len(active_records):<22}║
║    Monthly           : {counts.get('monthly',0):<22}║
║    Annual            : {counts.get('annual',0):<22}║
║    Corporate         : {counts.get('corporate',0):<22}║
║  Canceled (all time) : {canceled_total:<22}║
║  MRR                 : ${mrr:<21.2f}║
║  ARR                 : ${mrr*12:<21.2f}║
╠══════════════════════════════════════════════╣
║  Avg tenure (monthly)   : {str(avg_tenure('monthly') or '—')+'d':<19}║
║  Avg tenure (annual)    : {str(avg_tenure('annual') or '—')+'d':<19}║
║  Avg tenure (corporate) : {str(avg_tenure('corporate') or '—')+'d':<19}║
╚══════════════════════════════════════════════╝

Output: {DATA_DIR.resolve()}
""")

# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BusinessDen Revenue Tracker — Scraper")
    parser.add_argument("--key",  help="Stripe restricted API key (or STRIPE_KEY env var)")
    parser.add_argument("--full", action="store_true",
                        help="Force full re-fetch (clears incremental state and charge checkpoint)")
    args = parser.parse_args()

    stripe_key = args.key or os.environ.get("STRIPE_KEY", "").strip()
    if not stripe_key:
        print("Error: provide --key rk_live_... or set STRIPE_KEY env var")
        sys.exit(1)

    if args.full:
        for f in [STATE_FILE, CHECKPOINT_FILE]:
            if f.exists():
                f.unlink()
        print("Cleared state — performing full re-fetch.")

    print("BusinessDen Revenue Tracker Scraper")
    print(f"Key: {stripe_key[:14]}...")
    print(f"Output: {DATA_DIR.resolve()}")

    try:
        scrape(stripe_key)
    except requests.exceptions.HTTPError as e:
        try:
            msg = e.response.json().get("error", {}).get("message", str(e))
        except Exception:
            msg = str(e)
        print(f"\nStripe API error: {msg}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted — progress has been saved. Re-run to resume.")
        sys.exit(0)
