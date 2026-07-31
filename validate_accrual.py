#!/usr/bin/env python3
"""
BD Revenue -- Stripe structure validator (READ ONLY)  v2

Makes only GET requests. Writes stripe_structure_report.txt. Output is
scrubbed of customer names, emails, and customer/charge/invoice IDs so the
report can be pasted back verbatim.

v2 changes:
  - stripe-python 15 removed dict inheritance from StripeObject, so .get(),
    .keys() and .items() no longer exist on API objects. Everything is now
    normalized to plain dicts at the boundary.
  - Added a permissions probe that reports exactly which resources the key
    can read, so missing scopes can be fixed in one pass.
  - Errors are classified honestly: a permissions problem and a bug in this
    script no longer produce the same message.
  - Plan taxonomy falls back to price objects embedded in subscriptions when
    Price.list is not permitted.

Usage:
     export STRIPE_KEY=rk_live_...             (leading space = stays out of history)
    python validate_accrual.py

Optional:
    MAX_INVOICES=5000 python validate_accrual.py
    QUIET=1                     suppress stdout, write file only

Python 3.7+ compatible.
"""

import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import islice

try:
    import stripe
except ImportError:
    sys.exit("stripe library not installed. Run: pip install stripe")

KEY = os.environ.get("STRIPE_KEY") or os.environ.get("STRIPE_SECRET_KEY")
if not KEY:
    sys.exit("Set STRIPE_KEY (or STRIPE_SECRET_KEY) in this shell.")

stripe.api_key = KEY

OUTFILE = "stripe_structure_report.txt"

# When running in CI, suppress stdout so the report does not land in the
# Actions log. The workflow uploads the file as an artifact instead.
QUIET = os.environ.get("QUIET") == "1"

# The accrual ledger begins here. Anything earned before this date is
# discarded, but invoices whose service period crosses this line still
# matter -- their unearned remainder IS the opening deferred balance.
LEDGER_START = datetime(2026, 1, 1, tzinfo=timezone.utc)

# How far back the real collector must fetch to catch service periods that
# spill into 2026: one year of annual subscriptions plus a month of slack.
FETCH_START = datetime(2024, 12, 1, tzinfo=timezone.utc)

MAX_INVOICES = int(os.environ.get("MAX_INVOICES", "40000"))

_buffer = []
PERMS = {}
PRICE_INDEX = {}


def out(line=""):
    if not QUIET:
        print(line)
    _buffer.append(line)


def rule(title):
    out()
    out("=" * 72)
    out(title)
    out("=" * 72)


def sub(title):
    out()
    out("--- " + title + " " + "-" * max(0, 66 - len(title)))


# ---------------------------------------------------------------------------
# StripeObject normalization
#
# stripe-python 15 dropped dict inheritance. API objects support bracket
# access and `in`, but not .get(), .keys(), .items() or iteration. Rather
# than defend at every call site, convert to plain dicts once on the way in.
# ---------------------------------------------------------------------------

def plain(obj):
    """Recursively convert a StripeObject into ordinary dicts and lists.

    None is preserved rather than coerced to {}, so null fields such as
    `nickname` stay falsy for callers.
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        return dict((k, plain(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return [plain(v) for v in obj]
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            return plain(to_dict())
        except Exception:
            return {}
    return obj


_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_IDS = re.compile(r"\b(cus|ch|in|pi|py|txn|card|pm|re|sub)_[A-Za-z0-9]+")
_KEYS = re.compile(r"\b(rk|sk)_(live|test)_[A-Za-z0-9*]+")


def scrub(value):
    """Strip emails, API keys, and customer/charge identifiers from text."""
    if value is None:
        return ""
    text = str(value)
    text = _EMAIL.sub("[email]", text)
    text = _KEYS.sub(lambda m: m.group(1) + "_" + m.group(2) + "_[redacted]", text)
    text = _IDS.sub(lambda m: m.group(1) + "_[redacted]", text)
    return text[:160]


def money(cents, currency="usd"):
    if cents is None:
        return "-"
    return "{}{:,.2f}".format("$" if currency == "usd" else "", cents / 100.0)


def ts(unix):
    if not unix:
        return "-"
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d")


def days(a, b):
    if not a or not b:
        return None
    return int(round((b - a) / 86400.0))


def month_key(unix):
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m")


def is_permission_error(exc):
    name = type(exc).__name__
    if name in ("PermissionError", "AuthenticationError"):
        return True
    return "Permission denied" in str(exc) or "does not have access" in str(exc)


def guard(label, fn):
    """Run a section, distinguishing permission failures from code failures."""
    try:
        fn()
    except Exception as exc:
        out()
        if is_permission_error(exc):
            out("!! {} SKIPPED -- key lacks read access to this resource.".format(label))
            out("   {}".format(scrub(exc)))
        else:
            out("!! {} FAILED -- this is a bug in the validator, not a".format(label))
            out("   permissions problem. {}: {}".format(
                type(exc).__name__, scrub(exc)))
            import traceback
            tb = traceback.format_exc().strip().splitlines()
            for line in tb[-4:]:
                out("   | {}".format(scrub(line)))


# ---------------------------------------------------------------------------
# 0. Permissions probe
# ---------------------------------------------------------------------------

PROBES = [
    ("Product", lambda: stripe.Product.list(limit=1), "plan labels"),
    ("Price", lambda: stripe.Price.list(limit=1), "plan taxonomy"),
    ("Subscription", lambda: stripe.Subscription.list(limit=1),
     "subscriber counts, renewal calendar"),
    ("Invoice", lambda: stripe.Invoice.list(limit=1),
     "ACCRUAL LEDGER -- service periods"),
    ("Charge", lambda: stripe.Charge.list(limit=1), "cash basis cross-check"),
    ("BalanceTransaction", lambda: stripe.BalanceTransaction.list(limit=1),
     "net-of-fees line"),
    ("Refund", lambda: stripe.Refund.list(limit=1), "revenue reversals"),
    ("CreditNote", lambda: stripe.CreditNote.list(limit=1), "revenue reversals"),
    ("Customer", lambda: stripe.Customer.list(limit=1), "MemberPress reconciliation"),
]

CRITICAL = ["Price", "Subscription", "Invoice", "BalanceTransaction", "Refund"]


def section_probe():
    rule("0. PERMISSIONS PROBE")
    out("One limit=1 read per resource. Determines what the rest can do.")
    out()
    out("{:<22} {:<10} {}".format("RESOURCE", "ACCESS", "NEEDED FOR"))

    for name, call, needed in PROBES:
        try:
            call()
            PERMS[name] = True
            status = "ok"
        except Exception as exc:
            PERMS[name] = False
            status = "DENIED" if is_permission_error(exc) else "ERROR"
        out("{:<22} {:<10} {}".format(name, status, needed))

    needed_for = dict((p[0], p[2]) for p in PROBES)
    missing = [n for n in CRITICAL if not PERMS.get(n)]

    sub("Verdict")
    if not missing:
        out("All resources required for the accrual build are readable.")
    else:
        out("The accrual ledger CANNOT be built until read access is added for:")
        for n in missing:
            out("   - {:<20} ({})".format(n, needed_for[n]))
        out()
        out("This is not a validation-only problem. The production collector")
        out("reads the same resources on every run, so these scopes are")
        out("required regardless of what this script does.")


# ---------------------------------------------------------------------------
# 1. Environment
# ---------------------------------------------------------------------------

def section_env():
    rule("1. ENVIRONMENT")
    out("Python              {}".format(sys.version.split()[0]))
    out("stripe SDK          {}".format(getattr(stripe, "VERSION", "unknown")))
    out("Pinned API version  {}".format(
        getattr(stripe, "api_version", None) or "SDK default"))
    parts = KEY.split("_")
    prefix = "_".join(parts[:2]) if len(parts) >= 2 else KEY[:6]
    out("Key type            {} ({})".format(
        prefix, "RESTRICTED" if KEY.startswith("rk_") else "FULL SECRET"))
    out("Mode                {}".format("LIVE" if "_live_" in KEY else "TEST"))
    out("Ledger start        {}".format(LEDGER_START.strftime("%Y-%m-%d")))
    out("Collector fetch from  {}".format(FETCH_START.strftime("%Y-%m-%d")))


# ---------------------------------------------------------------------------
# 2. Subscriptions (runs before prices so it can harvest embedded price data)
# ---------------------------------------------------------------------------

def section_subscriptions():
    rule("2. SUBSCRIPTIONS")
    if not PERMS.get("Subscription"):
        out("Skipped -- no read access.")
        return

    by_status = Counter()
    by_price = Counter()
    price_amounts = Counter()
    multi_item = 0
    with_discount = 0
    with_trial = 0
    cancel_at_end = 0
    renewal_month = Counter()
    period_lengths = Counter()
    total = 0

    for raw in stripe.Subscription.list(status="all", limit=100).auto_paging_iter():
        s = plain(raw)
        total += 1
        by_status[s.get("status")] += 1

        items = (s.get("items") or {}).get("data") or []
        if len(items) > 1:
            multi_item += 1
        if s.get("discount"):
            with_discount += 1
        if s.get("trial_end"):
            with_trial += 1
        if s.get("cancel_at_period_end"):
            cancel_at_end += 1

        # Harvest price objects embedded in subscription items. This is the
        # fallback taxonomy when Price.list is not permitted.
        for it in items:
            price = it.get("price") or {}
            pid = price.get("id")
            if pid and pid not in PRICE_INDEX:
                rec = price.get("recurring") or {}
                interval = rec.get("interval")
                count = rec.get("interval_count") or 1
                cadence = "one-time"
                if interval:
                    cadence = interval if count == 1 else "{}x {}".format(count, interval)
                PRICE_INDEX[pid] = {
                    "id": pid,
                    "amount": price.get("unit_amount"),
                    "currency": price.get("currency") or "usd",
                    "cadence": cadence,
                    "nickname": scrub(price.get("nickname")),
                    "active": price.get("active"),
                    "meta": price.get("metadata") or {},
                    "prod_meta": {},
                    "source": "subscription",
                }

        if s.get("status") in ("active", "trialing", "past_due"):
            for it in items:
                price = it.get("price") or {}
                qty = it.get("quantity") or 1
                by_price[price.get("id")] += qty
                price_amounts[price.get("unit_amount")] += qty
            cps = s.get("current_period_start")
            cpe = s.get("current_period_end")
            if cpe:
                renewal_month[month_key(cpe)] += 1
            d = days(cps, cpe)
            if d:
                period_lengths[d] += 1

    sub("Counts by status")
    for status, n in by_status.most_common():
        out("  {:<20} {:>6}".format(str(status), n))
    out("  {:<20} {:>6}".format("TOTAL", total))

    sub("Active-equivalent subscriptions by price")
    out("{:<32} {:>10} {:<10} {:>8}".format("PRICE ID", "AMOUNT", "CADENCE", "SUBS"))
    for pid, n in by_price.most_common():
        info = PRICE_INDEX.get(pid, {})
        out("{:<32} {:>10} {:<10} {:>8}".format(
            str(pid), money(info.get("amount")), info.get("cadence", "?"), n))

    sub("Structural flags")
    out("  multi-item subscriptions    {:>6}   (corporate seat plans?)".format(multi_item))
    out("  carrying a discount/coupon  {:>6}   (changes recognized amount)".format(with_discount))
    out("  with a trial period         {:>6}   (zero-revenue service days)".format(with_trial))
    out("  set to cancel at period end {:>6}   (still accrue until then)".format(cancel_at_end))

    sub("Current period lengths (days) among active subs")
    out("Confirms the daily accrual denominator varies by period, not 1/365.")
    for d, n in sorted(period_lengths.items()):
        if 27 <= d <= 32:
            tag = "monthly"
        elif 360 <= d <= 372:
            tag = "annual"
        else:
            tag = "<-- IRREGULAR"
        out("  {:>4} days  {:>6} subs   {}".format(d, n, tag))

    sub("Forward renewal calendar (next 14 months)")
    for mk in sorted(renewal_month)[:14]:
        n = renewal_month[mk]
        out("  {}  {:>5}  {}".format(mk, n, "#" * min(50, n // 5)))

    sub("Price-point distribution among active subs")
    out("Reveals the annual ladder and any off-ladder pricing.")
    for amount, n in sorted(price_amounts.items(), key=lambda kv: -(kv[0] or 0)):
        out("  {:>10}  {:>6} subs".format(money(amount), n))


# ---------------------------------------------------------------------------
# 3. Prices and products
# ---------------------------------------------------------------------------

def section_prices():
    rule("3. PRICES AND PRODUCTS")

    if not PERMS.get("Price"):
        out("No read access to Price. Falling back to price objects embedded")
        out("in subscription items, which covers every plan currently in use")
        out("but will not show unused or archived prices.")
        sub("Taxonomy derived from subscriptions ({} prices)".format(len(PRICE_INDEX)))
        out("{:<32} {:>10} {:<10} {}".format("PRICE ID", "AMOUNT", "CADENCE", "NICKNAME"))
        for pid, r in sorted(PRICE_INDEX.items(),
                             key=lambda kv: -(kv[1].get("amount") or 0)):
            out("{:<32} {:>10} {:<10} {}".format(
                pid, money(r.get("amount")), r.get("cadence", "?"),
                (r.get("nickname") or "")[:28]))
        _metadata_summary()
        return

    products = {}
    if PERMS.get("Product"):
        for raw in stripe.Product.list(limit=100).auto_paging_iter():
            p = plain(raw)
            products[p.get("id")] = p

    rows = []
    for raw in stripe.Price.list(limit=100).auto_paging_iter():
        p = plain(raw)
        rec = p.get("recurring") or {}
        interval = rec.get("interval")
        count = rec.get("interval_count") or 1
        cadence = "one-time"
        if interval:
            cadence = interval if count == 1 else "{}x {}".format(count, interval)
        prod = products.get(p.get("product")) or {}
        row = {
            "id": p.get("id"),
            "active": p.get("active"),
            "amount": p.get("unit_amount"),
            "currency": p.get("currency") or "usd",
            "cadence": cadence,
            "scheme": p.get("billing_scheme"),
            "tiers": p.get("tiers_mode"),
            "nickname": scrub(p.get("nickname")),
            "product": scrub(prod.get("name") or p.get("product")),
            "meta": p.get("metadata") or {},
            "prod_meta": prod.get("metadata") or {},
            "source": "price",
        }
        rows.append(row)
        PRICE_INDEX[row["id"]] = row

    rows.sort(key=lambda r: (not r["active"], r["cadence"], -(r["amount"] or 0)))

    sub("All prices ({} total, {} active)".format(
        len(rows), sum(1 for r in rows if r["active"])))
    out("{:<32} {:>10} {:<10} {:<4} {}".format(
        "PRICE ID", "AMOUNT", "CADENCE", "ACT", "PRODUCT / NICKNAME"))
    for r in rows:
        label = r["nickname"] or r["product"]
        out("{:<32} {:>10} {:<10} {:<4} {}".format(
            str(r["id"]), money(r["amount"], r["currency"]), r["cadence"],
            "y" if r["active"] else "n", str(label)[:30]))

    tiered = [r for r in rows if r["scheme"] != "per_unit" or r["tiers"]]
    if tiered:
        sub("Non-flat pricing (needs special accrual handling)")
        for r in tiered:
            out("  {:<32} scheme={} tiers={}".format(
                str(r["id"]), r["scheme"], r["tiers"]))

    _metadata_summary()


def _metadata_summary():
    sub("Metadata keys on prices")
    key_vals = defaultdict(Counter)
    for r in PRICE_INDEX.values():
        for k, v in (r.get("meta") or {}).items():
            key_vals[k][scrub(v)] += 1
    if not key_vals:
        out("(none -- classification must come from amount + interval)")
    for k, counter in key_vals.items():
        if len(counter) <= 20:
            pairs = ", ".join("{}={}".format(val, n) for val, n in counter.most_common())
            out("  {:<24} {}".format(k, pairs))
        else:
            out("  {:<24} {} distinct values".format(k, len(counter)))

    prod_keys = defaultdict(Counter)
    for r in PRICE_INDEX.values():
        for k, v in (r.get("prod_meta") or {}).items():
            prod_keys[k][scrub(v)] += 1
    if prod_keys:
        sub("Metadata keys on products")
        for k, counter in prod_keys.items():
            if len(counter) <= 20:
                pairs = ", ".join("{}={}".format(val, n) for val, n in counter.most_common())
                out("  {:<24} {}".format(k, pairs))
            else:
                out("  {:<24} {} distinct values".format(k, len(counter)))

    out()
    out("QUESTION THIS ANSWERS: can corporate be told apart from regular")
    out("annual by metadata, or only by an amount threshold?")


# ---------------------------------------------------------------------------
# 4. Invoice line item shape -- the heart of the accrual ledger
# ---------------------------------------------------------------------------

def section_line_shape():
    rule("4. INVOICE LINE ITEM SHAPE")
    if not PERMS.get("Invoice"):
        out("Skipped -- no read access to Invoice.")
        out()
        out("This is the single most important section. The accrual ledger is")
        out("generated from line-level service periods, and nothing else in")
        out("the Stripe API carries them. Without Invoice read access there")
        out("is no accrual build.")
        return

    out("The accrual ledger is generated from line-level service periods,")
    out("so the exact field layout here determines the collector's parser.")

    sample = [plain(i) for i in islice(
        stripe.Invoice.list(limit=100, status="paid").auto_paging_iter(), 300)]
    if not sample:
        out("No paid invoices returned.")
        return

    first_line = None
    for inv in sample:
        lines = (inv.get("lines") or {}).get("data") or []
        if lines:
            first_line = lines[0]
            break
    if first_line is None:
        out("No line items found in sample.")
        return

    sub("Field keys present on a line item")
    keys = sorted(first_line.keys())
    for i in range(0, len(keys), 4):
        out("  " + "  ".join("{:<24}".format(k) for k in keys[i:i + 4]))

    probe = ["period", "proration", "type", "parent", "subscription",
             "subscription_item", "price", "plan", "pricing", "amount",
             "quantity", "discount_amounts", "tax_amounts", "currency"]
    sub("Presence of fields the collector depends on")
    for field in probe:
        out("  {:<22} {}".format(field, "present" if field in first_line else "ABSENT"))
    out()
    out("NOTE: recent API versions replaced `type` and `plan` with `parent`")
    out("and `pricing`. The above shows which dialect this account speaks.")

    sub("Example line (scrubbed)")
    per = first_line.get("period") or {}
    out("  amount        {}".format(money(first_line.get("amount"))))
    out("  period.start  {}".format(ts(per.get("start"))))
    out("  period.end    {}".format(ts(per.get("end"))))
    out("  period days   {}".format(days(per.get("start"), per.get("end"))))
    out("  proration     {}".format(first_line.get("proration")))
    out("  description   {}".format(scrub(first_line.get("description"))))

    period_lengths = Counter()
    multi_line = 0
    negative_lines = 0
    proration_lines = 0
    truncated = 0
    missing_period = 0
    zero_length = 0

    for inv in sample:
        lines_obj = inv.get("lines") or {}
        lines = lines_obj.get("data") or []
        if lines_obj.get("has_more"):
            truncated += 1
        if len(lines) > 1:
            multi_line += 1
        for ln in lines:
            per = ln.get("period") or {}
            d = days(per.get("start"), per.get("end"))
            if d is None:
                missing_period += 1
            elif d == 0:
                zero_length += 1
            else:
                period_lengths[d] += 1
            if (ln.get("amount") or 0) < 0:
                negative_lines += 1
            if ln.get("proration"):
                proration_lines += 1

    sub("Service period lengths across {} sampled invoices".format(len(sample)))
    odd = 0
    for d, n in sorted(period_lengths.items()):
        if 27 <= d <= 32:
            tag = "monthly"
        elif 360 <= d <= 372:
            tag = "annual"
        else:
            tag = "<-- IRREGULAR"
            odd += n
        out("  {:>4} days  {:>6} lines   {}".format(d, n, tag))

    sub("Parsing hazards")
    out("  invoices with >1 line        {:>6}".format(multi_line))
    out("  proration lines              {:>6}  (mid-cycle plan changes)".format(proration_lines))
    out("  negative-amount lines        {:>6}  (credits reduce accrual)".format(negative_lines))
    out("  lines with no period         {:>6}".format(missing_period))
    out("  zero-length periods          {:>6}  (no daily rate computable)".format(zero_length))
    out("  irregular-length lines       {:>6}".format(odd))
    out("  invoices with lines.has_more {:>6}  (need line expansion)".format(truncated))


# ---------------------------------------------------------------------------
# 5. Backfill sizing
# ---------------------------------------------------------------------------

def section_backfill():
    rule("5. BACKFILL SIZING (invoices from {})".format(
        FETCH_START.strftime("%Y-%m-%d")))
    if not PERMS.get("Invoice"):
        out("Skipped -- no read access to Invoice.")
        return

    out("Counting what the real collector must pull. This is the slow part.")

    by_month = Counter()
    paid_by_month = Counter()
    status_counts = Counter()
    crosses_ledger_start = 0
    scanned = 0
    earliest = None
    ledger_start_ts = int(LEDGER_START.timestamp())

    it = stripe.Invoice.list(
        limit=100,
        created={"gte": int(FETCH_START.timestamp())},
    ).auto_paging_iter()

    for raw in it:
        inv = plain(raw)
        scanned += 1
        created = inv.get("created")
        if created and (earliest is None or created < earliest):
            earliest = created
        if created:
            mk = month_key(created)
            by_month[mk] += 1
            paid_by_month[mk] += inv.get("amount_paid") or 0
        status_counts[inv.get("status")] += 1

        for ln in ((inv.get("lines") or {}).get("data") or []):
            per = ln.get("period") or {}
            if (per.get("end") or 0) > ledger_start_ts:
                crosses_ledger_start += 1
                break

        if scanned % 2000 == 0:
            sys.stderr.write("  ...scanned {} invoices\n".format(scanned))
        if scanned >= MAX_INVOICES:
            out("!! Hit MAX_INVOICES cap of {}. Counts below are partial.".format(
                MAX_INVOICES))
            break

    sub("Invoice volume by creation month")
    out("{:<10} {:>8} {:>14}".format("MONTH", "INVOICES", "AMOUNT PAID"))
    for mk in sorted(by_month):
        out("{:<10} {:>8} {:>14}".format(mk, by_month[mk], money(paid_by_month[mk])))

    sub("Totals")
    out("  invoices scanned                  {:>8}".format(scanned))
    out("  earliest in window                {:>8}".format(ts(earliest)))
    out("  with service crossing {}    {:>8}".format(
        LEDGER_START.strftime("%Y-%m-%d"), crosses_ledger_start))
    out()
    out("  Those crossing invoices build the opening deferred balance at")
    out("  {}. Skip them and that revenue vanishes from 2026.".format(
        LEDGER_START.strftime("%Y-%m-%d")))

    sub("Invoice status distribution")
    for status, n in status_counts.most_common():
        out("  {:<20} {:>8}".format(str(status), n))


# ---------------------------------------------------------------------------
# 6. Processing fees
# ---------------------------------------------------------------------------

def section_fees():
    rule("6. PROCESSING FEES")
    if not PERMS.get("BalanceTransaction"):
        out("Skipped -- no read access to BalanceTransaction.")
        out("Gross revenue is unaffected; only the net-of-fees line needs this.")
        return

    out("Needed for the secondary net-of-fees line.")

    txns = [plain(t) for t in islice(
        stripe.BalanceTransaction.list(limit=100, type="charge").auto_paging_iter(), 500)]
    if not txns:
        out("No charge balance transactions returned.")
        return

    fee_types = Counter()
    rates = []
    for t in txns:
        amount = t.get("amount") or 0
        fee = t.get("fee") or 0
        if amount > 0:
            rates.append(fee / float(amount))
        for fd in (t.get("fee_details") or []):
            fee_types[fd.get("type")] += 1

    rates.sort()
    sub("Effective fee rate across {} charges".format(len(rates)))
    if rates:
        def pct(x):
            return "{:.2f}%".format(x * 100)
        out("  min     {}".format(pct(rates[0])))
        out("  median  {}".format(pct(rates[len(rates) // 2])))
        out("  mean    {}".format(pct(sum(rates) / len(rates))))
        out("  max     {}".format(pct(rates[-1])))

    sub("Fee component types")
    for ft, n in fee_types.most_common():
        out("  {:<24} {:>6}".format(str(ft), n))


# ---------------------------------------------------------------------------
# 7. Refunds and credit notes
# ---------------------------------------------------------------------------

def section_refunds():
    rule("7. REFUNDS AND CREDIT NOTES")
    if not PERMS.get("Refund"):
        out("Skipped -- no read access to Refund.")
        return

    out("Refunds are the only thing that can reduce already-earned accrual.")

    refunds = [plain(r) for r in islice(stripe.Refund.list(
        limit=100,
        created={"gte": int(FETCH_START.timestamp())},
    ).auto_paging_iter(), 2000)]

    by_month = Counter()
    amount_by_month = Counter()
    reasons = Counter()
    for r in refunds:
        created = r.get("created")
        if not created:
            continue
        mk = month_key(created)
        by_month[mk] += 1
        amount_by_month[mk] += r.get("amount") or 0
        reasons[r.get("reason")] += 1

    sub("Refunds by month")
    out("{:<10} {:>8} {:>14}".format("MONTH", "COUNT", "AMOUNT"))
    for mk in sorted(by_month):
        out("{:<10} {:>8} {:>14}".format(mk, by_month[mk], money(amount_by_month[mk])))

    sub("Stated reasons")
    for reason, n in reasons.most_common():
        out("  {:<24} {:>6}".format(str(reason), n))

    if PERMS.get("CreditNote"):
        notes = [plain(n) for n in islice(
            stripe.CreditNote.list(limit=100).auto_paging_iter(), 500)]
        sub("Credit notes")
        out("  count in sample  {}".format(len(notes)))
        out("  (credit notes carry their own line periods and must be netted")
        out("   against accrual rather than treated as refunds)")


# ---------------------------------------------------------------------------

def main():
    out("BD REVENUE -- STRIPE STRUCTURE REPORT  v2")
    out("Generated {}".format(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
    out("Read-only. Scrubbed of names, emails, and customer identifiers.")

    guard("Permissions probe", section_probe)
    guard("Environment", section_env)
    guard("Subscriptions", section_subscriptions)
    guard("Prices", section_prices)
    guard("Line item shape", section_line_shape)
    guard("Backfill sizing", section_backfill)
    guard("Fees", section_fees)
    guard("Refunds", section_refunds)

    rule("END OF REPORT")
    out("Paste this file back into the conversation to unblock the build.")

    with open(OUTFILE, "w") as fh:
        fh.write("\n".join(_buffer) + "\n")
    sys.stderr.write("\nWrote {} ({} lines)\n".format(OUTFILE, len(_buffer)))
    if QUIET:
        sys.stderr.write("Quiet mode: report withheld from log, see artifact.\n")


if __name__ == "__main__":
    main()
