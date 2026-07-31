#!/usr/bin/env python3
"""
BD Revenue -- Stripe structure validator (READ ONLY)

Makes only GET requests. Writes stripe_structure_report.txt and prints the
same content to screen. Output is scrubbed of customer names, emails, and
customer/charge/invoice IDs so the report can be pasted back verbatim.

Usage:
     export STRIPE_KEY=rk_live_...             (leading space = stays out of history)
    python validate.py

Optional:
    MAX_INVOICES=5000 python validate.py       (cap the backfill scan; default 40000)

Python 3.7+ compatible.
"""

import os
import re
import sys
from collections import Counter, defaultdict
from itertools import islice
from datetime import datetime, timezone

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

# The accrual ledger begins here. Anything earned before this date is discarded,
# but invoices whose service period crosses this line still matter -- their
# unearned remainder IS the opening deferred balance.
LEDGER_START = datetime(2026, 1, 1, tzinfo=timezone.utc)

# How far back the real collector will need to fetch to catch service periods
# that spill into 2026. One year of annual subscriptions plus a month of slack.
FETCH_START = datetime(2024, 12, 1, tzinfo=timezone.utc)

MAX_INVOICES = int(os.environ.get("MAX_INVOICES", "40000"))

_buffer = []


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


_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_IDS = re.compile(r"\b(cus|ch|in|pi|py|txn|card|pm|re)_[A-Za-z0-9]+")


def scrub(value):
    """Strip emails and customer/charge/invoice identifiers from free text."""
    if value is None:
        return ""
    text = str(value)
    text = _EMAIL.sub("[email]", text)
    text = _IDS.sub(lambda m: m.group(1) + "_[redacted]", text)
    return text[:120]


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


def guard(label, fn):
    """Run a section, reporting permission/API failures instead of crashing."""
    try:
        fn()
    except Exception as exc:
        out()
        out("!! {} failed: {}".format(label, type(exc).__name__))
        out("   {}".format(scrub(exc)))
        out("   (If this is a permissions error, add read access for this")
        out("    resource to the restricted key and re-run.)")


# ---------------------------------------------------------------------------
# 1. Environment
# ---------------------------------------------------------------------------

def section_env():
    rule("1. ENVIRONMENT")
    out("Python              {}".format(sys.version.split()[0]))
    out("stripe SDK          {}".format(getattr(stripe, "VERSION", "unknown")))
    out("Pinned API version  {}".format(getattr(stripe, "api_version", None) or "SDK default"))
    prefix = KEY.split("_")[0] + "_" + KEY.split("_")[1] if KEY.count("_") >= 2 else KEY[:6]
    out("Key type            {} ({})".format(
        prefix,
        "RESTRICTED" if KEY.startswith("rk_") else "FULL SECRET -- consider a restricted key"))
    out("Mode                {}".format("LIVE" if "_live_" in KEY else "TEST"))
    out("Ledger start        {}".format(LEDGER_START.strftime("%Y-%m-%d")))
    out("Collector fetch from  {}".format(FETCH_START.strftime("%Y-%m-%d")))


# ---------------------------------------------------------------------------
# 2. Prices and products -- the real plan taxonomy
# ---------------------------------------------------------------------------

PRICE_INDEX = {}


def section_prices():
    rule("2. PRICES AND PRODUCTS")
    out("This is the plan taxonomy the collector will auto-discover.")

    products = {}
    for prod in stripe.Product.list(limit=100).auto_paging_iter():
        products[prod.id] = prod

    prices = list(stripe.Price.list(limit=100).auto_paging_iter())

    rows = []
    for p in prices:
        rec = p.get("recurring") or {}
        interval = rec.get("interval")
        count = rec.get("interval_count") or 1
        cadence = "one-time"
        if interval:
            cadence = interval if count == 1 else "{}x {}".format(count, interval)
        prod = products.get(p.get("product"))
        rows.append({
            "id": p.id,
            "active": p.get("active"),
            "amount": p.get("unit_amount"),
            "currency": p.get("currency"),
            "cadence": cadence,
            "scheme": p.get("billing_scheme"),
            "tiers": p.get("tiers_mode"),
            "nickname": scrub(p.get("nickname")),
            "product": scrub(prod.get("name") if prod else p.get("product")),
            "meta": dict(p.get("metadata") or {}),
            "prod_meta": dict((prod.get("metadata") or {})) if prod else {},
        })
        PRICE_INDEX[p.id] = rows[-1]

    rows.sort(key=lambda r: (not r["active"], r["cadence"], -(r["amount"] or 0)))

    sub("All prices ({} total, {} active)".format(
        len(rows), sum(1 for r in rows if r["active"])))
    out("{:<30} {:>10} {:<10} {:<4} {}".format("PRICE ID", "AMOUNT", "CADENCE", "ACT", "PRODUCT / NICKNAME"))
    for r in rows:
        label = r["nickname"] or r["product"]
        out("{:<30} {:>10} {:<10} {:<4} {}".format(
            r["id"], money(r["amount"], r["currency"]), r["cadence"],
            "y" if r["active"] else "n", label[:34]))

    sub("Metadata keys in use on prices")
    key_vals = defaultdict(Counter)
    for r in rows:
        for k, v in r["meta"].items():
            key_vals[k][scrub(v)] += 1
    if not key_vals:
        out("(none -- classification must come from amount + interval)")
    for k, counter in key_vals.items():
        if len(counter) <= 20:
            pairs = ", ".join("{}={}".format(val, n) for val, n in counter.most_common())
            out("  {:<24} {}".format(k, pairs))
        else:
            out("  {:<24} {} distinct values (high cardinality, not shown)".format(k, len(counter)))

    sub("Metadata keys in use on products")
    pkey_vals = defaultdict(Counter)
    for r in rows:
        for k, v in r["prod_meta"].items():
            pkey_vals[k][scrub(v)] += 1
    if not pkey_vals:
        out("(none)")
    for k, counter in pkey_vals.items():
        if len(counter) <= 20:
            pairs = ", ".join("{}={}".format(val, n) for val, n in counter.most_common())
            out("  {:<24} {}".format(k, pairs))
        else:
            out("  {:<24} {} distinct values".format(k, len(counter)))

    out()
    out("QUESTION THIS ANSWERS: can corporate be told apart from regular annual")
    out("by metadata, or only by amount threshold?")


# ---------------------------------------------------------------------------
# 3. Subscriptions -- counts, renewal calendar, ladder anomalies
# ---------------------------------------------------------------------------

def section_subscriptions():
    rule("3. SUBSCRIPTIONS")

    by_status = Counter()
    by_price = Counter()
    price_amounts = Counter()
    multi_item = 0
    with_discount = 0
    with_trial = 0
    cancel_at_end = 0
    renewal_month = Counter()
    total = 0

    for s in stripe.Subscription.list(status="all", limit=100).auto_paging_iter():
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

        if s.get("status") in ("active", "trialing", "past_due"):
            for it in items:
                price = it.get("price") or {}
                pid = price.get("id")
                qty = it.get("quantity") or 1
                by_price[pid] += qty
                price_amounts[price.get("unit_amount")] += qty
            cpe = s.get("current_period_end")
            if cpe:
                renewal_month[month_key(cpe)] += 1

    sub("Counts by status")
    for status, n in by_status.most_common():
        out("  {:<20} {:>6}".format(status, n))
    out("  {:<20} {:>6}".format("TOTAL", total))

    sub("Active-equivalent subscriptions by price")
    out("{:<30} {:>10} {:<10} {:>8}".format("PRICE ID", "AMOUNT", "CADENCE", "SUBS"))
    for pid, n in by_price.most_common():
        info = PRICE_INDEX.get(pid, {})
        out("{:<30} {:>10} {:<10} {:>8}".format(
            pid or "(none)",
            money(info.get("amount")),
            info.get("cadence", "?"),
            n))

    sub("Structural flags")
    out("  multi-item subscriptions   {:>6}   (seat-based corporate plans?)".format(multi_item))
    out("  carrying a discount/coupon {:>6}   (affects recognized amount)".format(with_discount))
    out("  with a trial period        {:>6}   (zero-revenue service days)".format(with_trial))
    out("  set to cancel at period end{:>6}   (still accrue until then)".format(cancel_at_end))

    sub("Forward renewal calendar (next 14 months, active subs)")
    for mk in sorted(renewal_month)[:14]:
        n = renewal_month[mk]
        out("  {}  {:>5}  {}".format(mk, n, "#" * min(60, n // 5)))

    sub("Price-point distribution among active subs")
    out("Reveals the annual ladder and any off-ladder pricing.")
    for amount, n in sorted(price_amounts.items(), key=lambda kv: -(kv[0] or 0)):
        out("  {:>10}  {:>6} subs".format(money(amount), n))


# ---------------------------------------------------------------------------
# 4. Invoice line item shape -- the heart of the accrual ledger
# ---------------------------------------------------------------------------

def section_line_shape():
    rule("4. INVOICE LINE ITEM SHAPE")
    out("The accrual ledger is generated from line-level service periods,")
    out("so the exact field layout here determines the collector's parser.")

    sample = list(islice(
        stripe.Invoice.list(limit=100, status="paid").auto_paging_iter(), 300))
    if not sample:
        out("No paid invoices returned.")
        return

    sub("Field keys present on a line item")
    first_line = None
    for inv in sample:
        lines = (inv.get("lines") or {}).get("data") or []
        if lines:
            first_line = lines[0]
            break
    if first_line is None:
        out("No line items found in sample.")
        return
    keys = sorted(first_line.keys())
    for i in range(0, len(keys), 4):
        out("  " + "  ".join("{:<24}".format(k) for k in keys[i:i + 4]))

    probe = ["period", "proration", "type", "parent", "subscription",
             "subscription_item", "price", "plan", "pricing", "amount",
             "quantity", "discount_amounts", "tax_amounts", "currency"]
    sub("Presence of fields the collector depends on")
    for field in probe:
        present = field in first_line
        out("  {:<22} {}".format(field, "present" if present else "ABSENT"))
    out()
    out("NOTE: recent API versions replaced `type` and `plan` with `parent`")
    out("and `pricing`. The above tells me which dialect this account speaks.")

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
    expected_monthly = range(27, 33)
    expected_annual = range(360, 372)
    odd = 0
    for d, n in sorted(period_lengths.items()):
        tag = ""
        if d in expected_monthly:
            tag = "monthly"
        elif d in expected_annual:
            tag = "annual"
        else:
            tag = "<-- IRREGULAR"
            odd += n
        out("  {:>4} days  {:>6} lines   {}".format(d, n, tag))

    sub("Parsing hazards")
    out("  invoices with >1 line       {:>6}".format(multi_line))
    out("  proration lines             {:>6}  (mid-cycle plan changes)".format(proration_lines))
    out("  negative-amount lines       {:>6}  (credits -- must reduce accrual)".format(negative_lines))
    out("  lines with no period        {:>6}".format(missing_period))
    out("  zero-length periods         {:>6}  (cannot compute a daily rate)".format(zero_length))
    out("  irregular-length lines      {:>6}".format(odd))
    out("  invoices with lines.has_more{:>6}  (need explicit line expansion)".format(truncated))


# ---------------------------------------------------------------------------
# 5. Backfill sizing
# ---------------------------------------------------------------------------

def section_backfill():
    rule("5. BACKFILL SIZING (invoices from {})".format(FETCH_START.strftime("%Y-%m-%d")))
    out("Counting what the real collector will have to pull. This is the slow part.")

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

    for inv in it:
        scanned += 1
        created = inv.get("created")
        if earliest is None or created < earliest:
            earliest = created
        mk = month_key(created)
        by_month[mk] += 1
        status_counts[inv.get("status")] += 1
        paid_by_month[mk] += inv.get("amount_paid") or 0

        for ln in ((inv.get("lines") or {}).get("data") or []):
            per = ln.get("period") or {}
            if (per.get("end") or 0) > ledger_start_ts:
                crosses_ledger_start += 1
                break

        if scanned % 2000 == 0:
            sys.stderr.write("  ...scanned {} invoices\n".format(scanned))
        if scanned >= MAX_INVOICES:
            out("!! Hit MAX_INVOICES cap of {}. Counts below are partial.".format(MAX_INVOICES))
            break

    sub("Invoice volume by creation month")
    out("{:<10} {:>8} {:>14}".format("MONTH", "INVOICES", "AMOUNT PAID"))
    for mk in sorted(by_month):
        out("{:<10} {:>8} {:>14}".format(mk, by_month[mk], money(paid_by_month[mk])))

    sub("Totals")
    out("  invoices scanned                    {:>8}".format(scanned))
    out("  earliest in window                  {:>8}".format(ts(earliest)))
    out("  with service crossing {}      {:>8}".format(
        LEDGER_START.strftime("%Y-%m-%d"), crosses_ledger_start))
    out()
    out("  Those crossing invoices are what build the opening deferred")
    out("  balance at {}. If the collector skipped them, that".format(LEDGER_START.strftime("%Y-%m-%d")))
    out("  revenue would vanish from 2026 entirely.")

    sub("Invoice status distribution")
    for status, n in status_counts.most_common():
        out("  {:<20} {:>8}".format(status or "(none)", n))


# ---------------------------------------------------------------------------
# 6. Processing fees
# ---------------------------------------------------------------------------

def section_fees():
    rule("6. PROCESSING FEES")
    out("Needed for the secondary net-of-fees line.")

    txns = list(islice(stripe.BalanceTransaction.list(
        limit=100, type="charge").auto_paging_iter(), 500))
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
        out("  {:<24} {:>6}".format(ft or "(none)", n))

    sample = txns[0]
    sub("Example charge transaction (scrubbed)")
    out("  gross   {}".format(money(sample.get("amount"))))
    out("  fee     {}".format(money(sample.get("fee"))))
    out("  net     {}".format(money(sample.get("net"))))
    out("  date    {}".format(ts(sample.get("created"))))


# ---------------------------------------------------------------------------
# 7. Refunds and credit notes
# ---------------------------------------------------------------------------

def section_refunds():
    rule("7. REFUNDS AND CREDIT NOTES")
    out("Refunds are the only thing that can reduce already-earned accrual.")

    refunds = list(islice(stripe.Refund.list(
        limit=100,
        created={"gte": int(FETCH_START.timestamp())},
    ).auto_paging_iter(), 2000))

    by_month = Counter()
    amount_by_month = Counter()
    reasons = Counter()
    for r in refunds:
        mk = month_key(r.get("created"))
        by_month[mk] += 1
        amount_by_month[mk] += r.get("amount") or 0
        reasons[r.get("reason")] += 1

    sub("Refunds by month")
    out("{:<10} {:>8} {:>14}".format("MONTH", "COUNT", "AMOUNT"))
    for mk in sorted(by_month):
        out("{:<10} {:>8} {:>14}".format(mk, by_month[mk], money(amount_by_month[mk])))

    sub("Stated reasons")
    for reason, n in reasons.most_common():
        out("  {:<24} {:>6}".format(reason or "(none given)", n))

    try:
        notes = list(islice(stripe.CreditNote.list(limit=100).auto_paging_iter(), 500))
        sub("Credit notes")
        out("  count in sample  {}".format(len(notes)))
        out("  (credit notes carry their own line periods and must be")
        out("   netted against accrual rather than treated as refunds)")
    except Exception:
        out()
        out("Credit note access not permitted by this key -- not critical.")


# ---------------------------------------------------------------------------

def main():
    out("BD REVENUE -- STRIPE STRUCTURE REPORT")
    out("Generated {}".format(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
    out("Read-only. Scrubbed of names, emails, and customer identifiers.")

    guard("Environment", section_env)
    guard("Prices", section_prices)
    guard("Subscriptions", section_subscriptions)
    guard("Line item shape", section_line_shape)
    guard("Backfill sizing", section_backfill)
    guard("Fees", section_fees)
    guard("Refunds", section_refunds)

    rule("END OF REPORT")
    out("Paste this file back into the conversation to unblock the collector build.")

    with open(OUTFILE, "w") as fh:
        fh.write("\n".join(_buffer) + "\n")
    sys.stderr.write("\nWrote {} ({} lines)\n".format(OUTFILE, len(_buffer)))
    if QUIET:
        sys.stderr.write("Quiet mode: report withheld from log, see artifact.\n")


if __name__ == "__main__":
    main()
