#!/usr/bin/env python3
"""
Adrian Danila — Rehab PO Log Database Manager

Local JSON database for Purchase Orders, invoices, vendors, properties, and budgets.
Provides CRUD operations and budget calculation functions.

Usage:
    python clients/adrian/execution/adrian_db_manager.py --demo          # Load sample data
    python clients/adrian/execution/adrian_db_manager.py --summary       # Budget summary
    python clients/adrian/execution/adrian_db_manager.py --status        # All PO statuses
    python clients/adrian/execution/adrian_db_manager.py --add-po        # Interactive PO creation
"""

import sys
import json
import os
import argparse
from pathlib import Path
from datetime import datetime
from copy import deepcopy

# Fix Windows encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
MAIN_ROOT = PROJECT_ROOT.parent.parent
load_dotenv(MAIN_ROOT / ".env")

DB_PATH = PROJECT_ROOT / "data" / "po_database.json"
SAMPLE_DATA_PATH = PROJECT_ROOT / "resources" / "sample_po_data.json"

# Default variance threshold (%)
VARIANCE_THRESHOLD = 10.0

# Backend mode: "json" (default) or "supabase"
_BACKEND = os.getenv("ADRIAN_DB_BACKEND", "json")
_supabase_client = None


def _get_supabase():
    """Lazy-init Supabase client."""
    global _supabase_client
    if _supabase_client is None:
        from supabase import create_client
        url = os.getenv("SUPABASE_URL_ADRIAN")
        key = os.getenv("SUPABASE_KEY_ADRIAN")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL_ADRIAN / SUPABASE_KEY_ADRIAN not set in .env")
        _supabase_client = create_client(url, key)
    return _supabase_client


def set_backend(backend: str):
    """Set database backend: 'json' or 'supabase'."""
    global _BACKEND
    _BACKEND = backend


# ── Supabase DB Operations ────────────────────────────────────

def load_db_supabase() -> dict:
    """Load all data from Supabase into the same dict format as JSON."""
    sb = _get_supabase()
    db = get_empty_db()
    db["properties"] = sb.table("properties").select("*").execute().data
    db["vendors"] = sb.table("vendors").select("*").execute().data
    db["budget_categories"] = sb.table("budget_categories").select("*").execute().data

    # Convert Supabase numeric strings to floats
    for cat in db["budget_categories"]:
        cat["budgeted"] = float(cat.get("budgeted", 0))

    pos = sb.table("purchase_orders").select("*").execute().data
    for po in pos:
        po["amount"] = float(po.get("amount", 0))
        if po.get("invoice_amount") is not None:
            po["invoice_amount"] = float(po["invoice_amount"])
    db["purchase_orders"] = pos

    invs = sb.table("invoices_unmatched").select("*").execute().data
    for inv in invs:
        inv["amount"] = float(inv.get("amount", 0))
        if isinstance(inv.get("line_items"), str):
            inv["line_items"] = json.loads(inv["line_items"])
    db["invoices_unmatched"] = invs

    logs = sb.table("activity_log").select("*").order("timestamp", desc=True).limit(100).execute().data
    db["activity_log"] = logs

    return db


def save_db_supabase(db: dict):
    """Write changes back to Supabase (insert-or-skip POs, invoices)."""
    sb = _get_supabase()

    # Get existing keys to avoid duplicates
    existing_pos = {r["po_number"] for r in sb.table("purchase_orders").select("po_number").execute().data}
    existing_invs = {r["invoice_number"] for r in sb.table("invoices_unmatched").select("invoice_number").execute().data}

    # Insert new purchase orders, update existing ones
    for po in db["purchase_orders"]:
        row = {k: v for k, v in po.items() if k not in ("id", "created_at")}
        if po["po_number"] in existing_pos:
            sb.table("purchase_orders").update(row).eq("po_number", po["po_number"]).execute()
        else:
            sb.table("purchase_orders").insert(row).execute()

    # Insert new invoices, update existing ones
    for inv in db["invoices_unmatched"]:
        row = {k: v for k, v in inv.items() if k not in ("id", "created_at")}
        if isinstance(row.get("line_items"), list):
            row["line_items"] = json.dumps(row["line_items"])
        if inv["invoice_number"] in existing_invs:
            sb.table("invoices_unmatched").update(row).eq("invoice_number", inv["invoice_number"]).execute()
        else:
            sb.table("invoices_unmatched").insert(row).execute()


def log_activity_supabase(event_type: str, po_number: str | None, message: str):
    """Insert a single activity log entry to Supabase."""
    sb = _get_supabase()
    sb.table("activity_log").insert({
        "type": event_type,
        "po_number": po_number,
        "message": message,
    }).execute()


def get_empty_db():
    return {
        "properties": [],
        "vendors": [],
        "budget_categories": [],
        "purchase_orders": [],
        "invoices_unmatched": [],
        "activity_log": [],
        "metadata": {
            "created": datetime.now().isoformat(),
            "last_modified": datetime.now().isoformat(),
            "version": "2.0",
            "variance_threshold_pct": VARIANCE_THRESHOLD,
        },
    }


def load_db() -> dict:
    if _BACKEND == "supabase":
        return load_db_supabase()
    if DB_PATH.exists():
        with open(DB_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return get_empty_db()


def save_db(db: dict):
    if _BACKEND == "supabase":
        save_db_supabase(db)
        return
    db["metadata"]["last_modified"] = datetime.now().isoformat()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)


def load_sample_data() -> dict:
    """Load sample PO data from resources for demo mode."""
    if not SAMPLE_DATA_PATH.exists():
        print(f"[ERROR] Sample data not found: {SAMPLE_DATA_PATH}")
        sys.exit(1)
    with open(SAMPLE_DATA_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    db = get_empty_db()
    db["properties"] = data.get("properties", [])
    db["vendors"] = data.get("vendors", [])
    db["budget_categories"] = data.get("budget_categories", [])
    db["purchase_orders"] = data.get("purchase_orders", [])
    db["invoices_unmatched"] = data.get("invoices_unmatched", [])
    db["activity_log"] = data.get("activity_log", [])
    return db


# ── PO Operations ──────────────────────────────────────────────

def get_next_po_number(db: dict, property_id: str) -> str:
    """Generate next sequential PO number for a property."""
    prefix_map = {
        "PROP001": "SRA",
    }
    prefix = prefix_map.get(property_id, "PO")
    existing = [
        po for po in db["purchase_orders"]
        if po["po_number"].startswith(prefix)
    ]
    # Find highest number in existing POs
    max_num = 0
    for po in existing:
        try:
            num = int(po["po_number"][len(prefix):])
            max_num = max(max_num, num)
        except ValueError:
            pass
    return f"{prefix}{max_num + 1:03d}"


def create_po(db: dict, property_id: str, vendor_id: str, description: str,
              category: str, amount: float, notes: str = "") -> dict:
    """Create a new Purchase Order."""
    po_number = get_next_po_number(db, property_id)
    po = {
        "po_number": po_number,
        "property_id": property_id,
        "vendor_id": vendor_id,
        "description": description,
        "category": category,
        "amount": amount,
        "date_created": datetime.now().strftime("%Y-%m-%d"),
        "date_due": None,
        "status": "pending",
        "invoice_number": None,
        "invoice_amount": None,
        "date_paid": None,
        "notes": notes,
    }
    db["purchase_orders"].append(po)
    log_activity(db, "po_created", po_number,
                 f"New PO {po_number} created: {description} (${amount:,.2f})")
    return po


def get_po(db: dict, po_number: str) -> dict | None:
    """Get a PO by number."""
    for po in db["purchase_orders"]:
        if po["po_number"] == po_number:
            return po
    return None


def update_po_status(db: dict, po_number: str, status: str, **kwargs) -> bool:
    """Update PO status and optional fields."""
    po = get_po(db, po_number)
    if not po:
        return False
    po["status"] = status
    for k, v in kwargs.items():
        if k in po:
            po[k] = v
    log_activity(db, "status_update", po_number,
                 f"PO {po_number} status changed to {status}")
    return True


def process_invoice(db: dict, po_number: str, invoice_number: str,
                    invoice_amount: float) -> dict:
    """Process an invoice against a PO. Returns result with variance info."""
    po = get_po(db, po_number)
    if not po:
        return {"success": False, "error": f"PO {po_number} not found"}

    po["invoice_number"] = invoice_number
    po["invoice_amount"] = invoice_amount

    variance = invoice_amount - po["amount"]
    variance_pct = (variance / po["amount"]) * 100 if po["amount"] > 0 else 0

    threshold = db["metadata"].get("variance_threshold_pct", VARIANCE_THRESHOLD)

    result = {
        "success": True,
        "po_number": po_number,
        "po_amount": po["amount"],
        "invoice_amount": invoice_amount,
        "variance": variance,
        "variance_pct": variance_pct,
        "exceeds_threshold": abs(variance_pct) > threshold,
    }

    if abs(variance_pct) > threshold:
        po["status"] = "variance"
        po["notes"] = (
            f"VARIANCE: Invoice ${variance:+,.2f} ({variance_pct:+.1f}%) "
            f"from PO. Exceeds {threshold}% threshold."
        )
        log_activity(db, "variance_alert", po_number,
                     f"VARIANCE ALERT: Invoice {invoice_number} is "
                     f"${abs(variance):,.2f} ({abs(variance_pct):.1f}%) "
                     f"{'over' if variance > 0 else 'under'} PO {po_number}")
    elif abs(variance) < 0.01:
        po["status"] = "received"
        log_activity(db, "invoice_received", po_number,
                     f"Invoice {invoice_number} received for PO {po_number} "
                     f"(${invoice_amount:,.2f} — matches PO)")
    else:
        po["status"] = "received"
        log_activity(db, "invoice_received", po_number,
                     f"Invoice {invoice_number} received for PO {po_number} "
                     f"(${invoice_amount:,.2f}, variance ${variance:+,.2f} "
                     f"within threshold)")

    return result


def mark_paid(db: dict, po_number: str) -> bool:
    """Mark a PO as paid."""
    po = get_po(db, po_number)
    if not po:
        return False
    po["status"] = "paid"
    po["date_paid"] = datetime.now().strftime("%Y-%m-%d")
    log_activity(db, "payment_processed", po_number,
                 f"PO {po_number} marked as paid "
                 f"(${po.get('invoice_amount') or po['amount']:,.2f})")
    return True


# ── Budget Calculations ────────────────────────────────────────

def get_budget_summary(db: dict, property_id: str = None) -> list[dict]:
    """Calculate budget vs actual for each category."""
    summaries = []
    categories = db.get("budget_categories", [])

    if property_id:
        categories = [c for c in categories if c["property_id"] == property_id]

    for cat in categories:
        prop_id = cat["property_id"]
        cat_code = cat["code"]
        budgeted = cat["budgeted"]

        # Sum PO amounts for this category + property
        pos = [
            po for po in db["purchase_orders"]
            if po["property_id"] == prop_id and po["category"] == cat_code
        ]
        committed = sum(po["amount"] for po in pos)
        actual_paid = sum(
            (po.get("invoice_amount") or po["amount"])
            for po in pos if po["status"] == "paid"
        )
        pending = sum(
            po["amount"] for po in pos
            if po["status"] in ("pending", "in_progress", "received")
        )
        variance_flagged = sum(
            (po.get("invoice_amount") or po["amount"])
            for po in pos if po["status"] == "variance"
        )

        pct_used = (committed / budgeted * 100) if budgeted > 0 else 0

        summaries.append({
            "property_id": prop_id,
            "property_name": get_property_name(db, prop_id),
            "category_code": cat_code,
            "category_name": cat["name"],
            "budgeted": budgeted,
            "committed": committed,
            "actual_paid": actual_paid,
            "pending": pending,
            "variance_flagged": variance_flagged,
            "remaining": budgeted - committed,
            "pct_used": pct_used,
            "over_budget": committed > budgeted,
        })

    return summaries


def get_property_summary(db: dict) -> list[dict]:
    """High-level budget summary per property."""
    summaries = []
    for prop in db.get("properties", []):
        cat_summaries = get_budget_summary(db, prop["id"])
        total_budget = sum(c["budgeted"] for c in cat_summaries)
        total_committed = sum(c["committed"] for c in cat_summaries)
        total_paid = sum(c["actual_paid"] for c in cat_summaries)
        total_pending = sum(c["pending"] for c in cat_summaries)
        over_budget_cats = [c for c in cat_summaries if c["over_budget"]]
        variance_pos = [
            po for po in db["purchase_orders"]
            if po["property_id"] == prop["id"] and po["status"] == "variance"
        ]

        summaries.append({
            "property_id": prop["id"],
            "property_name": prop["name"],
            "total_budget": total_budget,
            "total_committed": total_committed,
            "total_paid": total_paid,
            "total_pending": total_pending,
            "remaining": total_budget - total_committed,
            "pct_used": (total_committed / total_budget * 100) if total_budget > 0 else 0,
            "categories_over_budget": len(over_budget_cats),
            "variance_alerts": len(variance_pos),
            "total_pos": len([
                po for po in db["purchase_orders"]
                if po["property_id"] == prop["id"]
            ]),
        })

    return summaries


# ── Lookup Helpers ─────────────────────────────────────────────

def get_property_name(db: dict, property_id: str) -> str:
    for p in db.get("properties", []):
        if p["id"] == property_id:
            return p["name"]
    return property_id


def get_vendor_name(db: dict, vendor_id: str) -> str:
    for v in db.get("vendors", []):
        if v["id"] == vendor_id:
            return v["name"]
    return vendor_id


def get_vendor_by_name(db: dict, name: str) -> dict | None:
    name_lower = name.lower()
    for v in db.get("vendors", []):
        if name_lower in v["name"].lower():
            return v
    return None


def get_property_by_name(db: dict, name: str) -> dict | None:
    name_lower = name.lower()
    for p in db.get("properties", []):
        if name_lower in p["name"].lower():
            return p
    return None


# ── Unmatched Invoice Processing ───────────────────────────────

def get_unmatched_invoices(db: dict) -> list[dict]:
    """Get all unmatched invoices."""
    return [inv for inv in db.get("invoices_unmatched", []) if inv["status"] == "unmatched"]


def get_matched_invoices(db: dict) -> list[dict]:
    """Get invoices that already have PO references."""
    return [inv for inv in db.get("invoices_unmatched", []) if inv["status"] == "matched"]


def auto_create_po_for_invoice(db: dict, invoice: dict) -> dict:
    """Create a PO from an unmatched invoice and link them."""
    vendor_id = invoice.get("vendor_id", "NEW")
    category = invoice.get("category_guess", "MU")
    po = create_po(
        db,
        property_id="PROP001",
        vendor_id=vendor_id,
        description=invoice["description"],
        category=category,
        amount=invoice["amount"],
        notes=f"Auto-created from invoice {invoice['invoice_number']}",
    )
    # Link invoice to new PO
    invoice["po_reference"] = po["po_number"]
    invoice["status"] = "auto_matched"
    log_activity(db, "auto_po_created", po["po_number"],
                 f"Auto-created PO {po['po_number']} from invoice "
                 f"{invoice['invoice_number']} (${invoice['amount']:,.2f})")
    return po


def process_matched_invoice(db: dict, invoice: dict) -> dict:
    """Process an invoice that already has a PO reference."""
    po_ref = invoice.get("po_reference")
    if not po_ref:
        return {"success": False, "error": "No PO reference"}
    result = process_invoice(db, po_ref, invoice["invoice_number"], invoice["amount"])
    if result["success"]:
        invoice["status"] = "processed"
    return result


def process_all_invoices(db: dict) -> dict:
    """Process all unmatched invoices: match existing refs, auto-create POs for rest."""
    results = {"matched": [], "auto_created": [], "errors": []}

    for inv in db.get("invoices_unmatched", []):
        if inv["status"] in ("processed", "auto_matched"):
            continue

        if inv.get("po_reference") and inv["status"] == "matched":
            # Has a PO ref — process against existing PO
            result = process_matched_invoice(db, inv)
            if result.get("success"):
                results["matched"].append({
                    "invoice": inv["invoice_number"],
                    "po": inv["po_reference"],
                    "amount": inv["amount"],
                    "variance": result.get("variance", 0),
                    "variance_pct": result.get("variance_pct", 0),
                })
            else:
                results["errors"].append({
                    "invoice": inv["invoice_number"],
                    "error": result.get("error", "Unknown"),
                })
        elif inv["status"] == "unmatched":
            # No PO ref — auto-create PO
            po = auto_create_po_for_invoice(db, inv)
            results["auto_created"].append({
                "invoice": inv["invoice_number"],
                "po": po["po_number"],
                "vendor": inv["vendor"],
                "amount": inv["amount"],
                "category": inv.get("category_guess", "?"),
            })

    return results


# ── Activity Log ───────────────────────────────────────────────

def log_activity(db: dict, event_type: str, po_number: str | None, message: str):
    db["activity_log"].append({
        "timestamp": datetime.now().isoformat(),
        "type": event_type,
        "po_number": po_number,
        "message": message,
    })
    if _BACKEND == "supabase":
        try:
            log_activity_supabase(event_type, po_number, message)
        except Exception:
            pass  # already in local list, don't break on network error


def get_recent_activity(db: dict, limit: int = 20) -> list[dict]:
    return sorted(
        db.get("activity_log", []),
        key=lambda x: x["timestamp"],
        reverse=True,
    )[:limit]


# ── CLI ────────────────────────────────────────────────────────

def print_po_table(db: dict):
    """Print all POs in a formatted table."""
    pos = db.get("purchase_orders", [])
    if not pos:
        print("No purchase orders found.")
        return

    print(f"\n{'PO #':<10} {'Property':<22} {'Vendor':<22} {'Category':<6} "
          f"{'Amount':>12} {'Invoice':>12} {'Status':<12}")
    print("-" * 100)

    for po in sorted(pos, key=lambda x: x["po_number"]):
        prop = get_property_name(db, po["property_id"])[:20]
        vendor = get_vendor_name(db, po["vendor_id"])[:20]
        inv_amt = f"${po['invoice_amount']:,.0f}" if po.get("invoice_amount") else "—"
        status_icon = {
            "paid": "[PAID]",
            "pending": "[PEND]",
            "in_progress": "[WIP]",
            "received": "[RCVD]",
            "variance": "[VAR!]",
        }.get(po["status"], po["status"])

        print(f"{po['po_number']:<10} {prop:<22} {vendor:<22} {po['category']:<6} "
              f"${po['amount']:>11,.0f} {inv_amt:>12} {status_icon:<12}")

    total = sum(po["amount"] for po in pos)
    print("-" * 100)
    print(f"{'TOTAL':>60} ${total:>11,.0f}")
    print(f"\nTotal POs: {len(pos)} | "
          f"Paid: {sum(1 for p in pos if p['status'] == 'paid')} | "
          f"Pending: {sum(1 for p in pos if p['status'] in ('pending', 'in_progress'))} | "
          f"Variance: {sum(1 for p in pos if p['status'] == 'variance')}")


def print_budget_summary(db: dict):
    """Print budget vs actual summary."""
    for prop_sum in get_property_summary(db):
        print(f"\n{'=' * 70}")
        print(f"  {prop_sum['property_name']}")
        print(f"{'=' * 70}")
        print(f"  Total Budget:    ${prop_sum['total_budget']:>12,.0f}")
        print(f"  Committed:       ${prop_sum['total_committed']:>12,.0f}  "
              f"({prop_sum['pct_used']:.1f}%)")
        print(f"  Paid:            ${prop_sum['total_paid']:>12,.0f}")
        print(f"  Pending:         ${prop_sum['total_pending']:>12,.0f}")
        print(f"  Remaining:       ${prop_sum['remaining']:>12,.0f}")

        if prop_sum["categories_over_budget"] > 0:
            print(f"  ⚠ {prop_sum['categories_over_budget']} categories OVER BUDGET")
        if prop_sum["variance_alerts"] > 0:
            print(f"  ⚠ {prop_sum['variance_alerts']} variance alerts")

        # Category breakdown
        cat_sums = get_budget_summary(db, prop_sum["property_id"])
        if cat_sums:
            print(f"\n  {'Category':<22} {'Budget':>10} {'Committed':>10} "
                  f"{'Remaining':>10} {'Used':>7}")
            print(f"  {'-' * 62}")
            for cs in sorted(cat_sums, key=lambda x: x["pct_used"], reverse=True):
                flag = " !!" if cs["over_budget"] else ""
                print(f"  {cs['category_name']:<22} ${cs['budgeted']:>9,.0f} "
                      f"${cs['committed']:>9,.0f} ${cs['remaining']:>9,.0f} "
                      f"{cs['pct_used']:>6.1f}%{flag}")


def main():
    parser = argparse.ArgumentParser(description="Adrian PO Log Database Manager")
    parser.add_argument("--demo", action="store_true",
                        help="Load sample data for demo mode")
    parser.add_argument("--summary", action="store_true",
                        help="Print budget summary")
    parser.add_argument("--status", action="store_true",
                        help="Print all PO statuses")
    parser.add_argument("--activity", action="store_true",
                        help="Print recent activity")
    parser.add_argument("--property", type=str,
                        help="Filter by property ID (e.g., PROP001)")
    parser.add_argument("--backend", type=str, default="json",
                        choices=["json", "supabase"],
                        help="Database backend (default: json)")
    args = parser.parse_args()

    set_backend(args.backend)

    if args.demo:
        print("[DEMO] Loading sample PO data...")
        db = load_sample_data()
        save_db(db)
        unmatched = len(get_unmatched_invoices(db))
        matched = len(get_matched_invoices(db))
        print(f"[OK] Loaded {len(db['purchase_orders'])} POs, "
              f"{len(db['vendors'])} vendors, "
              f"{len(db['properties'])} properties")
        print(f"[OK] {unmatched} unmatched invoices, {matched} pre-matched invoices")
        print(f"[OK] Database saved to: {DB_PATH}")
        print_po_table(db)
        print_budget_summary(db)
        return

    db = load_db()

    if not db.get("purchase_orders"):
        print("[INFO] Database is empty. Run with --demo to load sample data.")
        return

    if args.status:
        print_po_table(db)
    elif args.summary:
        print_budget_summary(db)
    elif args.activity:
        print("\nRecent Activity:")
        print("-" * 80)
        for entry in get_recent_activity(db):
            ts = entry["timestamp"][:16].replace("T", " ")
            print(f"  [{ts}] {entry['message']}")
    else:
        print_po_table(db)
        print_budget_summary(db)


if __name__ == "__main__":
    main()
