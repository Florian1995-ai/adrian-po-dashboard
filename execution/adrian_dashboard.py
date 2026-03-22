#!/usr/bin/env python3
"""
Adrian Danila — Rehab PO Log Dashboard (Streamlit)

Polished web portal for property management PO tracking.
Budget vs actual charts, variance alerts, activity feed, PO status table.

Usage:
    streamlit run clients/adrian/execution/adrian_dashboard.py
    streamlit run clients/adrian/execution/adrian_dashboard.py -- --demo
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = Path(__file__).parent.parent
MAIN_ROOT = PROJECT_ROOT.parent.parent

sys.path.insert(0, str(PROJECT_ROOT / "execution"))

import streamlit as st
import pandas as pd

from adrian_db_manager import (
    load_db, save_db, load_sample_data, set_backend,
    get_property_summary, get_budget_summary, get_recent_activity,
    get_property_name, get_vendor_name, create_po, process_invoice,
    get_unmatched_invoices, get_matched_invoices, process_all_invoices,
)

# ── Page Config ────────────────────────────────────────────────

st.set_page_config(
    page_title="Rehab PO Log — Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────

st.markdown("""
<style>
    .main { padding-top: 1rem; }
    .stMetric { background: #f8f9fa; padding: 12px; border-radius: 8px; border-left: 4px solid #0d6efd; }
    .variance-alert { background: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; border-radius: 4px; margin: 4px 0; }
    .over-budget { background: #f8d7da; border-left: 4px solid #dc3545; padding: 12px; border-radius: 4px; margin: 4px 0; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; }
</style>
""", unsafe_allow_html=True)


# ── Load Data ──────────────────────────────────────────────────

@st.cache_data(ttl=30)
def get_db(_backend_key: str = "json"):
    """Load database. _backend_key busts cache when backend changes."""
    db = load_db()
    if not db.get("purchase_orders"):
        db = load_sample_data()
        save_db(db)
    return db


def refresh_data():
    st.cache_data.clear()


# ── Sidebar ────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://img.icons8.com/color/96/building.png", width=60)
    st.title("Rehab PO Log")
    st.caption("Property Management Automation")

    st.divider()

    # Backend selector
    backend = st.radio("Data Source", ["Supabase (Cloud)", "JSON (Local)"], horizontal=True)
    set_backend("supabase" if backend.startswith("Supabase") else "json")

    db = get_db(backend)
    properties = db.get("properties", [])
    prop_names = ["All Properties"] + [p["name"] for p in properties]
    selected_property = st.selectbox("Filter by Property", prop_names)

    st.divider()

    if st.button("Refresh Data", use_container_width=True):
        refresh_data()
        st.rerun()

    if st.button("Load Demo Data", use_container_width=True):
        db = load_sample_data()
        save_db(db)
        refresh_data()
        st.rerun()

    st.divider()
    backend_label = "Supabase" if backend.startswith("Supabase") else "Local JSON"
    st.caption(f"Backend: {backend_label}")
    st.caption(f"Last updated: {db.get('metadata', {}).get('last_modified', 'N/A')[:16]}")
    st.caption(f"POs in system: {len(db.get('purchase_orders', []))}")


# ── Filter Data ────────────────────────────────────────────────

all_pos = db.get("purchase_orders", [])
if selected_property != "All Properties":
    prop_id = next((p["id"] for p in properties if p["name"] == selected_property), None)
    filtered_pos = [po for po in all_pos if po["property_id"] == prop_id]
else:
    prop_id = None
    filtered_pos = all_pos


# ── Header Metrics ─────────────────────────────────────────────

st.title("Rehab PO Log Dashboard")

col1, col2, col3, col4, col5 = st.columns(5)

total_committed = sum(po["amount"] for po in filtered_pos)
total_paid = sum(
    (po.get("invoice_amount") or po["amount"])
    for po in filtered_pos if po["status"] == "paid"
)
pending_count = sum(1 for po in filtered_pos if po["status"] in ("pending", "in_progress"))
variance_count = sum(1 for po in filtered_pos if po["status"] == "variance")

if prop_id:
    cats = [c for c in db.get("budget_categories", []) if c["property_id"] == prop_id]
else:
    cats = db.get("budget_categories", [])
total_budget = sum(c["budgeted"] for c in cats)

with col1:
    st.metric("Total Budget", f"${total_budget:,.0f}")
with col2:
    st.metric("Committed", f"${total_committed:,.0f}",
              delta=f"{(total_committed/total_budget*100):.1f}% of budget" if total_budget else None)
with col3:
    st.metric("Paid", f"${total_paid:,.0f}")
with col4:
    st.metric("Pending POs", pending_count)
with col5:
    st.metric("Variance Alerts", variance_count,
              delta="Needs review" if variance_count > 0 else "All clear",
              delta_color="inverse" if variance_count > 0 else "normal")


# ── Tabs ───────────────────────────────────────────────────────

tab_budget, tab_pos, tab_invoices, tab_activity, tab_create = st.tabs([
    "Budget Overview", "Purchase Orders", "Incoming Invoices", "Activity Feed", "Create PO"
])


# ── Budget Overview Tab ────────────────────────────────────────

with tab_budget:
    st.subheader("Budget vs Actual by Property")

    for ps in get_property_summary(db):
        if prop_id and ps["property_id"] != prop_id:
            continue

        with st.expander(f"{ps['property_name']} — {ps['pct_used']:.1f}% committed", expanded=True):
            pcol1, pcol2, pcol3, pcol4 = st.columns(4)
            with pcol1:
                st.metric("Budget", f"${ps['total_budget']:,.0f}")
            with pcol2:
                st.metric("Committed", f"${ps['total_committed']:,.0f}")
            with pcol3:
                st.metric("Remaining", f"${ps['remaining']:,.0f}",
                           delta_color="inverse" if ps['remaining'] < 0 else "normal")
            with pcol4:
                st.metric("Variance Alerts", ps['variance_alerts'])

            # Category breakdown chart
            cat_sums = get_budget_summary(db, ps["property_id"])
            if cat_sums:
                chart_data = pd.DataFrame([
                    {
                        "Category": cs["category_name"],
                        "Budget": cs["budgeted"],
                        "Committed": cs["committed"],
                        "Paid": cs["actual_paid"],
                    }
                    for cs in sorted(cat_sums, key=lambda x: x["budgeted"], reverse=True)
                ])
                chart_data = chart_data.set_index("Category")

                st.bar_chart(chart_data, color=["#0d6efd", "#ffc107", "#28a745"])

                # Table with over-budget highlighting
                table_data = []
                for cs in sorted(cat_sums, key=lambda x: x["pct_used"], reverse=True):
                    status = "OVER" if cs["over_budget"] else (
                        ">85%" if cs["pct_used"] >= 85 else "OK")
                    table_data.append({
                        "Category": cs["category_name"],
                        "Budget": f"${cs['budgeted']:,.0f}",
                        "Committed": f"${cs['committed']:,.0f}",
                        "Paid": f"${cs['actual_paid']:,.0f}",
                        "Remaining": f"${cs['remaining']:,.0f}",
                        "Used %": f"{cs['pct_used']:.1f}%",
                        "Status": status,
                    })

                st.dataframe(
                    pd.DataFrame(table_data),
                    use_container_width=True,
                    hide_index=True,
                )


# ── Purchase Orders Tab ────────────────────────────────────────

with tab_pos:
    st.subheader("All Purchase Orders")

    # Status filter
    status_filter = st.multiselect(
        "Filter by status",
        ["paid", "pending", "in_progress", "received", "variance"],
        default=["paid", "pending", "in_progress", "received", "variance"],
    )

    po_data = []
    for po in sorted(filtered_pos, key=lambda x: x["date_created"], reverse=True):
        if po["status"] not in status_filter:
            continue

        status_icon = {
            "paid": "Paid",
            "pending": "Pending",
            "in_progress": "In Progress",
            "received": "Received",
            "variance": "VARIANCE",
        }.get(po["status"], po["status"])

        variance_str = ""
        if po.get("invoice_amount") and po["amount"] > 0:
            var = po["invoice_amount"] - po["amount"]
            var_pct = (var / po["amount"]) * 100
            if abs(var) > 0.01:
                variance_str = f"${var:+,.0f} ({var_pct:+.1f}%)"

        po_data.append({
            "PO #": po["po_number"],
            "Property": get_property_name(db, po["property_id"]),
            "Vendor": get_vendor_name(db, po["vendor_id"]),
            "Category": po["category"],
            "Description": po["description"][:50],
            "PO Amount": f"${po['amount']:,.0f}",
            "Invoice": f"${po['invoice_amount']:,.0f}" if po.get("invoice_amount") else "—",
            "Variance": variance_str or "—",
            "Status": status_icon,
            "Created": po["date_created"],
        })

    if po_data:
        st.dataframe(
            pd.DataFrame(po_data),
            use_container_width=True,
            hide_index=True,
            height=400,
        )
    else:
        st.info("No purchase orders match the current filters.")

    # Variance details
    variance_pos = [po for po in filtered_pos if po["status"] == "variance"]
    if variance_pos:
        st.subheader("Variance Details")
        for po in variance_pos:
            var = (po.get("invoice_amount") or 0) - po["amount"]
            var_pct = (var / po["amount"] * 100) if po["amount"] > 0 else 0
            st.markdown(f"""
            <div class="variance-alert">
                <strong>{po['po_number']}</strong> — {po['description']}<br>
                PO: ${po['amount']:,.2f} → Invoice: ${po.get('invoice_amount', 0):,.2f}
                (variance: <strong>${var:+,.2f} / {var_pct:+.1f}%</strong>)<br>
                <em>{po.get('notes', '')}</em>
            </div>
            """, unsafe_allow_html=True)


# ── Incoming Invoices Tab ──────────────────────────────────────

with tab_invoices:
    st.subheader("Incoming Invoices")

    all_invoices = db.get("invoices_unmatched", [])
    unmatched_invs = get_unmatched_invoices(db)
    matched_invs = get_matched_invoices(db)

    if all_invoices:
        icol1, icol2, icol3, icol4 = st.columns(4)
        with icol1:
            st.metric("Total Invoices", len(all_invoices))
        with icol2:
            st.metric("Unmatched (No PO)", len(unmatched_invs))
        with icol3:
            st.metric("Matched", len(matched_invs))
        with icol4:
            inv_total = sum(i["amount"] for i in all_invoices)
            st.metric("Invoice Total", f"${inv_total:,.0f}")

        # Process all button
        if unmatched_invs or matched_invs:
            if st.button("Process All Invoices (Auto-Create POs)", type="primary",
                         use_container_width=True):
                with st.spinner("Processing invoices..."):
                    results = process_all_invoices(db)
                    save_db(db)
                    refresh_data()

                if results["auto_created"]:
                    st.success(f"Auto-created {len(results['auto_created'])} POs "
                               f"for unmatched invoices")
                if results["matched"]:
                    st.success(f"Matched {len(results['matched'])} invoices to existing POs")
                if results["errors"]:
                    for err in results["errors"]:
                        st.error(f"Error: {err['invoice']} — {err['error']}")
                st.rerun()

        # Invoice table
        inv_data = []
        for inv in sorted(all_invoices, key=lambda x: x["date"], reverse=True):
            status_icon = {
                "unmatched": "No PO",
                "matched": "Has PO Ref",
                "auto_matched": "Auto-Created PO",
                "processed": "Processed",
            }.get(inv["status"], inv["status"])

            inv_data.append({
                "Invoice #": inv["invoice_number"],
                "Vendor": inv["vendor"],
                "Date": inv["date"],
                "Amount": f"${inv['amount']:,.2f}",
                "Description": inv["description"][:50],
                "PO Ref": inv.get("po_reference") or "NONE",
                "Category": inv.get("category_guess", "?"),
                "Status": status_icon,
            })

        st.dataframe(
            pd.DataFrame(inv_data),
            use_container_width=True,
            hide_index=True,
            height=500,
        )
    else:
        st.info("No incoming invoices to display.")


# ── Activity Feed Tab ──────────────────────────────────────────

with tab_activity:
    st.subheader("Recent Activity")

    activity = get_recent_activity(db, limit=30)
    if not activity:
        st.info("No activity recorded yet.")
    else:
        for entry in activity:
            ts = entry["timestamp"][:16].replace("T", " ")
            icon = {
                "po_created": "[PO]",
                "invoice_received": "[INV]",
                "payment_processed": "[PAY]",
                "variance_alert": "[VAR]",
                "budget_warning": "[BUD]",
                "status_update": "[UPD]",
            }.get(entry["type"], "[-]")

            color = "#dc3545" if "variance" in entry["type"] or "warning" in entry["type"] else "#333"

            st.markdown(
                f"<div style='padding: 6px 0; border-bottom: 1px solid #eee;'>"
                f"<span style='color: #999; font-size: 13px;'>{ts}</span> "
                f"{icon} <span style='color: {color};'>{entry['message']}</span></div>",
                unsafe_allow_html=True,
            )


# ── Create PO Tab ──────────────────────────────────────────────

with tab_create:
    st.subheader("Create New Purchase Order")

    create_mode = st.radio("Creation method", ["Form", "Natural Language"], horizontal=True)

    if create_mode == "Form":
        with st.form("create_po_form"):
            fcol1, fcol2 = st.columns(2)

            with fcol1:
                prop_options = {p["name"]: p["id"] for p in properties}
                selected_prop_name = st.selectbox("Property", list(prop_options.keys()))
                vendor_options = {v["name"]: v["id"] for v in db.get("vendors", [])}
                selected_vendor_name = st.selectbox("Vendor", list(vendor_options.keys()))
                category = st.selectbox("Category", [
                    "PW", "GD", "LS", "DR", "PL", "IT", "LG", "SF", "SG", "SC",
                ])

            with fcol2:
                description = st.text_input("Description")
                amount = st.number_input("Amount ($)", min_value=0.0, step=100.0)
                notes = st.text_area("Notes", height=68)

            submitted = st.form_submit_button("Create PO", type="primary", use_container_width=True)

            if submitted and description and amount > 0:
                po = create_po(
                    db,
                    property_id=prop_options[selected_prop_name],
                    vendor_id=vendor_options[selected_vendor_name],
                    description=description,
                    category=category,
                    amount=amount,
                    notes=notes,
                )
                save_db(db)
                refresh_data()
                st.success(f"PO {po['po_number']} created: {description} (${amount:,.2f})")
                st.rerun()

    else:  # Natural Language
        st.info("Type a natural language PO request. The AI will parse it into a structured PO.")
        nl_input = st.text_area(
            "PO Request",
            placeholder="E.g.: Plumbing repair for pool area at Summit Ridge, $3,800, ProFix Plumbing",
            height=100,
        )
        if st.button("Create PO from Text", type="primary", use_container_width=True):
            if nl_input:
                with st.spinner("AI is processing your request..."):
                    from adrian_po_processor import create_po_from_request
                    result = create_po_from_request(nl_input, db)

                if result.get("success"):
                    st.success(
                        f"PO {result['po_number']} created: "
                        f"{result['description']} (${result['amount']:,.2f}) "
                        f"— {result.get('vendor', '')} / {result.get('property', '')}"
                    )
                    refresh_data()
                    st.rerun()
                else:
                    st.error(f"Failed: {result.get('error', 'Unknown error')}")
            else:
                st.warning("Please enter a PO request.")


# ── Footer ─────────────────────────────────────────────────────

st.divider()
st.caption("Rehab PO Log Automation — Built by Florian Rolke for Adrian Danila / Multifamily X Consulting")
