#!/usr/bin/env python3
"""
Adrian Danila — AI PO/Invoice Processor

Extracts structured PO and invoice data from email text, PDF attachments,
or natural language PO creation requests using DeepSeek via OpenRouter.

Usage:
    python clients/adrian/execution/adrian_po_processor.py --demo
    python clients/adrian/execution/adrian_po_processor.py --text "HVAC repair, Unit 12B, $850, ABC Heating"
    python clients/adrian/execution/adrian_po_processor.py --invoice "path/to/invoice.txt"
    python clients/adrian/execution/adrian_po_processor.py --invoice-pdf "path/to/invoice.pdf"
    python clients/adrian/execution/adrian_po_processor.py --batch-pdf "path/to/pdf/directory"
"""

import sys
import json
import os
import argparse
from pathlib import Path
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent
MAIN_ROOT = PROJECT_ROOT.parent.parent
load_dotenv(MAIN_ROOT / ".env")

sys.path.insert(0, str(PROJECT_ROOT / "execution"))
from adrian_db_manager import load_db, save_db, get_po, process_invoice, create_po, get_vendor_by_name, get_property_by_name

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MODEL = "deepseek/deepseek-chat"

INVOICES_DIR = PROJECT_ROOT / "resources" / "Anonymized_Invoices"


# ── PDF Text Extraction ───────────────────────────────────────

def extract_pdf_text(pdf_path: str) -> str:
    """Extract all text from a PDF file using pdfplumber."""
    import pdfplumber
    import re

    with pdfplumber.open(pdf_path) as pdf:
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
    raw = "\n\n".join(pages)
    # Clean up whitespace artifacts
    raw = re.sub(r"[ \t]+", " ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


def batch_extract_pdfs(pdf_dir: str) -> dict[str, str]:
    """Extract text from all PDFs in a directory. Returns {filename: text}."""
    results = {}
    pdf_path = Path(pdf_dir)
    for f in sorted(pdf_path.glob("*.pdf")):
        try:
            text = extract_pdf_text(str(f))
            results[f.name] = text
            print(f"  [OK] {f.name} ({len(text)} chars)")
        except Exception as e:
            results[f.name] = f"ERROR: {e}"
            print(f"  [FAIL] {f.name}: {e}")
    return results


def call_deepseek(system_prompt: str, user_prompt: str, retries: int = 3) -> str:
    """Call DeepSeek via OpenRouter (OpenAI-compatible API) with retry."""
    import requests
    import time

    if not OPENROUTER_API_KEY:
        print("[ERROR] OPENROUTER_API_KEY not found in .env")
        sys.exit(1)

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://florianrolke.com",
                    "X-Title": "Adrian PO Log Automation",
                },
                json={
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 2000,
                },
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
        except requests.exceptions.HTTPError as e:
            if attempt < retries and response.status_code >= 500:
                print(f"[RETRY] Attempt {attempt}/{retries} failed (HTTP {response.status_code}), retrying in 3s...")
                time.sleep(3)
            else:
                raise


# ── Invoice Extraction ─────────────────────────────────────────

INVOICE_EXTRACTION_PROMPT = """You are an invoice data extraction system for a property management company.
Extract the following fields from the invoice text. Return ONLY valid JSON, no markdown fences.

Required fields:
{
  "vendor_name": "string",
  "invoice_number": "string",
  "invoice_date": "YYYY-MM-DD",
  "po_reference": "string or null (the PO number this invoice references)",
  "property_name": "string",
  "description": "string (brief description of work)",
  "line_items": [
    {"description": "string", "quantity": number, "unit_price": number, "total": number}
  ],
  "subtotal": number,
  "tax": number,
  "total": number,
  "payment_terms": "string",
  "due_date": "YYYY-MM-DD or null",
  "notes": "string or null"
}

If a field is not found in the text, use null. For amounts, use numbers (not strings).
"""


def extract_invoice_data(text: str) -> dict:
    """Extract structured data from invoice text using DeepSeek."""
    print(f"[AI] Extracting invoice data ({len(text)} chars)...")
    raw = call_deepseek(INVOICE_EXTRACTION_PROMPT, text)

    # Clean response — strip markdown fences if present
    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    try:
        data = json.loads(cleaned)
        print(f"[OK] Extracted: {data.get('vendor_name', '?')} — "
              f"Invoice #{data.get('invoice_number', '?')} — "
              f"${data.get('total', 0):,.2f}")
        return data
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse AI response: {e}")
        print(f"[DEBUG] Raw response:\n{raw[:500]}")
        return {}


# ── Natural Language PO Creation ───────────────────────────────

PO_CREATION_PROMPT = """You are a PO creation assistant for a property management company.
Parse the natural language request into a structured Purchase Order. Return ONLY valid JSON.

Known properties:
- "Summit Ridge Apartments" (ID: PROP001) — 248 units, Asheville NC

Known vendors:
- "Apex Builders LLC" (ID: V001) — General Contractor, Exterior, Landscaping, Pressure Washing
- "ProFix Plumbing & Handyman" (ID: V002) — Plumbing, Sewer, General Maintenance
- "TechSource Direct" (ID: V003) — IT Equipment & Technology
- "Keystone Staffing Group" (ID: V004) — National Leasing Specialist Deployment
- "SafeGuard Supply Co." (ID: V005) — Fire Safety Equipment & Smoke Alarms
- "SignWorks Design" (ID: V006) — Property Signage, Banners, Monument Signs
- "SecureLock Systems Inc." (ID: V007) — Door Lock Systems & Access Control

Budget categories: PW (Pressure Washing), GD (Gutters & Downspouts), LS (Landscaping & Rock),
DR (Drainage), PL (Plumbing), IT (IT Equipment), LG (Leasing Services),
SF (Safety & Fire), SG (Signage), SC (Security Hardware)

Return:
{
  "property_id": "PROP001",
  "property_name": "string",
  "vendor_id": "V001-V007 or NEW",
  "vendor_name": "string",
  "description": "string (clear scope of work)",
  "category": "two-letter code from above",
  "amount": number,
  "notes": "string or null"
}

If the vendor is not in the known list, set vendor_id to "NEW".
Default to PROP001 (Summit Ridge Apartments).
Best-guess the category from the description.
"""


def parse_po_request(text: str) -> dict:
    """Parse natural language PO creation request."""
    print(f"[AI] Parsing PO request: '{text[:80]}...'")
    raw = call_deepseek(PO_CREATION_PROMPT, text)

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    try:
        data = json.loads(cleaned)
        print(f"[OK] Parsed: {data.get('description', '?')} — "
              f"${data.get('amount', 0):,.2f} — "
              f"{data.get('property_name', '?')}")
        return data
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse AI response: {e}")
        return {}


# ── Match Invoice to PO ───────────────────────────────────────

def match_and_process_invoice(invoice_data: dict, db: dict = None) -> dict:
    """Match extracted invoice data to a PO and process it."""
    if db is None:
        db = load_db()

    po_ref = invoice_data.get("po_reference")
    total = invoice_data.get("total", 0)
    inv_num = invoice_data.get("invoice_number", "UNKNOWN")

    if not po_ref:
        return {
            "success": False,
            "error": "No PO reference found in invoice",
            "invoice_data": invoice_data,
        }

    po = get_po(db, po_ref)
    if not po:
        return {
            "success": False,
            "error": f"PO {po_ref} not found in database",
            "invoice_data": invoice_data,
        }

    result = process_invoice(db, po_ref, inv_num, total)
    save_db(db)

    return {
        **result,
        "invoice_data": invoice_data,
        "po_description": po["description"],
        "vendor": invoice_data.get("vendor_name", ""),
    }


def create_po_from_request(request_text: str, db: dict = None) -> dict:
    """Create a PO from natural language request."""
    if db is None:
        db = load_db()

    parsed = parse_po_request(request_text)
    if not parsed:
        return {"success": False, "error": "Failed to parse request"}

    po = create_po(
        db,
        property_id=parsed.get("property_id", "PROP001"),
        vendor_id=parsed.get("vendor_id", "NEW"),
        description=parsed.get("description", request_text),
        category=parsed.get("category", "MU"),
        amount=parsed.get("amount", 0),
        notes=parsed.get("notes", ""),
    )
    save_db(db)

    return {
        "success": True,
        "po_number": po["po_number"],
        "description": po["description"],
        "amount": po["amount"],
        "property": parsed.get("property_name", ""),
        "vendor": parsed.get("vendor_name", ""),
        "category": po["category"],
    }


# ── Demo Mode ──────────────────────────────────────────────────

def run_demo():
    """Demo: extract invoice data and process against sample PO database."""
    from adrian_db_manager import load_sample_data, save_db as save, print_po_table

    print("=" * 70)
    print("  ADRIAN PO LOG — AI PROCESSOR DEMO")
    print("=" * 70)

    # Step 1: Load sample database
    print("\n[1/4] Loading sample PO database...")
    sample_path = PROJECT_ROOT / "resources" / "sample_po_data.json"
    with open(sample_path, "r", encoding="utf-8") as f:
        sample = json.load(f)

    db = load_sample_data()
    save(db)
    print(f"  Loaded {len(db['purchase_orders'])} POs")

    # Step 2: Extract invoice data
    print("\n[2/4] Processing sample invoice with AI...")
    invoice_text = sample.get("sample_invoice_text", "")
    if not invoice_text:
        print("  [SKIP] No sample invoice text in data file")
        return

    invoice_data = extract_invoice_data(invoice_text)
    if not invoice_data:
        print("  [ERROR] Extraction failed")
        return

    print(f"\n  Extracted Invoice:")
    print(f"    Vendor:     {invoice_data.get('vendor_name', '?')}")
    print(f"    Invoice #:  {invoice_data.get('invoice_number', '?')}")
    print(f"    PO Ref:     {invoice_data.get('po_reference', '?')}")
    print(f"    Total:      ${invoice_data.get('total', 0):,.2f}")
    print(f"    Line items: {len(invoice_data.get('line_items', []))}")

    # Step 3: Match to PO and check variance
    print("\n[3/4] Matching invoice to PO and checking variance...")
    result = match_and_process_invoice(invoice_data, db)

    if result.get("success"):
        print(f"  PO Match:     {result['po_number']}")
        print(f"  PO Amount:    ${result['po_amount']:,.2f}")
        print(f"  Invoice:      ${result['invoice_amount']:,.2f}")
        print(f"  Variance:     ${result['variance']:+,.2f} "
              f"({result['variance_pct']:+.1f}%)")
        if result["exceeds_threshold"]:
            print(f"  ** VARIANCE EXCEEDS THRESHOLD — flagged for review **")
    else:
        print(f"  Error: {result.get('error', 'Unknown')}")

    # Step 4: Demo natural language PO creation
    print("\n[4/4] Creating PO from natural language...")
    test_requests = [
        "Plumbing repair for pool area at Summit Ridge, $3,800, ProFix Plumbing",
        "New signage banners for property entrance, $1,325, SignWorks Design",
    ]

    for req in test_requests:
        print(f"\n  Request: \"{req}\"")
        result = create_po_from_request(req, db)
        if result.get("success"):
            print(f"  → Created PO {result['po_number']}: "
                  f"{result['description']} (${result['amount']:,.2f})")
        else:
            print(f"  → Error: {result.get('error')}")

    print("\n" + "=" * 70)
    print("  DEMO COMPLETE")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Adrian PO/Invoice AI Processor")
    parser.add_argument("--demo", action="store_true",
                        help="Run demo with sample invoice data")
    parser.add_argument("--text", type=str,
                        help="Natural language PO creation request")
    parser.add_argument("--invoice", type=str,
                        help="Path to invoice text file to process")
    parser.add_argument("--invoice-pdf", type=str,
                        help="Path to invoice PDF file to extract and process")
    parser.add_argument("--batch-pdf", type=str,
                        help="Path to directory of PDF invoices to batch extract")
    args = parser.parse_args()

    if args.demo:
        run_demo()
    elif args.text:
        result = create_po_from_request(args.text)
        print(json.dumps(result, indent=2))
    elif args.invoice_pdf:
        path = Path(args.invoice_pdf)
        if not path.exists():
            print(f"[ERROR] File not found: {path}")
            sys.exit(1)
        print(f"[PDF] Extracting text from: {path.name}")
        text = extract_pdf_text(str(path))
        print(f"[PDF] Extracted {len(text)} chars from {len(text.split(chr(10)))} lines")
        print(f"\n--- EXTRACTED TEXT ---\n{text[:500]}{'...' if len(text) > 500 else ''}\n--- END ---\n")
        invoice_data = extract_invoice_data(text)
        if invoice_data:
            result = match_and_process_invoice(invoice_data)
            print(json.dumps(result, indent=2, default=str))
    elif args.batch_pdf:
        pdf_dir = Path(args.batch_pdf)
        if not pdf_dir.is_dir():
            print(f"[ERROR] Directory not found: {pdf_dir}")
            sys.exit(1)
        print(f"[BATCH] Extracting text from all PDFs in: {pdf_dir}")
        results = batch_extract_pdfs(str(pdf_dir))
        print(f"\n[BATCH] Extracted {len(results)} PDFs")
        for fname, text in results.items():
            if text.startswith("ERROR"):
                print(f"  FAIL: {fname} — {text}")
            else:
                print(f"  OK: {fname} — {len(text)} chars")
    elif args.invoice:
        path = Path(args.invoice)
        if not path.exists():
            print(f"[ERROR] File not found: {path}")
            sys.exit(1)
        text = path.read_text(encoding="utf-8")
        invoice_data = extract_invoice_data(text)
        if invoice_data:
            result = match_and_process_invoice(invoice_data)
            print(json.dumps(result, indent=2, default=str))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
