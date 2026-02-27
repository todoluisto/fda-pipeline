"""Transformer — cleans and flattens raw OpenFDA API responses.

Takes the nested JSON records from the API and produces a flat list of
dicts suitable for loading into a CSV / DataFrame.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _safe_get(obj: dict | None, *keys: str, default: str = "") -> str:
    """Safely traverse nested dicts/lists, returning *default* on any miss."""
    current = obj
    for key in keys:
        if current is None:
            return default
        if isinstance(current, list):
            current = current[0] if current else None
            if current is None:
                return default
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
    if current is None:
        return default
    if isinstance(current, list):
        return ", ".join(str(v) for v in current) if current else default
    return str(current)


def _derive_application_type(application_number: str) -> str:
    """Derive the application type from the application number prefix.

    The OpenFDA API does not populate application_type directly, but
    the prefix of application_number encodes it:
      - ANDA... → "ANDA" (Abbreviated New Drug Application)
      - NDA...  → "NDA"  (New Drug Application)
      - BLA...  → "BLA"  (Biologics License Application)
    """
    upper = application_number.upper()
    if upper.startswith("ANDA"):
        return "ANDA"
    if upper.startswith("NDA"):
        return "NDA"
    if upper.startswith("BLA"):
        return "BLA"
    logger.warning("Unrecognized application_number prefix: %s", application_number)
    return ""


def _extract_product_fields(product: dict) -> dict:
    """Extract product-level fields including strength and active ingredients.

    Strength and active ingredient names live under
    products[].active_ingredients[], not directly on the product object.
    """
    active_ingredients = product.get("active_ingredients", []) or []

    strengths = [
        ing.get("strength", "")
        for ing in active_ingredients
        if ing.get("strength")
    ]
    ingredient_names = [
        ing.get("name", "")
        for ing in active_ingredients
        if ing.get("name")
    ]

    return {
        "marketing_status": product.get("marketing_status", ""),
        "dosage_form": product.get("dosage_form", ""),
        "route": product.get("route", ""),
        "strength": "; ".join(strengths),
        "active_ingredients": "; ".join(ingredient_names),
    }


def _flatten_record(record: dict) -> list[dict]:
    """Flatten a single API record into one or more rows.

    Each record may have multiple products. We create one row per product
    so the CSV captures dosage_form, route, strength, and marketing_status
    for every product entry. If a record has no products, we still emit
    one row with those fields blank.

    Returns:
        A list of flat dicts (one per product row).
    """
    openfda = record.get("openfda", {}) or {}
    submissions = record.get("submissions", []) or []
    products = record.get("products", []) or []

    # Common fields shared across all product rows
    base = {
        "application_number": record.get("application_number", ""),
        "sponsor_name": record.get("sponsor_name", ""),
        "application_type": _derive_application_type(
            record.get("application_number", "")
        ),
        "brand_name": _safe_get(openfda, "brand_name"),
        "generic_name": _safe_get(openfda, "generic_name"),
        "manufacturer_name": _safe_get(openfda, "manufacturer_name"),
        "product_ndc": _safe_get(openfda, "product_ndc"),
    }

    # Latest submission (sorted by date descending)
    if submissions:
        sorted_subs = sorted(
            submissions,
            key=lambda s: s.get("submission_status_date", ""),
            reverse=True,
        )
        latest = sorted_subs[0]
        base["submission_type"] = latest.get("submission_type", "")
        base["submission_status"] = latest.get("submission_status", "")
        base["submission_status_date"] = latest.get("submission_status_date", "")
        # submission_class_code_description is nested under submission_class_code
        sub_class = latest.get("submission_class_code_description", "")
        if not sub_class:
            sub_class = _safe_get(latest, "submission_class_code", "description")
        base["submission_class_code_description"] = sub_class
    else:
        base["submission_type"] = ""
        base["submission_status"] = ""
        base["submission_status_date"] = ""
        base["submission_class_code_description"] = ""

    # Expand one row per product
    if not products:
        base["marketing_status"] = ""
        base["dosage_form"] = ""
        base["route"] = ""
        base["strength"] = ""
        base["active_ingredients"] = ""
        return [base]

    rows: list[dict] = []
    for product in products:
        row = dict(base)
        row.update(_extract_product_fields(product))
        rows.append(row)

    return rows


def transform(raw_records: list[dict]) -> list[dict]:
    """Transform a batch of raw API records into flat rows.

    Args:
        raw_records: List of result dicts straight from the API.

    Returns:
        List of flat dicts ready for DataFrame construction.
    """
    flat_rows: list[dict] = []
    for record in raw_records:
        try:
            flat_rows.extend(_flatten_record(record))
        except Exception:
            logger.exception(
                "Failed to flatten record: application_number=%s",
                record.get("application_number", "UNKNOWN"),
            )
    logger.info(
        "Transformed %d raw records into %d flat rows",
        len(raw_records), len(flat_rows),
    )
    return flat_rows
