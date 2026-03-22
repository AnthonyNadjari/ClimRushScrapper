"""Utility functions: phone normalization, dedup, CSV I/O."""

import csv
import json
import os
import re
from .models import Lead


def normalize_phone(raw: str) -> str:
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("33") and len(digits) >= 11:
        digits = "0" + digits[2:12]
    if digits.startswith("08"):
        return ""
    if len(digits) == 10 and digits.startswith("0"):
        return " ".join([digits[i : i + 2] for i in range(0, 10, 2)])
    return ""


def extract_cp(text: str, pattern: str = r"\b(7[5789]\d{3}|9[12345]\d{3})\b") -> str:
    match = re.search(pattern, text)
    return match.group(0) if match else ""


def deduplicate(leads: list[Lead]) -> list[Lead]:
    seen = set()
    result = []
    for lead in leads:
        name_key = re.sub(r"[^A-Z0-9]", "", lead.nom_entreprise.upper())
        key = f"{name_key}_{lead.code_postal}"
        if key and key not in seen:
            seen.add(key)
            result.append(lead)
    result.sort(key=lambda l: l.quality_score(), reverse=True)
    return result


def save_csv(leads: list[Lead], filepath: str, allowed_cp: list[str] | None = None) -> list[Lead]:
    valid = deduplicate([l for l in leads if l.is_valid(allowed_cp)])
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=Lead.fieldnames(), delimiter=";")
        writer.writeheader()
        for lead in valid:
            writer.writerow(lead.to_dict())
    return valid


def save_json_summary(leads: list[Lead], filepath: str, run_date: str = ""):
    """Save a JSON summary for the dashboard."""
    import datetime
    if not run_date:
        run_date = datetime.datetime.now(datetime.timezone.utc).isoformat()

    segments = {}
    for lead in leads:
        seg = lead.segment or "Unknown"
        if seg not in segments:
            segments[seg] = {"name": seg, "count": 0, "phones": 0, "websites": 0, "emails": 0}
        segments[seg]["count"] += 1
        if lead.telephone:
            segments[seg]["phones"] += 1
        if lead.site_web:
            segments[seg]["websites"] += 1
        if lead.email:
            segments[seg]["emails"] += 1

    summary = {
        "run_date": run_date,
        "total_leads": len(leads),
        "total_phones": len([l for l in leads if l.telephone]),
        "total_websites": len([l for l in leads if l.site_web]),
        "segments": list(segments.values()),
        "top_leads": [l.to_dict() for l in leads[:30]],
    }

    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary
