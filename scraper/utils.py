"""Utility functions: phone normalization, strong dedup, CSV I/O."""

from __future__ import annotations

import csv
import json
import os
import re
import unicodedata
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


def _normalize_name(name: str) -> str:
    """Aggressive name normalization for dedup.
    Strips accents, punctuation, common suffixes, lowercases everything.
    'Micro-Crèche Montessori Barrault - La Maison Bleue' -> 'microcrechemontessoribarrault'
    """
    if not name:
        return ""
    # Remove accents
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase
    s = ascii_str.lower()
    # Remove common suffixes/prefixes that create false negatives
    for noise in [
        "- la maison bleue", "la maison bleue",
        "- people & baby", "people & baby", "people and baby",
        "- babilou", "babilou",
        "les petits chaperons rouges", "lpcr",
        "micro creche montessori", "micro-creche montessori",
        "sarl", "sas", "eurl", "sasu", "sa ", "sci ",
        "paris", "france",
    ]:
        s = s.replace(noise, "")
    # Strip everything non-alphanumeric
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def _phone_key(phone: str) -> str:
    """Normalize phone to digits-only for dedup matching."""
    if not phone:
        return ""
    return re.sub(r"\D", "", phone)


def deduplicate(leads: list[Lead]) -> list[Lead]:
    """Strong multi-key deduplication.

    A lead is considered duplicate if ANY of these match an existing lead:
    1. Same normalized name + same postal code
    2. Same normalized name (ignoring postal code, catches cross-arrondissement dupes)
    3. Same phone number (if both have phones)
    4. Same website domain (if both have websites)

    When duplicates found, keep the one with the highest quality score.
    """
    # First pass: group by various keys
    by_name_cp: dict[str, Lead] = {}
    by_name: dict[str, Lead] = {}
    by_phone: dict[str, Lead] = {}
    by_domain: dict[str, Lead] = {}

    def _domain(url: str) -> str:
        if not url:
            return ""
        # Extract domain from URL
        url = url.lower().replace("http://", "").replace("https://", "").replace("www.", "")
        return url.split("/")[0].split("?")[0]

    def _better(new: Lead, existing: Lead) -> bool:
        return new.quality_score() > existing.quality_score()

    def _merge(winner: Lead, loser: Lead) -> Lead:
        """Merge: fill empty fields from loser into winner."""
        if not winner.telephone and loser.telephone:
            winner.telephone = loser.telephone
        if not winner.site_web and loser.site_web:
            winner.site_web = loser.site_web
        if not winner.email and loser.email:
            winner.email = loser.email
        if not winner.adresse and loser.adresse:
            winner.adresse = loser.adresse
        if not winner.code_postal and loser.code_postal:
            winner.code_postal = loser.code_postal
        if not winner.ville and loser.ville:
            winner.ville = loser.ville
        if not winner.note_google and loser.note_google:
            winner.note_google = loser.note_google
        if not winner.nb_avis and loser.nb_avis:
            winner.nb_avis = loser.nb_avis
        return winner

    result_map: dict[int, Lead] = {}  # id -> lead
    # Track which lead id each key points to
    name_cp_to_id: dict[str, int] = {}
    name_to_id: dict[str, int] = {}
    phone_to_id: dict[str, int] = {}
    domain_to_id: dict[str, int] = {}

    for lead in leads:
        norm_name = _normalize_name(lead.nom_entreprise)
        name_cp_key = f"{norm_name}_{lead.code_postal}" if norm_name else ""
        phone_key = _phone_key(lead.telephone)
        domain_key = _domain(lead.site_web)

        # Find existing match via any key
        existing_id = None
        if name_cp_key and name_cp_key in name_cp_to_id:
            existing_id = name_cp_to_id[name_cp_key]
        elif norm_name and norm_name in name_to_id:
            existing_id = name_to_id[norm_name]
        elif phone_key and phone_key in phone_to_id:
            existing_id = phone_to_id[phone_key]
        elif domain_key and domain_key in domain_to_id:
            existing_id = domain_to_id[domain_key]

        if existing_id is not None and existing_id in result_map:
            existing = result_map[existing_id]
            if _better(lead, existing):
                merged = _merge(lead, existing)
                result_map[existing_id] = merged
            else:
                _merge(existing, lead)
        else:
            # New unique lead
            lead_id = id(lead)
            result_map[lead_id] = lead
            if name_cp_key:
                name_cp_to_id[name_cp_key] = lead_id
            if norm_name:
                name_to_id[norm_name] = lead_id
            if phone_key:
                phone_to_id[phone_key] = lead_id
            if domain_key:
                domain_to_id[domain_key] = lead_id

    result = list(result_map.values())
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
