"""Lead data model with validation and quality scoring."""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict, fields


@dataclass
class Lead:
    nom_entreprise: str = ""
    adresse: str = ""
    code_postal: str = ""
    ville: str = ""
    telephone: str = ""
    email: str = ""
    site_web: str = ""
    note_google: str = ""
    nb_avis: str = ""
    segment: str = ""
    source: str = ""

    def is_valid(self, allowed_cp_prefixes: list[str] | None = None) -> bool:
        if allowed_cp_prefixes is None:
            allowed_cp_prefixes = ["75"]

        has_contact = bool(self.telephone) or bool(self.site_web)
        # Zone check: CP match, city name, or address mention
        in_zone = any(self.code_postal.startswith(p) for p in allowed_cp_prefixes) if self.code_postal else False
        if not in_zone and self.ville:
            in_zone = any(
                kw in self.ville.lower()
                for kw in ["paris", "boulogne", "neuilly", "levallois", "montreuil", "saint-denis",
                           "vincennes", "saint-ouen", "clichy", "issy", "vanves", "malakoff",
                           "montrouge", "ivry", "charenton", "pantin", "aubervilliers", "bagnolet",
                           "nanterre", "colombes", "courbevoie", "asnieres", "rueil",
                           "suresnes", "puteaux", "meudon", "clamart", "antony",
                           "gennevilliers", "chatillon", "sceaux", "garches", "sevres",
                           "chaville", "ville-d'avray", "saint-cloud", "la defense"]
            )
        if not in_zone and self.adresse:
            addr_lower = self.adresse.lower()
            in_zone = "paris" in addr_lower or "hauts-de-seine" in addr_lower or any(
                f"92{str(i).zfill(3)}" in addr_lower for i in range(100)
            ) or any(
                f"75{str(i).zfill(3)}" in addr_lower for i in range(21)
            )
        # If source is a Paris-specific search and no address info, assume Paris
        if not in_zone and not self.code_postal and not self.ville and not self.adresse:
            in_zone = True  # trust the search query geo-targeting
        name_ok = (
            bool(self.nom_entreprise.strip())
            and self.nom_entreprise.strip().lower()
            not in ["resultats", "résultats", "google maps", "", "plan", "itinéraire"]
        )
        return name_ok and has_contact

    def quality_score(self) -> int:
        s = 0
        if self.telephone:
            s += 3
        if self.site_web:
            s += 2
        if self.email:
            s += 3
        if self.adresse:
            s += 1
        if self.note_google:
            s += 1
        return s

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def fieldnames() -> list[str]:
        return [f.name for f in fields(Lead)]
