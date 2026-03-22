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
        in_zone = any(self.code_postal.startswith(p) for p in allowed_cp_prefixes) if self.code_postal else False
        # Fallback: check city name
        if not in_zone and self.ville:
            in_zone = any(
                kw in self.ville.lower()
                for kw in ["paris", "boulogne", "neuilly", "levallois", "montreuil", "saint-denis"]
            )
        name_ok = (
            bool(self.nom_entreprise.strip())
            and self.nom_entreprise.strip().lower()
            not in ["resultats", "résultats", "google maps", ""]
        )
        return name_ok and in_zone and has_contact

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
