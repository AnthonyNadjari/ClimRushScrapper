"""
ClimRush Scraper Engine — CLI entry point.
Runs a single segment from the config (for GitHub Actions matrix parallelism).
Supports arrondissement expansion for massive coverage.

Usage:
    python -m scraper.engine --config config/segments.json --segment-index 0 --output-dir output/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import io
import time

from playwright.async_api import async_playwright

from .models import Lead
from .utils import save_csv, deduplicate
from .browser import launch_browser
from .sources import gmaps, pagesjaunes, annuaire, societe

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)
log = logging.getLogger("climrush")

# Paris arrondissements for query expansion
ARRONDISSEMENTS = [
    ("75001", "paris 1er", 48.8606, 2.3376),
    ("75002", "paris 2eme", 48.8685, 2.3441),
    ("75003", "paris 3eme", 48.8637, 2.3615),
    ("75004", "paris 4eme", 48.8540, 2.3574),
    ("75005", "paris 5eme", 48.8462, 2.3497),
    ("75006", "paris 6eme", 48.8499, 2.3331),
    ("75007", "paris 7eme", 48.8566, 2.3150),
    ("75008", "paris 8eme", 48.8744, 2.3106),
    ("75009", "paris 9eme", 48.8769, 2.3390),
    ("75010", "paris 10eme", 48.8763, 2.3614),
    ("75011", "paris 11eme", 48.8597, 2.3793),
    ("75012", "paris 12eme", 48.8396, 2.3876),
    ("75013", "paris 13eme", 48.8322, 2.3561),
    ("75014", "paris 14eme", 48.8286, 2.3268),
    ("75015", "paris 15eme", 48.8421, 2.2994),
    ("75016", "paris 16eme", 48.8637, 2.2769),
    ("75017", "paris 17eme", 48.8852, 2.3094),
    ("75018", "paris 18eme", 48.8925, 2.3444),
    ("75019", "paris 19eme", 48.8871, 2.3817),
    ("75020", "paris 20eme", 48.8638, 2.3985),
]


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_zones(config_dir: str) -> dict:
    zones_path = os.path.join(config_dir, "zones.json")
    if os.path.exists(zones_path):
        with open(zones_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"departments": [{"code": "75", "label": "Paris", "pj_slug": "Paris+(75)", "lat": 48.8566, "lng": 2.3522}]}


def expand_queries_by_arrondissement(queries: list[str]) -> list[tuple[str, float, float]]:
    """Expand each query × 20 arrondissements for massive coverage."""
    expanded = []
    for query in queries:
        for cp, label, lat, lng in ARRONDISSEMENTS:
            expanded.append((f"{query} {label}", lat, lng))
    return expanded


async def scrape_segment(config: dict, segment_index: int, output_dir: str):
    segments = config.get("segments", [])
    enabled = [s for s in segments if s.get("enabled", True)]

    if segment_index >= len(enabled):
        log.error(f"Segment index {segment_index} out of range ({len(enabled)} enabled)")
        return

    seg = enabled[segment_index]
    name = seg["name"]
    concurrency = config.get("concurrency", 5)
    zones = config.get("zones", ["75"])
    expand = config.get("expand_arrondissements", False)
    sources = seg.get("sources", ["gmaps", "pagesjaunes"])
    exclude_kw = seg.get("exclude", [])

    config_dir = os.path.dirname(os.path.abspath(config.get("_config_path", "config/segments.json")))
    zone_data = load_zones(config_dir)
    dept_map = {d["code"]: d for d in zone_data.get("departments", [])}

    log.info("=" * 60)
    log.info(f"  SEGMENT: {name}")
    log.info(f"  Concurrency: {concurrency} | Zones: {zones} | Expand arrondissements: {expand}")
    log.info(f"  Sources: {sources}")
    log.info("=" * 60)

    semaphore = asyncio.Semaphore(concurrency)
    all_leads: list[Lead] = []

    async with async_playwright() as pw:
        browser = await launch_browser(pw)

        # ── Google Maps ──
        if "gmaps" in sources:
            log.info(f"  [{name}] Phase: Google Maps...")
            gmaps_tasks = []

            if expand and "75" in zones:
                # MASSIVE MODE: expand each query × 20 arrondissements
                expanded = expand_queries_by_arrondissement(seg.get("gmaps_queries", []))
                log.info(f"  [{name}] Expanded: {len(seg.get('gmaps_queries', []))} queries × 20 arrondissements = {len(expanded)} searches")
                for query, lat, lng in expanded:
                    gmaps_tasks.append(
                        gmaps.scrape_query(browser, query, name, exclude_kw, semaphore, zone_lat=lat, zone_lng=lng)
                    )
            else:
                for query_raw in seg.get("gmaps_queries", []):
                    for zone_code in zones:
                        zone = dept_map.get(zone_code, {"lat": 48.8566, "lng": 2.3522, "label": "Paris"})
                        query = f"{query_raw} {zone.get('label', 'Paris')}"
                        gmaps_tasks.append(
                            gmaps.scrape_query(browser, query, name, exclude_kw, semaphore, zone_lat=zone.get("lat", 48.8566), zone_lng=zone.get("lng", 2.3522))
                        )

            # Process in batches to avoid overwhelming
            batch_size = concurrency * 3
            for i in range(0, len(gmaps_tasks), batch_size):
                batch = gmaps_tasks[i:i + batch_size]
                results = await asyncio.gather(*batch, return_exceptions=True)
                for r in results:
                    if isinstance(r, list):
                        all_leads.extend(r)
                    elif isinstance(r, Exception):
                        log.error(f"  [GMaps] Task error: {r}")
                # Log progress
                done = min(i + batch_size, len(gmaps_tasks))
                unique_so_far = len(deduplicate([l for l in all_leads if l.is_valid()]))
                log.info(f"  [{name}] GMaps progress: {done}/{len(gmaps_tasks)} queries done, ~{unique_so_far} unique leads")

            gmaps_count = len([l for l in all_leads if l.source == "Google Maps"])
            log.info(f"  [{name}] Google Maps DONE: {gmaps_count} leads bruts")

        # ── Pages Jaunes ──
        if "pagesjaunes" in sources:
            log.info(f"  [{name}] Phase: Pages Jaunes...")
            pj_count_before = len(all_leads)
            max_pages = config.get("pj_max_pages", 5)

            for query in seg.get("pj_queries", []):
                for zone_code in zones:
                    zone = dept_map.get(zone_code, {"pj_slug": "Paris+(75)"})
                    try:
                        leads = await pagesjaunes.scrape_query(
                            browser, query, name, exclude_kw,
                            pj_slug=zone.get("pj_slug", "Paris+(75)"),
                            max_pages=max_pages,
                        )
                        all_leads.extend(leads)
                    except Exception as e:
                        log.error(f"  [PJ] Error: {e}")

            pj_count = len(all_leads) - pj_count_before
            log.info(f"  [{name}] Pages Jaunes DONE: {pj_count} leads bruts")

        # ── Annuaire.com ──
        if "annuaire" in sources:
            log.info(f"  [{name}] Phase: Annuaire.com...")
            ann_count_before = len(all_leads)

            for query in seg.get("pj_queries", []):  # reuse PJ queries
                for zone_code in zones:
                    location = f"paris-{zone_code}" if zone_code == "75" else zone_code
                    try:
                        leads = await annuaire.scrape_query(
                            browser, query, name, exclude_kw,
                            location=location,
                        )
                        all_leads.extend(leads)
                    except Exception as e:
                        log.error(f"  [Annuaire] Error: {e}")

            ann_count = len(all_leads) - ann_count_before
            log.info(f"  [{name}] Annuaire.com DONE: {ann_count} leads bruts")

        # ── Societe.com ──
        if "societe" in sources:
            log.info(f"  [{name}] Phase: Societe.com...")
            soc_count_before = len(all_leads)

            for query in seg.get("pj_queries", [])[:3]:  # limit to top 3 queries (slow source)
                for zone_code in zones:
                    try:
                        leads = await societe.scrape_query(
                            browser, query, name, exclude_kw,
                            postal_code=zone_code,
                        )
                        all_leads.extend(leads)
                    except Exception as e:
                        log.error(f"  [Societe] Error: {e}")

            soc_count = len(all_leads) - soc_count_before
            log.info(f"  [{name}] Societe.com DONE: {soc_count} leads bruts")

        await browser.close()

    # ── Save results ──
    safe_name = name.lower().replace(" ", "_").replace("&", "").replace("é", "e")[:30]
    csv_path = os.path.join(output_dir, f"segment_{segment_index}_{safe_name}.csv")
    valid = save_csv(all_leads, csv_path, zones)

    phones = len([l for l in valid if l.telephone])
    sites = len([l for l in valid if l.site_web])
    log.info(f"  [{name}] FINAL: {len(valid)} leads | {phones} tel | {sites} sites")
    log.info(f"  Saved: {csv_path}")
    return valid


async def main():
    parser = argparse.ArgumentParser(description="ClimRush Scraper Engine")
    parser.add_argument("--config", required=True, help="Path to segments.json")
    parser.add_argument("--segment-index", type=int, required=True, help="Index of segment to scrape")
    parser.add_argument("--output-dir", default="output/", help="Output directory")
    args = parser.parse_args()

    config = load_config(args.config)
    config["_config_path"] = args.config
    os.makedirs(args.output_dir, exist_ok=True)

    t0 = time.time()
    log.info("=" * 60)
    log.info("  CLIMRUSH SCRAPER ENGINE")
    log.info(f"  Config: {args.config}")
    log.info(f"  Segment index: {args.segment_index}")
    log.info(f"  Output: {args.output_dir}")
    log.info("=" * 60)

    await scrape_segment(config, args.segment_index, args.output_dir)

    elapsed = time.time() - t0
    log.info(f"  Done in {elapsed:.0f}s")


if __name__ == "__main__":
    asyncio.run(main())
