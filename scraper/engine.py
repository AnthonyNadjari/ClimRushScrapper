"""
ClimRush Scraper Engine — CLI entry point.
Runs a single segment from the config (for GitHub Actions matrix parallelism).

Usage:
    python -m scraper.engine --config config/segments.json --segment-index 0 --output-dir output/
"""

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
from .sources import gmaps, pagesjaunes

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


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_zones(config_dir: str) -> dict:
    zones_path = os.path.join(config_dir, "zones.json")
    if os.path.exists(zones_path):
        with open(zones_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"departments": [{"code": "75", "label": "Paris", "pj_slug": "Paris+(75)", "lat": 48.8566, "lng": 2.3522}]}


async def scrape_segment(config: dict, segment_index: int, output_dir: str):
    """Scrape a single segment with all configured sources."""
    segments = config.get("segments", [])
    # Filter to enabled only
    enabled = [s for s in segments if s.get("enabled", True)]

    if segment_index >= len(enabled):
        log.error(f"Segment index {segment_index} out of range (only {len(enabled)} enabled segments)")
        return

    seg = enabled[segment_index]
    name = seg["name"]
    concurrency = config.get("concurrency", 4)
    zones = config.get("zones", ["75"])
    sources = seg.get("sources", ["gmaps", "pagesjaunes"])
    exclude_kw = seg.get("exclude", [])

    # Load zone data
    config_dir = os.path.dirname(os.path.abspath(config.get("_config_path", "config/segments.json")))
    zone_data = load_zones(config_dir)
    dept_map = {d["code"]: d for d in zone_data.get("departments", [])}

    log.info("=" * 60)
    log.info(f"  SEGMENT: {name}")
    log.info(f"  Concurrency: {concurrency} | Zones: {zones}")
    log.info(f"  Sources: {sources}")
    log.info("=" * 60)

    semaphore = asyncio.Semaphore(concurrency)
    all_leads: list[Lead] = []

    async with async_playwright() as pw:
        browser = await launch_browser(pw)

        # ── Google Maps ──
        if "gmaps" in sources:
            log.info(f"  [{name}] Phase 1: Google Maps...")
            gmaps_tasks = []
            for query_raw in seg.get("gmaps_queries", []):
                for zone_code in zones:
                    zone = dept_map.get(zone_code, {"lat": 48.8566, "lng": 2.3522})
                    # Append zone to query if not already in it
                    query = query_raw
                    if not any(z in query.lower() for z in ["paris", "boulogne", "neuilly", "92", "93", "94"]):
                        zone_label = zone.get("label", "Paris")
                        query = f"{query_raw} {zone_label}"

                    gmaps_tasks.append(
                        gmaps.scrape_query(
                            browser, query, name, exclude_kw, semaphore,
                            zone_lat=zone.get("lat", 48.8566),
                            zone_lng=zone.get("lng", 2.3522),
                        )
                    )

            results = await asyncio.gather(*gmaps_tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    all_leads.extend(r)
                elif isinstance(r, Exception):
                    log.error(f"  [GMaps] Task error: {r}")

            gmaps_count = len([l for l in all_leads if l.source == "Google Maps"])
            log.info(f"  [{name}] Google Maps: {gmaps_count} leads bruts")

        # ── Pages Jaunes ──
        if "pagesjaunes" in sources:
            log.info(f"  [{name}] Phase 2: Pages Jaunes...")
            pj_count_before = len(all_leads)
            for query in seg.get("pj_queries", []):
                for zone_code in zones:
                    zone = dept_map.get(zone_code, {"pj_slug": "Paris+(75)"})
                    try:
                        leads = await pagesjaunes.scrape_query(
                            browser, query, name, exclude_kw,
                            pj_slug=zone.get("pj_slug", "Paris+(75)"),
                        )
                        all_leads.extend(leads)
                    except Exception as e:
                        log.error(f"  [PJ] Error: {e}")

            pj_count = len(all_leads) - pj_count_before
            log.info(f"  [{name}] Pages Jaunes: {pj_count} leads bruts")

        await browser.close()

    # ── Save results ──
    allowed_cp = zones
    safe_name = name.lower().replace(" ", "_").replace("&", "").replace("é", "e")[:30]
    csv_path = os.path.join(output_dir, f"segment_{segment_index}_{safe_name}.csv")
    valid = save_csv(all_leads, csv_path, allowed_cp)

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
