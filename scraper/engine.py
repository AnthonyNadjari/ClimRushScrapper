"""
ClimRush Scraper Engine — CLI entry point.
Runs a single segment from the config (for GitHub Actions matrix parallelism).
All 4 sources run in PARALLEL within each segment for maximum speed.

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


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_zones(config_dir: str) -> dict:
    zones_path = os.path.join(config_dir, "zones.json")
    if os.path.exists(zones_path):
        with open(zones_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"departments": [{"code": "75", "label": "Paris", "pj_slug": "Paris+(75)", "lat": 48.8566, "lng": 2.3522}]}


async def _run_gmaps(browser, seg: dict, name: str, exclude_kw: list[str], concurrency: int) -> list[Lead]:
    """Run all Google Maps queries with semaphore-controlled parallelism."""
    queries = seg.get("gmaps_queries", [])
    if not queries:
        return []

    log.info(f"  [{name}] GMaps: {len(queries)} queries (parallel, concurrency={concurrency})")
    semaphore = asyncio.Semaphore(concurrency)
    leads = []

    # Each query searches "X Paris" — no arrondissement expansion needed
    tasks = []
    for query in queries:
        full_query = f"{query} Paris"
        tasks.append(gmaps.scrape_query(browser, full_query, name, exclude_kw, semaphore))

    # Run all GMaps queries in parallel (semaphore limits concurrency)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, list):
            leads.extend(r)
        elif isinstance(r, Exception):
            log.error(f"  [GMaps] Task error: {r}")

    log.info(f"  [{name}] GMaps DONE: {len(leads)} leads bruts")
    return leads


async def _run_pagesjaunes(browser, seg: dict, name: str, exclude_kw: list[str], zone_data: dict, zones: list[str], max_pages: int) -> list[Lead]:
    """Run Pages Jaunes queries sequentially."""
    queries = seg.get("pj_queries", [])
    if not queries:
        return []

    dept_map = {d["code"]: d for d in zone_data.get("departments", [])}
    log.info(f"  [{name}] Pages Jaunes: {len(queries)} queries")
    leads = []

    for query in queries:
        for zone_code in zones:
            zone = dept_map.get(zone_code, {"pj_slug": "Paris+(75)"})
            try:
                result = await pagesjaunes.scrape_query(
                    browser, query, name, exclude_kw,
                    pj_slug=zone.get("pj_slug", "Paris+(75)"),
                    max_pages=max_pages,
                )
                leads.extend(result)
            except Exception as e:
                log.error(f"  [PJ] Error on '{query}': {e}")

    log.info(f"  [{name}] Pages Jaunes DONE: {len(leads)} leads bruts")
    return leads


async def _run_annuaire(browser, seg: dict, name: str, exclude_kw: list[str], zones: list[str]) -> list[Lead]:
    """Run Annuaire.com queries sequentially."""
    queries = seg.get("pj_queries", [])
    if not queries:
        return []

    log.info(f"  [{name}] Annuaire.com: {len(queries)} queries")
    leads = []

    for query in queries:
        for zone_code in zones:
            location = f"paris-{zone_code}" if zone_code == "75" else zone_code
            try:
                result = await annuaire.scrape_query(
                    browser, query, name, exclude_kw,
                    location=location,
                )
                leads.extend(result)
            except Exception as e:
                log.error(f"  [Annuaire] Error on '{query}': {e}")

    log.info(f"  [{name}] Annuaire.com DONE: {len(leads)} leads bruts")
    return leads


async def _run_societe(browser, seg: dict, name: str, exclude_kw: list[str], zones: list[str]) -> list[Lead]:
    """Run Societe.com queries (limited to top 3)."""
    queries = seg.get("pj_queries", [])[:3]
    if not queries:
        return []

    log.info(f"  [{name}] Societe.com: {len(queries)} queries")
    leads = []

    for query in queries:
        for zone_code in zones:
            try:
                result = await societe.scrape_query(
                    browser, query, name, exclude_kw,
                    postal_code=zone_code,
                )
                leads.extend(result)
            except Exception as e:
                log.error(f"  [Societe] Error on '{query}': {e}")

    log.info(f"  [{name}] Societe.com DONE: {len(leads)} leads bruts")
    return leads


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
    sources = seg.get("sources", ["gmaps", "pagesjaunes"])
    exclude_kw = seg.get("exclude", [])
    max_pages = config.get("pj_max_pages", 5)

    config_dir = os.path.dirname(os.path.abspath(config.get("_config_path", "config/segments.json")))
    zone_data = load_zones(config_dir)

    log.info("=" * 60)
    log.info(f"  SEGMENT: {name}")
    log.info(f"  Sources: {sources} | Concurrency: {concurrency}")
    log.info(f"  GMaps queries: {len(seg.get('gmaps_queries', []))} | PJ queries: {len(seg.get('pj_queries', []))}")
    log.info("=" * 60)

    async with async_playwright() as pw:
        browser = await launch_browser(pw)

        # === RUN ALL SOURCES IN PARALLEL ===
        source_tasks = []
        source_names = []

        if "gmaps" in sources:
            source_tasks.append(_run_gmaps(browser, seg, name, exclude_kw, concurrency))
            source_names.append("GMaps")

        if "pagesjaunes" in sources:
            source_tasks.append(_run_pagesjaunes(browser, seg, name, exclude_kw, zone_data, zones, max_pages))
            source_names.append("PJ")

        if "annuaire" in sources:
            source_tasks.append(_run_annuaire(browser, seg, name, exclude_kw, zones))
            source_names.append("Annuaire")

        if "societe" in sources:
            source_tasks.append(_run_societe(browser, seg, name, exclude_kw, zones))
            source_names.append("Societe")

        log.info(f"  [{name}] Launching {len(source_tasks)} sources in PARALLEL: {', '.join(source_names)}")

        results = await asyncio.gather(*source_tasks, return_exceptions=True)

        all_leads: list[Lead] = []
        for i, r in enumerate(results):
            if isinstance(r, list):
                all_leads.extend(r)
            elif isinstance(r, Exception):
                log.error(f"  [{source_names[i]}] Source failed: {r}")

        await browser.close()

    # === Save results ===
    safe_name = name.lower().replace(" ", "_").replace("&", "").replace("é", "e")[:30]
    csv_path = os.path.join(output_dir, f"segment_{segment_index}_{safe_name}.csv")
    valid = save_csv(all_leads, csv_path, zones)

    phones = len([l for l in valid if l.telephone])
    sites = len([l for l in valid if l.site_web])
    log.info(f"  [{name}] FINAL: {len(valid)} unique leads | {phones} tel | {sites} sites")
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
