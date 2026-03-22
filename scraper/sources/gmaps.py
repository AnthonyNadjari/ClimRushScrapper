"""Google Maps scraping: scroll feed, click listings, extract details."""

import asyncio
import logging
import re
from typing import Optional

from ..models import Lead
from ..utils import normalize_phone, extract_cp
from ..browser import new_context, new_stealth_page, accept_cookies, USER_AGENTS

log = logging.getLogger("climrush")

MAX_RETRIES = 3
RETRY_DELAY = 4


async def _wait_detail(page, timeout_ms: int = 5000) -> bool:
    try:
        await page.wait_for_function(
            """() => {
                const h1 = document.querySelector('h1');
                if (!h1) return false;
                const t = h1.innerText.trim().toLowerCase();
                return t && t !== 'résultats' && t !== 'resultats' && t.length > 1;
            }""",
            timeout=timeout_ms,
        )
        return True
    except Exception:
        return False


async def _scroll_feed(page, max_scrolls: int = 35) -> int:
    feed = await page.query_selector('div[role="feed"]')
    if not feed:
        return 0
    prev_count = 0
    no_change = 0
    for _ in range(max_scrolls):
        items = await page.query_selector_all('div[role="feed"] > div a[aria-label]')
        count = len(items)
        if count == prev_count:
            no_change += 1
            if no_change >= 3:
                break
        else:
            no_change = 0
        prev_count = count
        await page.evaluate(
            'const f = document.querySelector(\'div[role="feed"]\'); if(f) f.scrollTop = f.scrollHeight;'
        )
        await asyncio.sleep(0.5)
        end = await page.query_selector("span.HlvSq")
        if end:
            break
    return prev_count


async def _extract_detail(page, segment: str, fallback_name: str = "") -> Lead:
    lead = Lead(segment=segment, source="Google Maps")
    try:
        h1 = await page.query_selector("h1")
        if h1:
            name = (await h1.inner_text()).strip()
            if name.lower() in ["résultats", "resultats", "google maps", ""]:
                lead.nom_entreprise = fallback_name
            else:
                lead.nom_entreprise = name
        else:
            lead.nom_entreprise = fallback_name

        if lead.nom_entreprise:
            lead.nom_entreprise = re.sub(r"(?i)sponsoris[ée].*", "", lead.nom_entreprise).strip()
            lead.nom_entreprise = re.sub(r"(?i)par booking\.com.*", "", lead.nom_entreprise).strip()

        addr_btn = await page.query_selector('button[data-item-id="address"]')
        if addr_btn:
            addr_text = (await addr_btn.inner_text()).strip()
            lead.adresse = addr_text
            cp = extract_cp(addr_text)
            if cp:
                lead.code_postal = cp
            # Extract city from address
            for city_match in re.finditer(r"(\d{5})\s+([A-ZÀ-Ü][a-zà-ü\-]+(?:\s+[A-ZÀ-Ü][a-zà-ü\-]+)*)", addr_text):
                lead.ville = city_match.group(2)
                break
            if not lead.ville and "paris" in addr_text.lower():
                lead.ville = "Paris"
            if lead.ville and not lead.code_postal:
                lead.code_postal = "75000"

        phone_btn = await page.query_selector('button[data-item-id^="phone"]')
        if phone_btn:
            lead.telephone = normalize_phone((await phone_btn.inner_text()).strip())

        web_link = await page.query_selector('a[data-item-id="authority"]')
        if web_link:
            href = await web_link.get_attribute("href") or ""
            if href and href != "#":
                lead.site_web = href

        rating_el = await page.query_selector("div.fontDisplayLarge")
        if rating_el:
            lead.note_google = (await rating_el.inner_text()).strip().replace(",", ".")

        reviews_el = await page.query_selector('span[aria-label*="avis"]')
        if reviews_el:
            aria = await reviews_el.get_attribute("aria-label") or ""
            m = re.search(r"(\d[\d\s]*)", aria)
            if m:
                lead.nb_avis = m.group(1).replace(" ", "")

    except Exception as e:
        log.debug(f"  GMaps extract error: {e}")
    return lead


async def _process_item(page, item, idx, total, segment, exclude_kw) -> Optional[Lead]:
    try:
        name = await item.get_attribute("aria-label") or ""
        if name.startswith("Visiter") or name.startswith("Itin") or not name:
            return None
        if any(kw.upper() in name.upper() for kw in exclude_kw):
            return None
        await item.click()
        await asyncio.sleep(0.4)
        await _wait_detail(page, timeout_ms=5000)
        lead = await _extract_detail(page, segment, fallback_name=name)
        if lead.is_valid():
            tel = lead.telephone or "no tel"
            log.info(f"    [GMaps] {idx+1}/{total}: {lead.nom_entreprise[:40]} | {tel}")
            return lead
        return None
    except Exception:
        return None


async def scrape_query(
    browser,
    query: str,
    segment: str,
    exclude_kw: list[str],
    semaphore: asyncio.Semaphore,
    zone_lat: float = 48.8566,
    zone_lng: float = 2.3522,
) -> list[Lead]:
    """Scrape a single Google Maps query. Semaphore controls concurrency."""
    async with semaphore:
        for attempt in range(MAX_RETRIES + 1):
            ctx = None
            page = None
            try:
                log.info(f"  [GMaps:{segment[:18]}] Searching: '{query}'")
                ctx = await new_context(browser, lat=zone_lat, lng=zone_lng)
                page = await new_stealth_page(ctx)

                url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(1.5)
                await accept_cookies(page)

                total = await _scroll_feed(page, max_scrolls=35)
                log.info(f"  [GMaps:{segment[:18]}] Found {total} listings for '{query}'")

                items = await page.query_selector_all('div[role="feed"] > div a[aria-label]')
                leads = []
                for i, item in enumerate(items):
                    lead = await _process_item(page, item, i, len(items), segment, exclude_kw)
                    if lead:
                        leads.append(lead)
                    await asyncio.sleep(0.25)

                log.info(f"  [GMaps:{segment[:18]}] '{query}' -> {len(leads)} leads")
                return leads

            except Exception as e:
                if attempt < MAX_RETRIES:
                    log.warning(f"  [GMaps:{segment[:18]}] Retry {attempt+1}/{MAX_RETRIES} for '{query}': {e}")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    log.error(f"  [GMaps:{segment[:18]}] FAILED after {MAX_RETRIES} retries: '{query}'")
                    return []
            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass
                if ctx:
                    try:
                        await ctx.close()
                    except Exception:
                        pass
