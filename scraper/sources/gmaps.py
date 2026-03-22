"""Google Maps scraping: scroll feed, extract from feed first, click only when needed."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

from ..models import Lead
from ..utils import normalize_phone, extract_cp
from ..browser import new_context, new_stealth_page, accept_cookies, USER_AGENTS

log = logging.getLogger("climrush")

MAX_RETRIES = 2
RETRY_DELAY = 3


async def _scroll_feed(page, max_scrolls: int = 15) -> int:
    """Scroll the GMaps feed to load listings. Returns count of items found."""
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
            if no_change >= 2:
                break
        else:
            no_change = 0
        prev_count = count
        await page.evaluate(
            'const f = document.querySelector(\'div[role="feed"]\'); if(f) f.scrollTop = f.scrollHeight;'
        )
        await asyncio.sleep(0.4)
        end = await page.query_selector("span.HlvSq")
        if end:
            break
    return prev_count


async def _extract_from_feed(item) -> dict:
    """Extract data from the feed listing without clicking into details."""
    info = {"name": "", "address": "", "rating": "", "reviews": "", "phone": "", "website": ""}

    try:
        info["name"] = (await item.get_attribute("aria-label")) or ""

        # Navigate to the parent container that holds all info for this listing
        parent_text = await item.evaluate_handle(
            """el => {
                let container = el.parentElement;
                for (let i = 0; i < 5; i++) {
                    if (!container) break;
                    if (container.querySelectorAll('span').length > 3) break;
                    container = container.parentElement;
                }
                return container;
            }"""
        )

        if parent_text:
            all_text = await parent_text.evaluate("el => el ? el.innerText : ''")

            if all_text:
                # Rating (e.g. "4,5" or "4.5")
                rating_match = re.search(r"(\d[.,]\d)\s*\(", all_text)
                if rating_match:
                    info["rating"] = rating_match.group(1).replace(",", ".")

                # Review count
                reviews_match = re.search(r"\((\d[\d\s\u202f]*)\)", all_text)
                if reviews_match:
                    info["reviews"] = reviews_match.group(1).replace(" ", "").replace("\u202f", "")

                # Address (lines with postal codes or street patterns)
                lines = all_text.split("\n")
                for line in lines:
                    line = line.strip()
                    if re.search(r"\b\d{5}\b", line) and len(line) > 10:
                        info["address"] = line
                        break
                    elif re.search(r"^\d+\s+(rue|av|avenue|bd|boulevard|place|passage|impasse)", line, re.IGNORECASE):
                        info["address"] = line
                        break

                # Phone in text (French format)
                phone_match = re.search(r"(?:0[1-9])[\s.]?(?:\d{2}[\s.]?){4}", all_text)
                if phone_match:
                    info["phone"] = phone_match.group(0)

            # Look for phone via tel: links
            phone_els = await parent_text.evaluate(
                """el => {
                    if (!el) return '';
                    const links = el.querySelectorAll('a[href^="tel:"]');
                    for (const l of links) return l.href.replace('tel:', '');
                    return '';
                }"""
            )
            if phone_els:
                info["phone"] = phone_els

            # Website link
            website = await parent_text.evaluate(
                """el => {
                    if (!el) return '';
                    const links = el.querySelectorAll('a[data-item-id="authority"]');
                    for (const l of links) return l.href || '';
                    return '';
                }"""
            )
            if website:
                info["website"] = website

    except Exception as e:
        log.debug(f"  GMaps feed extract error: {e}")

    return info


async def _extract_detail(page, segment: str, fallback_name: str = "") -> Lead:
    """Extract full details from a clicked listing's detail panel."""
    lead = Lead(segment=segment, source="Google Maps")
    try:
        h1 = await page.query_selector("h1")
        if h1:
            name = (await h1.inner_text()).strip()
            if name.lower() in ["resultats", "résultats", "google maps", ""]:
                lead.nom_entreprise = fallback_name
            else:
                lead.nom_entreprise = name
        else:
            lead.nom_entreprise = fallback_name

        if lead.nom_entreprise:
            lead.nom_entreprise = re.sub(r"(?i)sponsoris[ée].*", "", lead.nom_entreprise).strip()

        addr_btn = await page.query_selector('button[data-item-id="address"]')
        if addr_btn:
            addr_text = (await addr_btn.inner_text()).strip()
            lead.adresse = addr_text
            cp = extract_cp(addr_text)
            if cp:
                lead.code_postal = cp
            if "paris" in addr_text.lower():
                lead.ville = "Paris"

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


async def _build_lead_from_feed(feed_info: dict, segment: str) -> Lead:
    """Build a Lead from feed-extracted data (no click needed)."""
    lead = Lead(segment=segment, source="Google Maps")
    lead.nom_entreprise = feed_info.get("name", "")

    if lead.nom_entreprise:
        lead.nom_entreprise = re.sub(r"(?i)sponsoris[ée].*", "", lead.nom_entreprise).strip()

    addr = feed_info.get("address", "")
    if addr:
        lead.adresse = addr
        cp = extract_cp(addr)
        if cp:
            lead.code_postal = cp
        if "paris" in addr.lower():
            lead.ville = "Paris"

    if feed_info.get("rating"):
        lead.note_google = feed_info["rating"]
    if feed_info.get("reviews"):
        lead.nb_avis = feed_info["reviews"]
    if feed_info.get("phone"):
        lead.telephone = normalize_phone(feed_info["phone"])
    if feed_info.get("website"):
        lead.site_web = feed_info["website"]

    return lead


async def _process_item(page, item, idx, total, segment, exclude_kw, seen_names: set) -> Optional[Lead]:
    """Process a single feed item. Extract from feed first, click only if needed."""
    try:
        feed_info = await _extract_from_feed(item)
        name = feed_info.get("name", "")

        if not name or name.startswith("Visiter") or name.startswith("Itin"):
            return None
        if any(kw.upper() in name.upper() for kw in exclude_kw):
            return None

        # Deduplicate by normalized name
        norm = re.sub(r"[^A-Z0-9]", "", name.upper())
        if norm in seen_names:
            return None
        seen_names.add(norm)

        # Try to build lead from feed data first
        has_phone = bool(feed_info.get("phone"))
        has_website = bool(feed_info.get("website"))

        if has_phone or has_website:
            lead = await _build_lead_from_feed(feed_info, segment)
            if lead.is_valid():
                tel = lead.telephone or "no tel"
                log.info(f"    [GMaps] {idx+1}/{total}: {lead.nom_entreprise[:40]} | {tel} (feed)")
                return lead

        # Click into detail page for full extraction
        try:
            await item.click()
            await asyncio.sleep(0.2)
            # Wait for detail panel
            try:
                await page.wait_for_function(
                    """() => {
                        const h1 = document.querySelector('h1');
                        if (!h1) return false;
                        const t = h1.innerText.trim().toLowerCase();
                        return t && t !== 'resultats' && t !== 'résultats' && t.length > 1;
                    }""",
                    timeout=2500,
                )
            except Exception:
                pass
            lead = await _extract_detail(page, segment, fallback_name=name)
            if lead.is_valid():
                tel = lead.telephone or "no tel"
                log.info(f"    [GMaps] {idx+1}/{total}: {lead.nom_entreprise[:40]} | {tel}")
                return lead
        except Exception:
            pass

        # Last resort: accept lead with just a name from GMaps (we know it's in Paris)
        lead = await _build_lead_from_feed(feed_info, segment)
        if lead.nom_entreprise and (lead.telephone or lead.site_web):
            log.info(f"    [GMaps] {idx+1}/{total}: {lead.nom_entreprise[:40]} | fallback")
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
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(1)
                await accept_cookies(page)

                total = await _scroll_feed(page, max_scrolls=15)
                log.info(f"  [GMaps:{segment[:18]}] Found {total} listings for '{query}'")

                items = await page.query_selector_all('div[role="feed"] > div a[aria-label]')
                leads = []
                seen_names: set[str] = set()

                for i, item in enumerate(items):
                    lead = await _process_item(page, item, i, len(items), segment, exclude_kw, seen_names)
                    if lead:
                        leads.append(lead)
                    await asyncio.sleep(0.1)

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
