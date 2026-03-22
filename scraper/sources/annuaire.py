"""Annuaire.com scraping: server-rendered HTML business directory."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

from ..models import Lead
from ..utils import normalize_phone, extract_cp
from ..browser import new_context, new_stealth_page, accept_cookies

log = logging.getLogger("climrush")

MAX_RETRIES = 2
RETRY_DELAY = 3


async def _extract_results(page, segment: str) -> list[Lead]:
    """Extract business listings from the current Annuaire.com results page."""
    leads = []

    # Try multiple selectors for business cards
    containers = await page.query_selector_all("article.bg-white")
    if not containers:
        containers = await page.query_selector_all('div[class*="result-item"]')
    if not containers:
        containers = await page.query_selector_all('li[class*="result"]')
    if not containers:
        containers = await page.query_selector_all("article")
    if not containers:
        # Fallback: try generic card-like containers
        containers = await page.query_selector_all('div[class*="card"]')

    for container in containers:
        try:
            lead = Lead(segment=segment, source="Annuaire.com")

            # --- Name ---
            name_el = await container.query_selector(
                "h2 a, h3 a, h2, h3, "
                '[class*="name"] a, [class*="title"] a, '
                '[class*="denomination"], [data-name]'
            )
            if name_el:
                lead.nom_entreprise = (await name_el.inner_text()).strip()
            if not lead.nom_entreprise:
                # Try the first bold/strong text in the container
                bold_el = await container.query_selector("strong, b")
                if bold_el:
                    lead.nom_entreprise = (await bold_el.inner_text()).strip()
            if not lead.nom_entreprise:
                continue

            # --- Address ---
            addr_el = await container.query_selector(
                "address, "
                '[class*="address"], [class*="adresse"], '
                '[class*="location"], [itemprop="address"]'
            )
            if addr_el:
                addr_text = (await addr_el.inner_text()).strip()
                # Clean up multi-line addresses
                addr_text = re.sub(r"\s+", " ", addr_text).strip()
                lead.adresse = addr_text
                cp = extract_cp(addr_text)
                if cp:
                    lead.code_postal = cp
                # Extract city
                for m in re.finditer(
                    r"(\d{5})\s+([A-Z\u00c0-\u00dc][a-z\u00e0-\u00fc\-]+(?:\s+[A-Z\u00c0-\u00dc][a-z\u00e0-\u00fc\-]+)*)",
                    addr_text,
                ):
                    lead.ville = m.group(2)
                    break
                if not lead.ville and "paris" in addr_text.lower():
                    lead.ville = "Paris"
                if lead.ville and not lead.code_postal:
                    lead.code_postal = "75000"
            else:
                # Fallback: look for postal code anywhere in the container text
                container_text = (await container.inner_text()).strip()
                cp = extract_cp(container_text)
                if cp:
                    lead.code_postal = cp
                if "paris" in container_text.lower():
                    lead.ville = "Paris"

            # --- Phone ---
            phone_el = await container.query_selector(
                'a[href^="tel:"], '
                '[class*="phone"], [class*="tel"], '
                '[itemprop="telephone"]'
            )
            if phone_el:
                phone_text = await phone_el.get_attribute("href") or ""
                if phone_text.startswith("tel:"):
                    phone_text = phone_text.replace("tel:", "").strip()
                else:
                    phone_text = (await phone_el.inner_text()).strip()
                lead.telephone = normalize_phone(phone_text)

            if not lead.telephone:
                # Try to find phone in text content with regex
                container_text = (await container.inner_text()).strip()
                phone_match = re.search(r"(?:0[1-9])[\s.]?(?:\d{2}[\s.]?){4}", container_text)
                if phone_match:
                    lead.telephone = normalize_phone(phone_match.group(0))

            # --- Website ---
            web_el = await container.query_selector(
                'a[href*="http"][class*="website"], '
                'a[href*="http"][class*="site"], '
                'a[data-tracking*="website"], '
                'a[rel="nofollow"][href^="http"]'
            )
            if web_el:
                href = await web_el.get_attribute("href") or ""
                if href and not href.startswith("javascript") and "annuaire.com" not in href:
                    lead.site_web = href

            if not lead.site_web:
                # Look for any external link that's not annuaire.com itself
                links = await container.query_selector_all('a[href^="http"]')
                for link in links:
                    href = await link.get_attribute("href") or ""
                    if href and "annuaire.com" not in href and "google" not in href:
                        text = (await link.inner_text()).strip().lower()
                        if any(kw in text for kw in ["site", "web", "visiter", "voir"]):
                            lead.site_web = href
                            break

            if lead.is_valid():
                leads.append(lead)

        except Exception as e:
            log.debug(f"  [Annuaire] Extract error: {e}")
            continue

    return leads


async def _go_next_page(page) -> bool:
    """Try to navigate to the next page. Returns True if successful."""
    next_btn = await page.query_selector(
        'a[rel="next"], '
        'a[class*="next"], '
        'a:has-text("Suivant"), '
        'a:has-text("Page suivante"), '
        'li.next a, '
        '[class*="pagination"] a:last-child'
    )
    if not next_btn:
        return False

    try:
        # Check it's visible and not disabled
        is_visible = await next_btn.is_visible()
        if not is_visible:
            return False

        href = await next_btn.get_attribute("href")
        if href and href != "#":
            await next_btn.click()
            await asyncio.sleep(2)
            return True

        # If no href, try clicking anyway
        await next_btn.click()
        await asyncio.sleep(2)
        return True
    except Exception:
        return False


async def scrape_query(
    browser,
    query: str,
    segment: str,
    exclude_kw: list[str],
    location: str = "paris-75",
    max_pages: int = 3,
) -> list[Lead]:
    """Scrape Annuaire.com for a single query with pagination."""
    log.info(f"  [Annuaire:{segment[:18]}] Searching: '{query}'")

    ctx = None
    page = None
    all_leads = []

    for attempt in range(MAX_RETRIES + 1):
        try:
            ctx = await new_context(browser)
            page = await new_stealth_page(ctx)

            # Build URL: replace spaces with + for query part
            clean_query = re.sub(r"\s+", "+", query.strip())
            base_url = f"https://www.annuaire.com/recherche/{clean_query}/{location}"

            for pg in range(1, max_pages + 1):
                url = base_url if pg == 1 else f"{base_url}?page={pg}"

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(1.5)
                    if pg == 1:
                        await accept_cookies(page)

                    # Check for no results
                    no_results = await page.query_selector(
                        '[class*="no-result"], [class*="noResult"], '
                        ':has-text("Aucun resultat"), :has-text("Aucun professionnel")'
                    )
                    if no_results:
                        # Verify it's actually a "no results" message, not just containing text
                        try:
                            text = (await no_results.inner_text()).strip()
                            if len(text) < 200 and ("aucun" in text.lower() or "no result" in text.lower()):
                                log.info(f"  [Annuaire:{segment[:18]}] No results on page {pg}")
                                break
                        except Exception:
                            pass

                    leads = await _extract_results(page, segment)

                    for lead in leads:
                        if any(kw.upper() in lead.nom_entreprise.upper() for kw in exclude_kw):
                            continue
                        all_leads.append(lead)
                        tel = lead.telephone or "no tel"
                        log.info(f"    [Annuaire] {lead.nom_entreprise[:40]} | {tel}")

                    if not leads:
                        log.info(f"  [Annuaire:{segment[:18]}] No more results on page {pg}")
                        break

                    # Navigate to next page if not the last
                    if pg < max_pages:
                        has_next = await _go_next_page(page)
                        if not has_next:
                            break

                except Exception as e:
                    log.warning(f"  [Annuaire:{segment[:18]}] Page {pg} error: {e}")
                    break

            log.info(f"  [Annuaire:{segment[:18]}] '{query}' -> {len(all_leads)} leads")
            return all_leads

        except Exception as e:
            if attempt < MAX_RETRIES:
                log.warning(f"  [Annuaire:{segment[:18]}] Retry {attempt+1}/{MAX_RETRIES}: {e}")
                await asyncio.sleep(RETRY_DELAY)
            else:
                log.error(f"  [Annuaire:{segment[:18]}] FAILED: '{query}'")
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
