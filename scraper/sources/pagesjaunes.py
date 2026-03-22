"""Pages Jaunes scraping: search, pagination, phone reveal."""

import asyncio
import logging
import urllib.parse
from typing import Optional

from ..models import Lead
from ..utils import normalize_phone, extract_cp
from ..browser import new_context, new_stealth_page, accept_cookies

log = logging.getLogger("climrush")

MAX_RETRIES = 2
RETRY_DELAY = 3
PJ_MAX_PAGES = 5


async def _extract_results(page, segment: str) -> list[Lead]:
    leads = []

    containers = await page.query_selector_all('li[id^="bi-"]')
    if not containers:
        containers = await page.query_selector_all(".bi-bloc")
    if not containers:
        containers = await page.query_selector_all('[class*="bi-content"]')
    if not containers:
        containers = await page.query_selector_all("article[data-pjlabel]")

    for container in containers:
        try:
            lead = Lead(segment=segment, source="Pages Jaunes")

            name_el = await container.query_selector(
                '.bi-denomination, .bi-header-title a, h3 a, [class*="denomination"]'
            )
            if name_el:
                lead.nom_entreprise = (await name_el.inner_text()).strip()
            if not lead.nom_entreprise:
                continue

            addr_el = await container.query_selector(
                '.bi-address .bi-adresse, .bi-address, [class*="address"], [class*="adresse"]'
            )
            if addr_el:
                addr_text = (await addr_el.inner_text()).strip()
                lead.adresse = addr_text
                cp = extract_cp(addr_text)
                if cp:
                    lead.code_postal = cp
                if "paris" in addr_text.lower():
                    lead.ville = "Paris"
                elif not lead.ville:
                    # Try to extract city from address
                    import re
                    for m in re.finditer(r"(\d{5})\s+([A-ZÀ-Ü][a-zà-ü\-]+(?:\s+[A-ZÀ-Ü][a-zà-ü\-]+)*)", addr_text):
                        lead.ville = m.group(2)
                        break
                if lead.ville and not lead.code_postal:
                    lead.code_postal = "75000"

            # Try to reveal phone
            phone_btn = await container.query_selector(
                'button[data-pjlabel*="phone"], [class*="phone"] button, '
                ".bi-phone button, button[data-pjlabel=\"tel_click\"]"
            )
            if phone_btn:
                try:
                    await phone_btn.click()
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

            phone_el = await container.query_selector(
                '.bi-phone .phone-number, .bi-phone a, [class*="phone-number"], '
                'a[href^="tel:"], [data-phone-number]'
            )
            if phone_el:
                phone_text = await phone_el.get_attribute("href") or await phone_el.inner_text()
                phone_text = phone_text.replace("tel:", "").strip()
                lead.telephone = normalize_phone(phone_text)

            web_el = await container.query_selector(
                'a[data-pjlabel*="site_internet"], a[data-pjlabel*="website"], '
                '.bi-website a, a[class*="website"]'
            )
            if web_el:
                href = await web_el.get_attribute("href") or ""
                if href and not href.startswith("javascript"):
                    lead.site_web = href

            if lead.is_valid():
                leads.append(lead)

        except Exception as e:
            log.debug(f"  [PJ] Extract error: {e}")
            continue

    return leads


async def scrape_query(
    browser,
    query: str,
    segment: str,
    exclude_kw: list[str],
    pj_slug: str = "Paris+(75)",
    max_pages: int = PJ_MAX_PAGES,
) -> list[Lead]:
    """Scrape Pages Jaunes for a single query. Sequential with pagination."""
    log.info(f"  [PJ:{segment[:18]}] Searching: '{query}'")

    ctx = None
    page = None
    all_leads = []

    for attempt in range(MAX_RETRIES + 1):
        try:
            ctx = await new_context(browser)
            page = await new_stealth_page(ctx)

            encoded = urllib.parse.quote(query)
            base_url = f"https://www.pagesjaunes.fr/annuaire/chercherlespros?quoiqui={encoded}&ou={pj_slug}"

            for pg in range(1, max_pages + 1):
                url = base_url if pg == 1 else f"{base_url}&page={pg}"

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(1.5)
                    if pg == 1:
                        await accept_cookies(page)

                    no_results = await page.query_selector('.noResult, .no-result, [class*="no-result"]')
                    if no_results:
                        break

                    leads = await _extract_results(page, segment)

                    for lead in leads:
                        if any(kw.upper() in lead.nom_entreprise.upper() for kw in exclude_kw):
                            continue
                        all_leads.append(lead)
                        tel = lead.telephone or "no tel"
                        log.info(f"    [PJ] {lead.nom_entreprise[:40]} | {tel}")

                    if not leads:
                        break

                    if pg < max_pages:
                        next_btn = await page.query_selector(
                            'a[id="pagination-next"], a.next, [class*="pagination"] a:has-text("Suivant")'
                        )
                        if not next_btn:
                            break

                    await asyncio.sleep(1)
                except Exception as e:
                    log.warning(f"  [PJ:{segment[:18]}] Page {pg} error: {e}")
                    break

            log.info(f"  [PJ:{segment[:18]}] '{query}' -> {len(all_leads)} leads")
            return all_leads

        except Exception as e:
            if attempt < MAX_RETRIES:
                log.warning(f"  [PJ:{segment[:18]}] Retry {attempt+1}/{MAX_RETRIES}: {e}")
                await asyncio.sleep(RETRY_DELAY)
            else:
                log.error(f"  [PJ:{segment[:18]}] FAILED: '{query}'")
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
