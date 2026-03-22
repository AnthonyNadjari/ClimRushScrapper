"""Societe.com scraping: business registry data with SIRET, legal name, address."""

from __future__ import annotations

import asyncio
import logging
import re
import urllib.parse
from typing import Optional

from ..models import Lead
from ..utils import normalize_phone, extract_cp
from ..browser import new_context, new_stealth_page, accept_cookies

log = logging.getLogger("climrush")

MAX_RETRIES = 2
RETRY_DELAY = 4


async def _extract_results(page, segment: str) -> list[Lead]:
    """Extract business listings from the current Societe.com results page."""
    leads = []

    # Societe.com results are typically in a table or list of company cards
    containers = await page.query_selector_all('div[class*="company"], div[class*="result"]')
    if not containers:
        containers = await page.query_selector_all("table.result tbody tr")
    if not containers:
        containers = await page.query_selector_all('a[href*="/societe/"]')
        if containers:
            # These are links, wrap extraction differently
            return await _extract_from_links(page, containers, segment)
    if not containers:
        # Fallback: look for any repeated structure with company info
        containers = await page.query_selector_all('[class*="entreprise"], [class*="fiche"]')

    for container in containers:
        try:
            lead = Lead(segment=segment, source="Societe.com")

            # --- Name ---
            name_el = await container.query_selector(
                'a[href*="/societe/"], h2, h3, '
                '[class*="name"], [class*="denomination"], '
                "strong, b"
            )
            if name_el:
                lead.nom_entreprise = (await name_el.inner_text()).strip()
            if not lead.nom_entreprise:
                # Try full container text, take first line
                text = (await container.inner_text()).strip()
                if text:
                    lead.nom_entreprise = text.split("\n")[0].strip()
            if not lead.nom_entreprise:
                continue

            # Clean name (remove SIRET/SIREN from name if appended)
            lead.nom_entreprise = re.sub(r"\s*\d{9,14}\s*$", "", lead.nom_entreprise).strip()

            # --- Address ---
            addr_el = await container.query_selector(
                '[class*="address"], [class*="adresse"], '
                '[class*="location"], address'
            )
            if addr_el:
                addr_text = re.sub(r"\s+", " ", (await addr_el.inner_text()).strip())
                lead.adresse = addr_text
            else:
                # Try to find address in container text
                container_text = (await container.inner_text()).strip()
                for line in container_text.split("\n"):
                    line = line.strip()
                    if re.search(r"\b\d{5}\b", line) and len(line) > 8:
                        lead.adresse = line
                        break

            if lead.adresse:
                cp = extract_cp(lead.adresse)
                if cp:
                    lead.code_postal = cp
                for m in re.finditer(
                    r"(\d{5})\s+([A-Z\u00c0-\u00dc][a-z\u00e0-\u00fc\-]+(?:\s+[A-Z\u00c0-\u00dc][a-z\u00e0-\u00fc\-]+)*)",
                    lead.adresse,
                ):
                    lead.ville = m.group(2)
                    break
                if not lead.ville and "paris" in lead.adresse.lower():
                    lead.ville = "Paris"
                if lead.ville and not lead.code_postal:
                    lead.code_postal = "75000"

            if lead.is_valid():
                leads.append(lead)

        except Exception as e:
            log.debug(f"  [Societe] Extract error: {e}")
            continue

    return leads


async def _extract_from_links(page, links, segment: str) -> list[Lead]:
    """Extract basic info when results are link-based rather than card-based."""
    leads = []
    for link in links:
        try:
            lead = Lead(segment=segment, source="Societe.com")
            lead.nom_entreprise = (await link.inner_text()).strip()
            if not lead.nom_entreprise:
                continue
            lead.nom_entreprise = re.sub(r"\s*\d{9,14}\s*$", "", lead.nom_entreprise).strip()

            href = await link.get_attribute("href") or ""
            lead.site_web = href if href.startswith("http") else f"https://www.societe.com{href}"

            # Try to get surrounding text for address
            parent = await link.evaluate_handle("el => el.parentElement")
            if parent:
                parent_text = await parent.evaluate("el => el ? el.innerText : ''")
                if parent_text:
                    for line in parent_text.split("\n"):
                        line = line.strip()
                        if re.search(r"\b\d{5}\b", line) and line != lead.nom_entreprise:
                            lead.adresse = line
                            cp = extract_cp(line)
                            if cp:
                                lead.code_postal = cp
                            if "paris" in line.lower():
                                lead.ville = "Paris"
                            break

            if lead.ville and not lead.code_postal:
                lead.code_postal = "75000"

            if lead.is_valid():
                leads.append(lead)

        except Exception as e:
            log.debug(f"  [Societe] Link extract error: {e}")
            continue

    return leads


async def _extract_detail_page(page, lead: Lead) -> Lead:
    """Extract additional details from a company's detail page on Societe.com.

    Enriches the lead with: phone, website, SIRET.
    """
    try:
        # --- Phone ---
        phone_el = await page.query_selector(
            'a[href^="tel:"], '
            '[class*="phone"], [class*="telephone"], '
            '[itemprop="telephone"]'
        )
        if phone_el:
            phone_text = await phone_el.get_attribute("href") or ""
            if phone_text.startswith("tel:"):
                phone_text = phone_text.replace("tel:", "").strip()
            else:
                phone_text = (await phone_el.inner_text()).strip()
            lead.telephone = normalize_phone(phone_text)

        # --- Website ---
        web_el = await page.query_selector(
            'a[href*="http"][class*="website"], '
            'a[href*="http"][class*="site"], '
            'a[rel="nofollow"][href^="http"]'
        )
        if web_el:
            href = await web_el.get_attribute("href") or ""
            if href and "societe.com" not in href and not href.startswith("javascript"):
                lead.site_web = href

        # --- Address (more detailed on detail page) ---
        if not lead.adresse:
            addr_el = await page.query_selector(
                '[itemprop="address"], [class*="address"], '
                '[class*="adresse"]'
            )
            if addr_el:
                addr_text = re.sub(r"\s+", " ", (await addr_el.inner_text()).strip())
                lead.adresse = addr_text
                cp = extract_cp(addr_text)
                if cp:
                    lead.code_postal = cp
                if "paris" in addr_text.lower() and not lead.ville:
                    lead.ville = "Paris"

    except Exception as e:
        log.debug(f"  [Societe] Detail extract error: {e}")

    return lead


async def scrape_query(
    browser,
    query: str,
    segment: str,
    exclude_kw: list[str],
    postal_code: str = "75",
    max_pages: int = 2,
) -> list[Lead]:
    """Scrape Societe.com for a single query with pagination.

    Societe.com provides business registry data. We extract what we can
    from the results page and optionally click into detail pages for
    phone/website enrichment.
    """
    log.info(f"  [Societe:{segment[:18]}] Searching: '{query}'")

    ctx = None
    page = None
    all_leads = []

    for attempt in range(MAX_RETRIES + 1):
        try:
            ctx = await new_context(browser)
            page = await new_stealth_page(ctx)

            encoded_query = urllib.parse.quote(query)
            base_url = f"https://www.societe.com/cgi-bin/search?champs={encoded_query}&code_postal={postal_code}"

            for pg in range(1, max_pages + 1):
                url = base_url if pg == 1 else f"{base_url}&page={pg}"

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=25000)
                    await asyncio.sleep(2)
                    if pg == 1:
                        await accept_cookies(page)

                    # Check for no results
                    page_text = await page.inner_text("body")
                    if any(
                        phrase in page_text.lower()
                        for phrase in ["aucun resultat", "aucune entreprise", "0 resultat"]
                    ):
                        log.info(f"  [Societe:{segment[:18]}] No results on page {pg}")
                        break

                    leads = await _extract_results(page, segment)

                    # Enrich leads: click into detail pages for missing phone/website
                    # Only do this for leads that lack both phone and website
                    enriched_leads = []
                    for lead in leads:
                        if any(kw.upper() in lead.nom_entreprise.upper() for kw in exclude_kw):
                            continue

                        if not lead.telephone and not lead.site_web:
                            # Try to click the detail link for enrichment
                            try:
                                detail_link = await page.query_selector(
                                    f'a[href*="/societe/"]:has-text("{lead.nom_entreprise[:30]}")'
                                )
                                if detail_link:
                                    href = await detail_link.get_attribute("href") or ""
                                    if href:
                                        detail_url = href if href.startswith("http") else f"https://www.societe.com{href}"
                                        await page.goto(detail_url, wait_until="domcontentloaded", timeout=15000)
                                        await asyncio.sleep(1.5)
                                        lead = await _extract_detail_page(page, lead)
                                        # Navigate back to results
                                        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                                        await asyncio.sleep(1)
                            except Exception as e:
                                log.debug(f"  [Societe] Detail enrichment error: {e}")

                        if lead.is_valid():
                            enriched_leads.append(lead)
                            tel = lead.telephone or "no tel"
                            log.info(f"    [Societe] {lead.nom_entreprise[:40]} | {tel}")

                    all_leads.extend(enriched_leads)

                    if not leads:
                        log.info(f"  [Societe:{segment[:18]}] No more results on page {pg}")
                        break

                    # Navigate to next page
                    if pg < max_pages:
                        next_btn = await page.query_selector(
                            'a[rel="next"], a:has-text("Suivant"), '
                            'a:has-text("Page suivante"), '
                            '[class*="pagination"] a[class*="next"]'
                        )
                        if not next_btn:
                            break
                        try:
                            await next_btn.click()
                            await asyncio.sleep(2)
                        except Exception:
                            break

                except Exception as e:
                    log.warning(f"  [Societe:{segment[:18]}] Page {pg} error: {e}")
                    break

            log.info(f"  [Societe:{segment[:18]}] '{query}' -> {len(all_leads)} leads")
            return all_leads

        except Exception as e:
            if attempt < MAX_RETRIES:
                log.warning(f"  [Societe:{segment[:18]}] Retry {attempt+1}/{MAX_RETRIES}: {e}")
                await asyncio.sleep(RETRY_DELAY)
            else:
                log.error(f"  [Societe:{segment[:18]}] FAILED: '{query}'")
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
