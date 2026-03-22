"""Playwright browser setup, stealth, and cookie handling."""

import asyncio
import random
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-default-apps",
    "--disable-sync",
    "--disable-translate",
    "--metrics-recording-only",
    "--no-first-run",
]


async def launch_browser(pw) -> Browser:
    return await pw.chromium.launch(headless=True, args=BROWSER_ARGS)


async def new_context(
    browser: Browser,
    lat: float = 48.8566,
    lng: float = 2.3522,
) -> BrowserContext:
    ctx = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1366, "height": 900},
        locale="fr-FR",
        timezone_id="Europe/Paris",
        geolocation={"latitude": lat, "longitude": lng},
        permissions=["geolocation"],
    )
    return ctx


async def new_stealth_page(ctx: BrowserContext) -> Page:
    page = await ctx.new_page()
    await page.add_init_script(
        'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
    )
    return page


async def accept_cookies(page: Page):
    selectors = [
        'button[aria-label*="Tout accepter"]',
        'button:has-text("Tout accepter")',
        'button:has-text("Accepter")',
        "#didomi-notice-agree-button",
        'button[id*="accept"]',
        'button:has-text("Continuer sans accepter")',
    ]
    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(0.8)
                return
        except Exception:
            continue
