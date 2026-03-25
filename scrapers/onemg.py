import re

from playwright.async_api import TimeoutError

from .base import human_delay
from .matching import AUTO_ACCEPT_SCORE, NEAR_MATCH_SCORE, score_parsed
from utils.medicine_parser import parse_medicine

BASE_URL = "https://www.1mg.com"


def clean_currency_value(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = re.sub(
        r"(?i)(Discounted Price:|Sale Price:|MRP:|(?:RS\.?|INR|\u20B9)\s*|Original Price:)",
        "",
        text,
    ).strip()
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    return cleaned if re.fullmatch(r"\d+(?:\.\d+)?", cleaned) else None


def clean_discount_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"(\d+)%?\s*off", text, re.IGNORECASE)
    return f"{match.group(1)}% off" if match else None


async def close_popup(page) -> None:
    try:
        close_btn = await page.query_selector(
            'button.Dialog__buttonClass__lEIRu[aria-label="cross"]'
        )
        if close_btn:
            await close_btn.click(timeout=8000)
            print("  -> Closed popup (Dialog cross button)")
            await human_delay(1, 2)
            return

        fallback_selectors = [
            'button[aria-label*="close"]',
            'button[class*="close"]',
            '[class*="close"] img[src*="cross"]',
            'div[class*="popup"] button',
            '[aria-label*="dismiss"]',
            '[role="dialog"] button',
        ]
        for selector in fallback_selectors:
            button = await page.query_selector(selector)
            if button:
                await button.click(timeout=6000)
                print(f"  -> Closed fallback popup using {selector}")
                await human_delay(1.5, 3)
                return

        print("  -> No popup found or already closed")
    except Exception as exc:
        print(f"  -> Popup close failed (non-critical): {exc}")
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await human_delay(2, 4)


async def scrape_1mg(medicine, page):
    results = []
    best_candidate = None
    best_score = -1

    try:
        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await human_delay(2, 4)
        await close_popup(page)

        search_input = await page.wait_for_selector(
            "input#search-medicine.Search__searchInput__kTMfA",
            timeout=25000,
            state="visible",
        )
        await search_input.click()
        await search_input.fill(medicine.canonicalName)
        await human_delay(1, 2.5)
        await search_input.press("Enter")

        print(f"  -> Searched: {medicine.canonicalName}")
        await human_delay(4, 8)

        try:
            await page.wait_for_selector(
                "div.mTop-20.flex.flexWrap.SearchResultContainer__skuListContainer__qQlVM",
                timeout=35000,
            )
        except TimeoutError:
            print("  WARNING: No results container appeared")
            return []

        for _ in range(6):
            await page.evaluate("window.scrollBy(0, 1800)")
            await human_delay(2.5, 4)

        cards = await page.query_selector_all(
            "div.flex.SearchResultContainer__cardContainer__dgEls a.noAnchorColor.width-100"
        )
        print(f"  -> Found {len(cards)} product cards")

        ref = {
            "brand": (medicine.brand or "").strip().upper(),
            "variant": (medicine.variant or "NORMAL").strip().upper(),
            "strength": (medicine.strength or "UNKNOWN").strip().upper(),
            "form": (medicine.form or "NORMAL").strip().upper(),
            "canonicalName": (medicine.canonicalName or "").strip().upper(),
        }

        for card in cards[:20]:
            try:
                name_el = await card.query_selector(
                    "div.smallSemiBold.textPrimary.marginTop-4.VerticalProductTile__header__z1Knt.VerticalProductTile__htmlNodeWrapper__YlJIR"
                )
                name = (await name_el.inner_text()).strip() if name_el else ""
                if not name:
                    fallback_name_el = await card.query_selector(
                        "div.VerticalProductTile__header__z1Knt"
                    )
                    name = (
                        (await fallback_name_el.inner_text()).strip()
                        if fallback_name_el
                        else ""
                    )

                if len(name) < 6:
                    continue

                parsed_prod = await parse_medicine(name)
                score = score_parsed(ref, parsed_prod)
                if score < NEAR_MATCH_SCORE:
                    continue

                href = await card.get_attribute("href")
                if not href or not href.startswith("/drugs/"):
                    continue

                product_url = BASE_URL + href
                endpoint = href

                pack_el = await card.query_selector("div.xSmallRegular.textSecondary")
                pack = (await pack_el.inner_text()).strip() if pack_el else None

                price_el = await card.query_selector("span.textPrimary.l5Medium")
                raw_price = (await price_el.inner_text()).strip() if price_el else None
                price = clean_currency_value(raw_price)

                mrp_el = await card.query_selector("strike.smallRegular")
                raw_mrp = (await mrp_el.inner_text()).strip() if mrp_el else None
                original_price = clean_currency_value(raw_mrp)

                discount_el = await card.query_selector("span.successColor.smallMedium")
                raw_discount = (
                    (await discount_el.inner_text()).strip() if discount_el else None
                )
                discount = clean_discount_text(raw_discount)

                candidate = {
                    "source": "ONEMG",
                    "name": name,
                    "pack": pack,
                    "price": price,
                    "originalPrice": original_price,
                    "discount": discount,
                    "productUrl": product_url,
                    "endpoint": endpoint,
                    "_score": score,
                }

                if score > best_score:
                    best_candidate = candidate
                    best_score = score

                if score >= AUTO_ACCEPT_SCORE:
                    break
            except Exception:
                continue
    except Exception as exc:
        print(f"  ERROR: 1MG error: {exc}")

    if best_candidate:
        results = [best_candidate]

    return results
