import re

from playwright.async_api import TimeoutError

from .base import human_delay
from .helpers import candidate_priority, extract_percent_discount, first_text
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
    return extract_percent_discount(text, lowercase_off=True)


def slug_to_searchable_text(href: str | None) -> str:
    if not href:
        return ""
    slug = href.rstrip("/").split("/")[-1]
    slug = re.sub(r"-\d+$", "", slug)
    return slug.replace("-", " ").upper()


async def close_popup(page) -> None:
    selectors = [
        'button.Dialog__buttonClass__lEIRu[aria-label="cross"]',
        'button[aria-label*="close"]',
        'button[class*="close"]',
        '[aria-label*="dismiss"]',
        'div[class*="popup"] button',
        '[role="dialog"] button',
        '[class*="close"] img[src*="cross"]',
    ]

    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            if not await locator.is_visible():
                continue
            await locator.click(timeout=3000)
            print(f"  -> Closed popup using {selector}")
            await human_delay(1, 2)
            return
        except Exception:
            continue

    print("  -> No visible popup close button found")


async def scrape_1mg(medicine, page):
    results = []
    best_candidate = None

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
                name = await first_text(
                    card,
                    [
                        "div.smallSemiBold.textPrimary.marginTop-4.VerticalProductTile__header__z1Knt.VerticalProductTile__htmlNodeWrapper__YlJIR",
                        "div.VerticalProductTile__header__z1Knt",
                        "div[class*='VerticalProductTile__header']",
                    ],
                )

                if len(name) < 6:
                    continue

                href = await card.get_attribute("href")
                if not href or not (href.startswith("/drugs/") or href.startswith("/otc/")):
                    continue

                parsed_prod = await parse_medicine(name)
                score = score_parsed(ref, parsed_prod)
                card_text = (await card.inner_text()).strip()

                if score < NEAR_MATCH_SCORE:
                    # Some 1mg cards omit strength in the visible title.
                    fallback_text = " ".join(
                        part for part in [name, card_text, slug_to_searchable_text(href)] if part
                    )
                    parsed_prod = await parse_medicine(fallback_text)
                    score = score_parsed(ref, parsed_prod)
                    if score < NEAR_MATCH_SCORE:
                        continue

                product_url = BASE_URL + href
                endpoint = href

                pack = await first_text(
                    card,
                    [
                        "div.xSmallRegular.textSecondary",
                        "div[class*='textSecondary']",
                        "[class*='pack']",
                    ],
                )

                raw_price = await first_text(
                    card,
                    [
                        "span.textPrimary.l5Medium",
                        "[class*='l5Medium']",
                        "[class*='price']",
                    ],
                )
                price = clean_currency_value(raw_price)
                if not price:
                    price = clean_currency_value(card_text)

                raw_mrp = await first_text(
                    card,
                    [
                        "strike.smallRegular",
                        "strike",
                        "[class*='OriginalPrice']",
                    ],
                )
                original_price = clean_currency_value(raw_mrp)

                raw_discount = await first_text(
                    card,
                    [
                        "span.successColor.smallMedium",
                        "[class*='successColor']",
                        "[class*='discount']",
                    ],
                )
                discount = clean_discount_text(raw_discount)
                if not discount:
                    discount = clean_discount_text(card_text)

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

                if (
                    best_candidate is None
                    or candidate_priority(candidate) > candidate_priority(best_candidate)
                ):
                    best_candidate = candidate

                if score >= AUTO_ACCEPT_SCORE:
                    break
            except Exception:
                continue
    except Exception as exc:
        print(f"  ERROR: 1MG error: {exc}")

    if best_candidate:
        results = [best_candidate]

    return results
