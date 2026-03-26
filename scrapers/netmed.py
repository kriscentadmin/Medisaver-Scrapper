import asyncio
import re
from urllib.parse import urlparse

from playwright.async_api import TimeoutError

from .base import human_delay
from .helpers import extract_percent_discount, extract_price_from_text, first_text
from .matching import AUTO_ACCEPT_SCORE, NEAR_MATCH_SCORE, score_parsed
from utils.medicine_parser import parse_medicine

BASE_URL = "https://www.netmeds.com"
SEARCH_URL = "https://www.netmeds.com/products?q={}"


def get_endpoint(url: str | None) -> str | None:
    return urlparse(url).path if url else None


def extract_pack_from_text(text: str) -> str | None:
    if not text:
        return None
    match = re.search(r"\b(\d+)\b", text)
    return match.group(1) if match else None


def extract_price_value(text: str | None) -> str | None:
    return extract_price_from_text(text)


def normalize_price_prefix(text: str | None) -> str | None:
    return extract_price_from_text(text)


def clean_discount_text(text: str | None) -> str | None:
    return extract_percent_discount(text)


async def close_popup_if_any(page) -> None:
    try:
        close_btn = await page.query_selector("#close.close")
        if close_btn:
            await close_btn.click(force=True)
            await page.wait_for_function(
                "() => document.querySelector('#close')?.style.visibility === 'hidden'",
                timeout=5000,
            )
            await asyncio.sleep(0.5)
    except Exception:
        pass


async def scrape_netmeds(medicine, page):
    results = []
    best_candidate: dict | None = None

    search_url = SEARCH_URL.format(medicine.canonicalName.replace(" ", "+"))
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    await human_delay(3, 5)
    await close_popup_if_any(page)

    try:
        await page.wait_for_selector(".product-card-container", timeout=25000)
    except TimeoutError:
        no_results = await first_text(page, [".no-results", "[class*='no-results']", "body"])
        if no_results and "no results" in no_results.lower():
            return []
        return []
    cards = await page.query_selector_all(".product-card-container")

    for card in cards[:12]:
        try:
            name_el = await card.query_selector("h3")
            name = (await name_el.inner_text()).strip() if name_el else ""
            if not name:
                continue

            parsed_prod = await parse_medicine(name)
            print(f"  NETMEDS raw name: {name}")

            score = score_parsed(
                {
                    "brand": medicine.brand,
                    "variant": medicine.variant,
                    "strength": medicine.strength,
                    "form": medicine.form,
                    "canonicalName": medicine.canonicalName,
                },
                parsed_prod,
            )

            if score < NEAR_MATCH_SCORE:
                continue

            link_el = await card.query_selector("a[href^='/product']")
            href = await link_el.get_attribute("href") if link_el else None
            if not href:
                continue

            card_text = (await card.inner_text()).strip()
            card_price = extract_price_value(card_text)
            card_discount = clean_discount_text(card_text)

            candidate = {
                "name": name,
                "href": href,
                "card_price": card_price,
                "card_discount": card_discount,
                "_score": score,
            }

            if best_candidate is None or score > int(best_candidate.get("_score", -1)):
                best_candidate = candidate

            if score >= AUTO_ACCEPT_SCORE:
                break
        except Exception as exc:
            print(f"  NETMEDS card error: {exc}")
            continue

    if not best_candidate:
        return results

    product_url = BASE_URL + best_candidate["href"]
    endpoint = get_endpoint(product_url)

    await page.goto(product_url, wait_until="domcontentloaded", timeout=45000)
    await human_delay(3, 5)
    await close_popup_if_any(page)

    pack = extract_pack_from_text(best_candidate["name"])
    pack_text = await first_text(
        page,
        [
            ".jm-body-xxxs-bold",
            ".jm-body-xxxs.jm-fc-primary-gray-80",
            "[class*='pack']",
            "[class*='Pack']",
        ],
    )
    if pack_text:
        if pack_text and "MRP" not in pack_text.upper():
            pack = pack_text

    price = None
    price_text = await first_text(
        page,
        [
            ".effective-price-div",
            "[class*='effective-price']",
            "[class*='final-price']",
            "[class*='sale-price']",
        ],
    )
    if price_text:
        price = extract_price_value(price_text)
    if not price:
        price = best_candidate.get("card_price")

    original_price = None
    mrp_text = await first_text(
        page,
        [
            ".marked-price",
            "[class*='marked-price']",
            "[class*='mrp']",
            "[class*='MRP']",
        ],
    )
    if mrp_text:
        original_price = normalize_price_prefix(mrp_text)

    discount = None
    discount_text = await first_text(
        page,
        [
            ".jm-fc-light-sparkle-80",
            "[class*='sparkle']",
            "[class*='discount']",
            "[class*='Discount']",
        ],
    )
    if discount_text:
        discount = clean_discount_text(discount_text) or discount_text
    if not discount:
        discount = best_candidate.get("card_discount")

    results.append(
        {
            "source": "NETMEDS",
            "name": best_candidate["name"],
            "pack": pack,
            "price": price,
            "originalPrice": original_price,
            "discount": discount,
            "productUrl": product_url,
            "endpoint": endpoint,
            "_score": best_candidate["_score"],
        }
    )

    return results
