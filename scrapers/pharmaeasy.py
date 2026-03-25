import re
from urllib.parse import urlparse

from playwright.async_api import TimeoutError

from .base import human_delay
from .matching import AUTO_ACCEPT_SCORE, NEAR_MATCH_SCORE, score_parsed
from utils.medicine_parser import parse_medicine

BASE_URL = "https://pharmeasy.in"
SEARCH_URL = "https://pharmeasy.in/search/all?name={}"


def extract_discount_only(text):
    if not text:
        return None
    match = re.search(r"(\d+\s*%\s*OFF)", text)
    return match.group(1).strip() if match else None


async def scrape_pharmeasy(medicine, page):
    results = []
    best_candidate = None
    best_score = -1

    try:
        query = medicine.canonicalName.replace(" ", "%20")
        await page.goto(
            SEARCH_URL.format(query),
            wait_until="domcontentloaded",
            timeout=45000,
        )
        await human_delay(3, 6)

        product_selector = "div[class*='ProductCard_medicineUnitContainer']"
        try:
            await page.wait_for_selector(product_selector, timeout=20000)
        except TimeoutError:
            print("  PHARMEASY: no results container")
            return []

        for _ in range(4):
            await page.mouse.wheel(0, 1200)
            await human_delay(1.5, 2.5)

        cards = await page.query_selector_all(product_selector)

        for card in cards[:20]:
            try:
                name_el = await card.query_selector("h1")
                name = (await name_el.inner_text()).strip() if name_el else ""
                if not name:
                    continue

                parsed_prod = await parse_medicine(name)

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

                link_el = await card.query_selector("a")
                href = await link_el.get_attribute("href") if link_el else ""
                if not href:
                    continue

                product_url = BASE_URL + href if href.startswith("/") else href
                endpoint = urlparse(product_url).path

                price_el = await card.query_selector("[class*='ourPrice']")
                price = (await price_el.inner_text()).strip() if price_el else None

                mrp_el = await card.query_selector("[class*='striked']")
                original_price = (await mrp_el.inner_text()).strip() if mrp_el else None

                discount_el = await card.query_selector("[class*='Discount']")
                discount_raw = (await discount_el.inner_text()).strip() if discount_el else None
                discount = extract_discount_only(discount_raw)

                pack_el = await card.query_selector("[class*='measurementUnit']")
                pack = (await pack_el.inner_text()).strip() if pack_el else None

                candidate = {
                    "source": "PHARMEASY",
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

    except Exception as e:
        print(f"PHARMEASY ERROR: {e}")

    if best_candidate:
        results.append(best_candidate)

    return results
