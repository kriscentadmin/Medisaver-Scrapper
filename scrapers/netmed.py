import asyncio
import re
from urllib.parse import urlparse

from .base import human_delay
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
    best_score = -1

    search_url = SEARCH_URL.format(medicine.canonicalName.replace(" ", "+"))
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    await human_delay(3, 5)
    await close_popup_if_any(page)

    await page.wait_for_selector(".product-card-container", timeout=25000)
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

            candidate = {
                "name": name,
                "href": href,
                "_score": score,
            }

            if score > best_score:
                best_candidate = candidate
                best_score = score

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

    price = None
    price_el = await page.query_selector(".effective-price-div")
    if price_el:
        text = (await price_el.inner_text()).strip()
        match = re.search(r"[\d,.]+", text)
        if match:
            price = match.group(0)

    results.append(
        {
            "source": "NETMEDS",
            "name": best_candidate["name"],
            "pack": pack,
            "price": price,
            "productUrl": product_url,
            "endpoint": endpoint,
            "_score": best_candidate["_score"],
        }
    )

    return results
