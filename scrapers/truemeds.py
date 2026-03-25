import re
from urllib.parse import urlparse

from playwright.async_api import TimeoutError

from .base import human_delay
from .matching import AUTO_ACCEPT_SCORE, NEAR_MATCH_SCORE, score_parsed
from utils.medicine_parser import parse_medicine

BASE_URL = "https://www.truemeds.in"


def extract_numbers(text: str) -> list[str]:
    return re.findall(r"\d+", text or "")


async def close_blocking_modal(page) -> None:
    close_selectors = [
        "button[aria-label='Close']",
        "button[aria-label*='close' i]",
        "button[class*='close']",
        "[role='dialog'] button",
        "[role='presentation'] button",
        ".MuiDialog-root button",
    ]

    for selector in close_selectors:
        try:
            button = await page.query_selector(selector)
            if button:
                await button.click(force=True, timeout=3000)
                await human_delay(1, 2)
                return
        except Exception:
            continue

    try:
        await page.evaluate(
            """
            () => {
                const modalRoots = document.querySelectorAll(
                    '.MuiDialog-root, [role="dialog"], .MuiModal-root'
                );
                for (const node of modalRoots) {
                    node.remove();
                }
            }
            """
        )
    except Exception:
        pass


async def scrape_truemeds(medicine, page):
    results = []
    best_candidate = None
    best_score = -1

    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
    await human_delay(2, 4)
    await close_blocking_modal(page)

    search_input = await page.wait_for_selector("#searchInput", timeout=20000)
    await search_input.click(force=True)
    await search_input.fill(medicine.canonicalName)
    await human_delay(3, 6)

    try:
        await page.wait_for_selector("div.sc-a35f2f57-2", timeout=20000)
    except TimeoutError:
        print("  TRUEMEDS: no results container")
        return []

    items = await page.query_selector_all("div.sc-17296275-0")

    for item in items[:10]:
        try:
            name_el = await item.query_selector("p.sc-17296275-3.eFNfxd")
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

            link_el = await item.query_selector("a")
            href = await link_el.get_attribute("href") if link_el else ""
            href = href.strip() if href else ""
            if not href:
                continue

            product_url = href if href.startswith("http") else BASE_URL + href
            endpoint = urlparse(product_url).path

            price_el = await item.query_selector("p.sc-17296275-8.cgGPNE")
            price = (await price_el.inner_text()).strip() if price_el else None

            mrp_el = await item.query_selector("span.sc-17296275-6.gBsAGy")
            original_price = (await mrp_el.inner_text()).strip() if mrp_el else None

            discount_el = await item.query_selector("span.sc-17296275-7.dRtIYQ")
            discount = (await discount_el.inner_text()).strip() if discount_el else None

            pack = None
            nums = extract_numbers(name)
            if nums:
                pack = nums[-1]

            candidate = {
                "source": "TRUEMEDS",
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
        except Exception as exc:
            print(f"  TRUEMEDS item error: {exc}")
            continue

    if best_candidate:
        results.append(best_candidate)

    return results
