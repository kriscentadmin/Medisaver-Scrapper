import re
from urllib.parse import urlparse

from playwright.async_api import TimeoutError

from .base import human_delay
from .helpers import candidate_priority, extract_percent_discount, extract_price_from_text, first_text
from .matching import AUTO_ACCEPT_SCORE, NEAR_MATCH_SCORE, score_parsed
from utils.medicine_parser import parse_medicine

BASE_URL = "https://www.truemeds.in"


def extract_numbers(text: str) -> list[str]:
    return re.findall(r"\d+", text or "")


def normalize_text(text: str) -> str:
    normalized = (text or "").strip().lower()
    normalized = re.sub(r"\bi\s*\.\s*u\s*\.\b", "iu", normalized)
    normalized = re.sub(r"\bm\s*\.\s*i\s*\.\s*u\s*\.\b", "miu", normalized)
    normalized = re.sub(r"\bm\s*\.\s*u\s*\.\b", "mu", normalized)
    return re.sub(r"\s+", " ", normalized)


def generate_search_terms(medicine_name: str) -> list[str]:
    brand = medicine_name.split()[0].lower()
    normalized = normalize_text(medicine_name)
    numbers = extract_numbers(medicine_name)
    form = "rotacap" if "rotacap" in normalized else "tablet" if "tablet" in normalized else ""

    terms = [medicine_name]
    if numbers:
        for number in numbers:
            if int(number) > 50:
                terms.append(f"{brand} {number} {form}".strip())
                if int(number) >= 1000:
                    gm = int(number) // 1000
                    terms.append(f"{brand} {gm}gm {form}".strip())
                    terms.append(f"{brand} {gm} gm {form}".strip())
        if len(numbers) >= 2:
            terms.append(f"{brand} {'/'.join(numbers[:2])} {form}".strip())
    terms.append(f"{brand} {form}".strip())
    terms.append(f"{brand} sr {form}".strip())
    terms.append(brand)

    return list(dict.fromkeys(term for term in terms if term))


def extract_discount_text(text: str | None) -> str | None:
    return extract_percent_discount(text)


def normalize_strength_amount(value: str, unit: str) -> tuple[str, float]:
    amount = float(value)
    normalized_unit = unit.lower()
    if normalized_unit == "g":
        return ("mg", amount * 1000)
    if normalized_unit == "mcg":
        return ("mg", amount / 1000)
    if normalized_unit == "mg":
        return ("mg", amount)
    if normalized_unit == "miu":
        return ("iu", amount * 1_000_000)
    if normalized_unit == "mu":
        return ("iu", amount * 1_000_000)
    return ("iu", amount)


def extract_strength_values(text: str) -> list[tuple[str, float]]:
    normalized = normalize_text(text)
    values: set[tuple[str, float]] = set()

    combo_pattern = re.compile(r"(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s*(mg|mcg|g|iu|miu|mu)")
    for first, second, unit in combo_pattern.findall(normalized):
        values.add(normalize_strength_amount(first, unit))
        values.add(normalize_strength_amount(second, unit))

    single_pattern = re.compile(r"(\d+(?:\.\d+)?)\s*(mg|mcg|g|iu|miu|mu)")
    for value, unit in single_pattern.findall(normalized):
        values.add(normalize_strength_amount(value, unit))

    return sorted(values)


def extract_loose_strength_numbers(text: str) -> set[float]:
    normalized = normalize_text(text)
    values: set[float] = set()

    combo_pattern = re.compile(r"(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)")
    for first, second in combo_pattern.findall(normalized):
        values.add(float(first))
        values.add(float(second))

    single_pattern = re.compile(r"\b(\d+(?:\.\d+)?)\b")
    for value in single_pattern.findall(normalized):
        values.add(float(value))

    return values


def relaxed_text_match(product_name: str, medicine_name: str, expected_variant: str | None, expected_form: str | None) -> bool:
    product = normalize_text(product_name)
    query = normalize_text(medicine_name)
    brand = medicine_name.split()[0].lower()

    if brand not in product:
        return False

    normalized_variant = normalize_text(expected_variant or "")
    if normalized_variant and normalized_variant != "normal":
        if not all(token in product for token in normalized_variant.split()):
            return False

    normalized_form = normalize_text(expected_form or "")
    if normalized_form and normalized_form != "normal" and normalized_form not in product:
        return False

    query_strengths = extract_strength_values(medicine_name)
    product_strengths = extract_strength_values(product_name)
    if query_strengths:
        if "foracort" in query:
            product_values = {value for _, value in product_strengths}
            if 200 in product_values and 400 not in product_values:
                return True
        if not product_strengths:
            query_numeric_values = {value for _, value in query_strengths}
            product_numeric_values = extract_loose_strength_numbers(product_name)
            if not query_numeric_values.intersection(product_numeric_values):
                return False
            return True
        if set(query_strengths) != set(product_strengths):
            return False

    return True


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
    ref = {
        "brand": medicine.brand,
        "variant": medicine.variant,
        "strength": medicine.strength,
        "form": medicine.form,
        "canonicalName": medicine.canonicalName,
    }

    for term in generate_search_terms(medicine.canonicalName):
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
        await human_delay(2, 4)
        await close_blocking_modal(page)

        search_input = await page.wait_for_selector("#searchInput", timeout=20000)
        await search_input.click(force=True)
        await search_input.fill(term)
        await human_delay(3, 6)

        try:
            await page.wait_for_selector("div.sc-a35f2f57-2", timeout=20000)
        except TimeoutError:
            continue

        items = await page.query_selector_all("div.sc-17296275-0")

        for item in items[:10]:
            try:
                item_text = (await item.inner_text()).strip()
                name = await first_text(
                    item,
                    [
                        "p.sc-17296275-3.eFNfxd",
                        "[class*='medicine-name']",
                        "[class*='product-name']",
                        "p",
                    ],
                )
                if not name:
                    continue

                parsed_prod = await parse_medicine(name)
                score = score_parsed(ref, parsed_prod)
                relaxed_match = relaxed_text_match(
                    name,
                    medicine.canonicalName,
                    medicine.variant,
                    medicine.form,
                )
                if score < NEAR_MATCH_SCORE and not relaxed_match:
                    continue

                link_el = await item.query_selector("a")
                href = await link_el.get_attribute("href") if link_el else ""
                href = href.strip() if href else ""
                if not href:
                    continue

                product_url = href if href.startswith("http") else BASE_URL + href
                endpoint = urlparse(product_url).path

                price_text = await first_text(
                    item,
                    [
                        "p.sc-17296275-8.cgGPNE",
                        "[class*='selling-price']",
                        "[class*='price']",
                    ],
                )
                price = extract_price_from_text(price_text) if price_text else None
                if not price:
                    price = extract_price_from_text(item_text)

                original_price_text = await first_text(
                    item,
                    [
                        "span.sc-17296275-6.gBsAGy",
                        "[class*='mrp']",
                        "[class*='strike']",
                    ],
                )
                original_price = (
                    extract_price_from_text(original_price_text) if original_price_text else None
                )

                discount_text = await first_text(
                    item,
                    [
                        "span.sc-17296275-7.dRtIYQ",
                        "[class*='discount']",
                        "[class*='offer']",
                    ],
                )
                discount = extract_discount_text(discount_text)
                if not discount:
                    discount = extract_discount_text(item_text)

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
                    "_score": max(score, NEAR_MATCH_SCORE if relaxed_match else score),
                }

                if (
                    best_candidate is None
                    or candidate_priority(candidate) > candidate_priority(best_candidate)
                ):
                    best_candidate = candidate

                if candidate["_score"] >= AUTO_ACCEPT_SCORE:
                    break
            except Exception as exc:
                print(f"  TRUEMEDS item error: {exc}")
                continue

        if best_candidate and int(best_candidate.get("_score", 0)) >= NEAR_MATCH_SCORE:
            break

    if best_candidate:
        results.append(best_candidate)

    return results
