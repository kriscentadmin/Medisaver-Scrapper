import re
from urllib.parse import urlparse

from playwright.async_api import TimeoutError

from .base import human_delay
from .helpers import candidate_priority, extract_percent_discount, extract_price_from_text, first_text
from .matching import AUTO_ACCEPT_SCORE, NEAR_MATCH_SCORE, score_parsed
from utils.medicine_parser import parse_medicine

BASE_URL = "https://pharmeasy.in"
SEARCH_URL = "https://pharmeasy.in/search/all?name={}"


def extract_discount_only(text: str | None) -> str | None:
    return extract_percent_discount(text)


def generate_search_terms(medicine) -> list[str]:
    terms = []
    brand = (medicine.brand or "").strip()
    variant = (medicine.variant or "").strip()
    canonical_name = (medicine.canonicalName or "").strip()

    if brand:
        terms.append(brand)
    if brand and variant and variant.upper() != "NORMAL":
        terms.append(f"{brand} {variant}")
    if canonical_name:
        terms.append(canonical_name)

    return list(dict.fromkeys(term for term in terms if term))


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def extract_strengths(text: str) -> list[float]:
    normalized = normalize_text(text)
    strengths: list[float] = []

    for first, second in re.findall(r"(\d+\.?\d*)\s*/\s*(\d+\.?\d*)", normalized):
        strengths.append(float(first))
        strengths.append(float(second))

    for value, unit in re.findall(r"(\d+\.?\d*)\s*(mg|mcg|g)", normalized):
        amount = float(value)
        if unit == "g":
            amount *= 1000
        elif unit == "mcg":
            amount /= 1000
        strengths.append(amount)

    return sorted(set(strengths))


def extract_pack_count(text: str) -> int | None:
    match = re.search(r"(\d+)\s*(tablet|capsule|rotacap)", normalize_text(text))
    return int(match.group(1)) if match else None


def extract_brand(search_text: str) -> str:
    parts = normalize_text(search_text).split()
    brand_parts = []
    for part in parts:
        if re.search(r"\d", part):
            break
        brand_parts.append(part)
    return " ".join(brand_parts)


def brand_and_variant_match(
    search_text: str,
    product_text: str,
    expected_variant: str | None,
) -> bool:
    search = normalize_text(search_text)
    product = normalize_text(product_text)
    brand = extract_brand(search)

    if not brand or not product.startswith(brand):
        return False

    normalized_variant = normalize_text(expected_variant or "")
    if normalized_variant and normalized_variant != "normal":
        variant_tokens = normalized_variant.split()
        if not all(token in product for token in variant_tokens):
            return False

    return True


def strength_match(search_text: str, product_text: str) -> bool:
    search_strengths = extract_strengths(search_text)
    product_strengths = extract_strengths(product_text)

    if not search_strengths:
        return True
    if len(search_strengths) != len(product_strengths):
        return False

    for search_strength in search_strengths:
        if not any(abs(search_strength - product_strength) < 0.1 for product_strength in product_strengths):
            return False

    return True


def pharmaeasy_exact_match(search_text: str, product_text: str, expected_variant: str | None) -> bool:
    if not brand_and_variant_match(search_text, product_text, expected_variant):
        return False

    search_pack = extract_pack_count(search_text)
    product_pack = extract_pack_count(product_text)
    if search_pack and product_pack and abs(search_pack - product_pack) > 10:
        return False

    return True


def pharmaeasy_candidate_priority(candidate: dict) -> tuple[int, int, int, int]:
    return (
        1 if candidate.get("_exact_match") else 0,
        *candidate_priority(candidate),
    )


def pharmaeasy_effective_score(parsed_score: int, exact_match: bool) -> int:
    if exact_match and parsed_score < NEAR_MATCH_SCORE:
        return NEAR_MATCH_SCORE
    return parsed_score


def is_unavailable_card(text: str | None) -> bool:
    if not text:
        return False
    normalized = text.strip().lower()
    markers = [
        "out of stock",
        "price to be updated",
        "notify me",
        "we do not sell this product",
    ]
    return any(marker in normalized for marker in markers)


async def close_popup_if_any(page) -> None:
    selectors = [
        "span.CT_InterstitialClose",
        "span[onclick*='closeIframe']",
        "[class*='InterstitialClose']",
    ]

    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            if not await locator.is_visible():
                continue
            await locator.click(timeout=3000)
            print(f"  PHARMEASY: closed popup using {selector}")
            await human_delay(1, 2)
            return
        except Exception:
            continue


async def scrape_pharmeasy(medicine, page):
    results = []
    best_candidate = None

    try:
        product_selector = "div[class*='ProductCard_medicineUnitContainer']"

        for term in generate_search_terms(medicine):
            query = term.replace(" ", "%20")
            await page.goto(
                SEARCH_URL.format(query),
                wait_until="domcontentloaded",
                timeout=45000,
            )
            await human_delay(3, 6)
            await close_popup_if_any(page)

            try:
                await page.wait_for_selector(product_selector, timeout=20000)
            except TimeoutError:
                continue

            for _ in range(4):
                await page.mouse.wheel(0, 1200)
                await human_delay(1.5, 2.5)

            cards = await page.query_selector_all(product_selector)

            for card in cards[:20]:
                try:
                    card_text = await card.inner_text()
                    if is_unavailable_card(card_text):
                        continue

                    name = await first_text(card, ["h1", "h2", "[class*='ProductCard_medicineName']"])
                    if not name:
                        continue

                    parsed_prod = await parse_medicine(name)
                    exact_match = pharmaeasy_exact_match(
                        medicine.canonicalName,
                        name,
                        medicine.variant,
                    )

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

                    effective_score = pharmaeasy_effective_score(score, exact_match)
                    if effective_score < NEAR_MATCH_SCORE:
                        continue

                    link_el = await card.query_selector("a")
                    href = await link_el.get_attribute("href") if link_el else ""
                    if not href:
                        continue

                    product_url = BASE_URL + href if href.startswith("/") else href
                    endpoint = urlparse(product_url).path

                    price_text = await first_text(
                        card,
                        ["[class*='ourPrice']", "[class*='price']", "[class*='sellingPrice']"],
                    )
                    price = extract_price_from_text(price_text) if price_text else None
                    if not price:
                        price = extract_price_from_text(card_text)

                    original_price_text = await first_text(
                        card,
                        ["[class*='striked']", "[class*='strike']", "[class*='mrp']"],
                    )
                    original_price = (
                        extract_price_from_text(original_price_text) if original_price_text else None
                    )
                    if not original_price:
                        matches = re.findall(
                            r"(?:\u20B9|RS\.?)\s*([\d,]+(?:\.\d{1,2})?)",
                            card_text,
                            flags=re.IGNORECASE,
                        )
                        if len(matches) > 1:
                            original_price = matches[1].replace(",", "")

                    discount_raw = await first_text(
                        card,
                        ["[class*='Discount']", "[class*='discount']", "[class*='offer']"],
                    )
                    discount = extract_discount_only(discount_raw)
                    if not discount:
                        discount = extract_discount_only(card_text)

                    pack = await first_text(
                        card,
                        ["[class*='measurementUnit']", "[class*='unit']", "[class*='pack']"],
                    )

                    candidate = {
                        "source": "PHARMEASY",
                        "name": name,
                        "pack": pack,
                        "price": price,
                        "originalPrice": original_price,
                        "discount": discount,
                        "productUrl": product_url,
                        "endpoint": endpoint,
                        "_exact_match": exact_match,
                        "_score": effective_score,
                    }

                    if (
                        best_candidate is None
                        or pharmaeasy_candidate_priority(candidate) > pharmaeasy_candidate_priority(best_candidate)
                    ):
                        best_candidate = candidate

                    if exact_match and effective_score >= NEAR_MATCH_SCORE:
                        break

                    if effective_score >= AUTO_ACCEPT_SCORE:
                        break
                except Exception:
                    continue

            if best_candidate and int(best_candidate.get("_score", 0)) >= NEAR_MATCH_SCORE:
                break

    except Exception as e:
        print(f"PHARMEASY ERROR: {e}")

    if best_candidate:
        results.append(best_candidate)

    return results
