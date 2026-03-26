import re


async def first_text(element, selectors: list[str]) -> str | None:
    for selector in selectors:
        try:
            target = await element.query_selector(selector)
            if not target:
                continue
            text = (await target.inner_text()).strip()
            if text:
                return text
        except Exception:
            continue
    return None


def candidate_priority(candidate: dict) -> tuple[int, int, int]:
    populated_fields = sum(
        1 for field in ("price", "originalPrice", "discount", "pack", "productUrl", "endpoint")
        if candidate.get(field)
    )
    has_price = 1 if candidate.get("price") else 0
    return (int(candidate.get("_score", 0)), has_price, populated_fields)


def extract_price_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = re.search(r"(?:\u20B9|RS\.?|INR)\s*([\d,]+(?:\.\d{1,2})?)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).replace(",", "")
    return None


def extract_percent_discount(text: str | None, lowercase_off: bool = False) -> str | None:
    if not text:
        return None
    match = re.search(r"(\d+)\s*%?\s*off", text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)
