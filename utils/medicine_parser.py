import asyncio
import csv
import re
from pathlib import Path

from prisma import Prisma

db = Prisma()

TYPO_FIX: dict[str, str] = {
    "TAB": "TABLET",
    "TABS": "TABLET",
    "TABLETS": "TABLET",
    "CAP": "CAPSULE",
    "CAPS": "CAPSULE",
    "CAPSULES": "CAPSULE",
    "OINMENT": "OINTMENT",
    "OINT": "OINTMENT",
    "SYP": "SYRUP",
    "INJ": "INJECTION",
}

PACK_PATTERNS = [
    re.compile(r"\d+\s*'S"),
    re.compile(r"\d+\s*TABS?\b"),
    re.compile(r"\d+\s*CAPS?\b"),
    re.compile(r"\d+\s*TABLETS\b"),
    re.compile(r"\d+\s*CAPSULES\b"),
    re.compile(r"STRIP OF \d+"),
    re.compile(r"STRIP OF\b"),
    re.compile(r"BOTTLE OF \d+"),
    re.compile(r"BOTTLE OF\b"),
    re.compile(r"PACK OF \d+"),
    re.compile(r"PACK OF\b"),
    re.compile(r"\d+\s*ML BOTTLE"),
    re.compile(r"\d+\s*ML\b"),
    re.compile(r"\d+ML\b"),
    re.compile(r"\d+X\d+(?:\.\d+)?ML?"),
    re.compile(r"\d+ X \d+ ML?"),
]

UNIT_REGEX = r"(MG|MCG|G|GM|ML|IU|%|MIU|MU)"
STRENGTH_REGEX = re.compile(rf"\b\d+(?:\.\d+)?\s?{UNIT_REGEX}\b", re.IGNORECASE)
COMBO_STRENGTH_REGEX = re.compile(
    rf"\b\d+(?:\.\d+)?\s?{UNIT_REGEX}(?:\s?[+/]\s?\d+(?:\.\d+)?\s?{UNIT_REGEX})+",
    re.IGNORECASE,
)
SLASH_COMBO_REGEX = re.compile(
    rf"\b\d+(?:\.\d+)?(?:\s*/\s*\d+(?:\.\d+)?)*\s?{UNIT_REGEX}\b",
    re.IGNORECASE,
)
DEDUP_STRENGTH_REGEX = re.compile(r"^\d+(?:\.\d+)?(MG|MCG|G|ML|IU|%|MIU|MU)$")

forms_cache: list[str] = []
variants_cache: set[str] = set()
ignored_variant_tokens = {"OF"}
special_variant_tokens = {"PLUS"}
load_lock = asyncio.Lock()
load_task: asyncio.Task | None = None


def normalize(text: str) -> str:
    normalized = text.upper()
    normalized = re.sub(r"\bI\s*\.\s*U\s*\.\b", "IU", normalized)
    normalized = re.sub(r"\bM\s*\.\s*I\s*\.\s*U\s*\.\b", "MIU", normalized)
    normalized = re.sub(r"\bM\s*\.\s*U\s*\.\b", "MU", normalized)
    normalized = normalized.replace("-", " ")
    normalized = normalized.replace("+", " PLUS ")
    normalized = normalized.replace("/", " / ")
    normalized = re.sub(r"[()]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)

    for from_text, to_text in TYPO_FIX.items():
        normalized = re.sub(rf"\b{re.escape(from_text)}\b", to_text, normalized)

    return normalized.strip()


def remove_pack(text: str) -> str:
    cleaned = text
    for pattern in PACK_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def tokenize(text: str) -> list[str]:
    return [token for token in text.split() if token]


def clean_duplicate_strengths(tokens: list[str]) -> list[str]:
    seen: set[str] = set()
    cleaned: list[str] = []

    for token in tokens:
        normalized = token.replace(" ", "").upper()
        if DEDUP_STRENGTH_REGEX.fullmatch(normalized):
            if normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)
            continue

        cleaned.append(token)

    return cleaned


def extract_strength(text: str) -> str | None:
    combo = COMBO_STRENGTH_REGEX.search(text)
    if combo:
        return re.sub(r"\s", "", combo.group(0))

    slash = SLASH_COMBO_REGEX.search(text)
    if slash:
        return re.sub(r"\s", "", slash.group(0))

    match = STRENGTH_REGEX.search(text)
    if match:
        return re.sub(r"\s", "", match.group(0))

    return None


def extract_numeric_strength(tokens: list[str]) -> str | None:
    for index, token in enumerate(tokens):
        if not re.fullmatch(r"\d{1,4}", token) or index == 0:
            continue

        previous = tokens[index - 1] if index > 0 else ""
        if re.fullmatch(r"[A-Z]\d+", previous):
            continue
        if previous in variants_cache:
            continue
        if int(token) > 2000:
            continue

        return token

    return None


async def _fetch_parser_data() -> None:
    global forms_cache
    global variants_cache

    if not db.is_connected():
        await db.connect()

    forms = await db.form.find_many()
    variants = await db.variant.find_many()

    forms_cache = sorted(
        {
            item.form.strip().upper()
            for item in forms
            if item.form and item.form.strip()
        },
        key=len,
        reverse=True,
    )

    variants_cache = {
        item.variant.strip().upper()
        for item in variants
        if item.variant and item.variant.strip()
    }


async def load_parser_data() -> None:
    global load_task

    if load_task is None:
        async with load_lock:
            if load_task is None:
                load_task = asyncio.create_task(_fetch_parser_data())

    try:
        await load_task
    except Exception:
        load_task = None
        raise


def extract_form(text: str) -> str | None:
    padded = f" {text.upper()} "
    for form in forms_cache:
        if f" {form} " in padded:
            return form
    return None


def extract_variant(tokens: list[str]) -> str | None:
    found = [
        token
        for index, token in enumerate(tokens)
        if index != 0
        and (token in variants_cache or token in special_variant_tokens)
        and token not in ignored_variant_tokens
    ]
    return " ".join(found) if found else None


def extract_brand(
    tokens: list[str],
    strength: str | None,
    form: str | None,
    variant: str | None,
) -> str:
    stop_tokens: set[str] = set()

    if strength:
        stop_tokens.add(strength)

    if form:
        stop_tokens.update(form.split(" "))

    if variant:
        stop_tokens.update(variant.split(" "))

    brand: list[str] = []
    for token in tokens:
        if token in stop_tokens or re.fullmatch(r"\d+", token):
            break
        brand.append(token)

    return " ".join(brand) if brand else "UNKNOWN"


def build_canonical(brand: str, variant: str, strength: str, form: str) -> str:
    normalized_brand = (brand or "UNKNOWN").upper()
    normalized_variant = (variant or "NORMAL").upper()
    normalized_strength = (strength or "UNKNOWN").upper()
    normalized_form = (form or "NORMAL").upper()

    parts: list[str] = []
    if normalized_brand and normalized_brand != "UNKNOWN":
        parts.append(normalized_brand)
    if normalized_variant not in {"NORMAL", "UNKNOWN"} and normalized_variant not in normalized_brand:
        parts.append(normalized_variant)
    if normalized_strength not in {"NORMAL", "UNKNOWN"} and normalized_strength not in normalized_brand:
        parts.append(normalized_strength)
    if normalized_form not in {"NORMAL", "UNKNOWN"}:
        parts.append(normalized_form)

    return " ".join(dict.fromkeys(parts))


async def parse_medicine(raw_name: str, debug: bool = False) -> dict[str, str]:
    await load_parser_data()

    normalized_name = normalize(raw_name)
    form = extract_form(normalized_name)

    name = remove_pack(normalized_name)

    tokens = tokenize(name)
    tokens = clean_duplicate_strengths(tokens)

    strength = extract_strength(name) or extract_numeric_strength(tokens)
    if strength:
        strength = re.sub(r"GM", "G", strength, flags=re.IGNORECASE)

    variant = extract_variant(tokens)
    brand = extract_brand(tokens, strength, form, variant)

    if not variant:
        variant = "NORMAL"
    if not form:
        form = "NORMAL"
    if not strength:
        strength = "UNKNOWN"

    result = {
        "brand": brand,
        "variant": variant,
        "strength": strength,
        "form": form,
        "canonicalName": build_canonical(brand, variant, strength, form),
    }

    if debug:
        print("RAW:", raw_name)
        print("NORMALIZED:", name)
        print("TOKENS:", tokens)
        print("PARSED:", result)

    return result


async def parse_csv_debug(csv_path: str) -> list[dict[str, str]]:
    path = Path(csv_path)
    if not path.exists():
        print(f"CSV file not found: {path}")
        return []

    parsed_list: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            raw_name = (
                row.get("Medicine")
                or row.get("medicine")
                or row.get("Name")
                or row.get("name")
            )
            if not raw_name:
                continue
            parsed = await parse_medicine(raw_name, debug=True)
            parsed_list.append(parsed)
    return parsed_list


if __name__ == "__main__":
    all_parsed = asyncio.run(parse_csv_debug("medicines.csv"))
    print(f"Total medicines parsed: {len(all_parsed)}")
