import asyncio
import random
import re
import sqlite3
from playwright.async_api import async_playwright

BASE_URL = "https://pharmeasy.in"
SEARCH_URL = "https://pharmeasy.in/search/all?name={}"
HEADLESS = True
TIMEOUT = 60000

MEDICINES = [
    "GLYCIPHAGE SR 1000 MG TABLET 10",
    "PANTOSEC 40 MG TABLET 10",
    "GEMER 2mg TABLET 10",
    "GLUCONORM G 2 MG TABLET 15",
    "FORACORT 6/200 MCG ROTACAP 30",
    "CONCOR COR 2.5 MG TABLET 10",
    "FLAVEDON MR 35 MG TABLET 10",
]

# ---------------- DATABASE ----------------
conn = sqlite3.connect("pharmeasy.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS medicines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    search_name TEXT,
    name TEXT,
    pack TEXT,
    price TEXT,
    original_price TEXT,
    discount TEXT,
    product_url TEXT,
    endpoint TEXT
)
""")
conn.commit()


def save_to_db(data):
    cursor.execute("""
        INSERT INTO medicines (
            search_name, name, pack, price,
            original_price, discount, product_url, endpoint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["search_name"],
        data["name"],
        data["pack"],
        data["discount_price"],
        data["original_price"],
        data["discount_percent"],
        data["product_url"],
        data["endpoint"]
    ))
    conn.commit()


# ---------------- HELPERS ----------------

def normalize_text(text):
    return re.sub(r'\s+', ' ', text.lower()).strip()


def extract_discount_percent(text):
    """
    Extract only numeric percent from:
    ₹43.07
    25% OFF
    """
    if not text:
        return None

    m = re.search(r'(\d+)\s*%', text)
    return m.group(1) if m else None


# ---------------- MATCH CONFIG ----------------

VARIANT_WORDS = [
    "p", "m", "xr", "sr", "er", "mr", "od",
    "forte", "plus", "neo", "trio", "g",
    "ir", "sita", "met", "dapa"
]


def extract_strengths(text):
    text = text.lower()
    strengths = []

    for a, b in re.findall(r'(\d+\.?\d*)\s*/\s*(\d+\.?\d*)', text):
        strengths.append(float(a))
        strengths.append(float(b))

    for v, unit in re.findall(r'(\d+\.?\d*)\s*(mg|mcg|g)', text):
        v = float(v)
        if unit == "g":
            v *= 1000
        if unit == "mcg":
            v /= 1000
        strengths.append(v)

    return sorted(set(strengths))


def extract_pack(text):
    m = re.search(r'(\d+)\s*(tablet|capsule|rotacap)', text.lower())
    return int(m.group(1)) if m else None


def extract_brand(search):
    search = search.lower()
    parts = search.split()

    brand_parts = []
    for p in parts:
        if re.search(r'\d', p):
            break
        brand_parts.append(p)

    return " ".join(brand_parts)


def brand_and_variant_match(search, product):
    search = normalize_text(search)
    product = normalize_text(product)

    brand = extract_brand(search)

    if not product.startswith(brand):
        return False

    remaining = product[len(brand):].strip()
    words = remaining.split()

    for word in words:
        if re.search(r'\d', word):
            break
        if word in VARIANT_WORDS and word not in search:
            return False

    return True


def strength_match(search, product):
    s_strength = extract_strengths(search)
    p_strength = extract_strengths(product)

    if s_strength:
        if len(s_strength) != len(p_strength):
            return False

        for s in s_strength:
            if not any(abs(s - p) < 0.1 for p in p_strength):
                return False

    return True


def exact_match(search, product):
    search_l = normalize_text(search)
    product_l = normalize_text(product)

    if not brand_and_variant_match(search_l, product_l):
        return False

    if not strength_match(search_l, product_l):
        return False

    s_pack = extract_pack(search_l)
    p_pack = extract_pack(product_l)

    if s_pack and p_pack:
        if abs(s_pack - p_pack) > 10:
            return False

    return True


# ---------------- SCRAPER ----------------

async def scrape_medicine(page, medicine):
    print(f"\nSearching: {medicine}")

    url = SEARCH_URL.format(medicine.replace(" ", "%20"))
    await page.goto(url, timeout=TIMEOUT)

    await page.wait_for_selector(
        "div[class*='ProductCard_medicineUnitContainer']",
        timeout=TIMEOUT
    )

    await page.wait_for_timeout(2000)

    for _ in range(4):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(1000)

    cards = await page.query_selector_all(
        "div[class*='ProductCard_medicineUnitContainer']"
    )

    print(f"Total cards found: {len(cards)}")

    for index, card in enumerate(cards):
        try:
            name_el = await card.query_selector("h1")
            if not name_el:
                continue

            name = (await name_el.inner_text()).strip()

            if not exact_match(medicine, name):
                continue

            print(f"Matched at position: {index + 1} -> {name}")

            pack_el = await card.query_selector("[class*='measurementUnit']")
            price_el = await card.query_selector("[class*='ourPrice']")
            mrp_el = await card.query_selector("[class*='striked']")
            discount_el = await card.query_selector("[class*='Discount']")
            link_el = await card.query_selector("a")

            href = await link_el.get_attribute("href") if link_el else None

            discount_text = (await discount_el.inner_text()).strip() if discount_el else None
            discount_percent = extract_discount_percent(discount_text)

            result = {
                "search_name": medicine,
                "name": name,
                "pack": (await pack_el.inner_text()).strip() if pack_el else None,
                "discount_price": (await price_el.inner_text()).strip() if price_el else None,
                "original_price": (await mrp_el.inner_text()).strip() if mrp_el else None,
                "discount_percent": discount_percent,
                "product_url": BASE_URL + href if href else None,
                "endpoint": href
            }

            print("Found:", result)
            save_to_db(result)
            return result

        except:
            continue

    print("No exact match:", medicine)

    empty = {
        "search_name": medicine,
        "name": None,
        "pack": None,
        "discount_price": None,
        "original_price": None,
        "discount_percent": None,
        "product_url": None,
        "endpoint": None
    }

    save_to_db(empty)
    return empty


# ---------------- MAIN ----------------

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)

        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="en-IN",
            timezone_id="Asia/Kolkata"
        )

        page = await context.new_page()

        for med in MEDICINES:
            await scrape_medicine(page, med)
            await asyncio.sleep(random.uniform(4, 8))

        await browser.close()
        conn.close()
        print("\nData saved to pharmeasy.db")


if __name__ == "__main__":
    asyncio.run(main())
