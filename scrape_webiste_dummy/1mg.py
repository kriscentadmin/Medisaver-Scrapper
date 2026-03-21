# ////////////////////////////////////////////////////////////////////////////////////////////
import asyncio
import re
import random
import sqlite3
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from rapidfuzz import fuzz

BASE_URL = "https://www.1mg.com"
SEARCH_URL = "https://www.1mg.com/search/all?name={}"
TIMEOUT = 30000
HEADLESS = True

MEDICINES = [
    "GLYCIPHAGE SR 1000 MG TABLET 10",
    "AZORAN 50 MG TABLET 10",
    "FORACORT 200 MCG ROTACAP 30",
    "GLUCONORM G2 MG TABLET 15",
    "GALVUS MET 50/500 MG TABLET 15",
    "NEUROBION FORTE TABLET",
    "PANTOSEC TABLET 10",
    "GEMER 2 TABLET 10"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119 Safari/537.36",
]

# -----------------------------
# DATABASE
# -----------------------------
conn = sqlite3.connect("medicines.db")
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
    if not data:
        return
    cursor.execute("""
        INSERT INTO medicines (
            search_name, name, pack, price,
            original_price, discount,
            product_url, endpoint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("search_name"),
        data.get("name"),
        data.get("pack"),
        data.get("discount_price"),
        data.get("original_price"),
        data.get("discount_percent"),
        data.get("product_url"),
        data.get("endpoint")
    ))
    conn.commit()


# -----------------------------
# STRENGTH FUNCTIONS
# -----------------------------
def normalize_strength(value, unit):
    value = float(value)
    unit = unit.lower()
    if unit == "g":
        return value * 1000
    elif unit == "mcg":
        return value / 1000
    return value


def extract_strength(text):
    text = text.lower()
    strengths = []

    # normal strengths
    for val, unit in re.findall(r'(\d+(?:\.\d+)?)\s*(mg|g|mcg)', text):
        strengths.append(normalize_strength(val, unit))

    # combination like 6/200
    for combo in re.findall(r'(\d+)\s*/\s*(\d+)\s*(mg|mcg|g)', text):
        v1, v2, unit = combo
        strengths.append(normalize_strength(v1, unit))
        strengths.append(normalize_strength(v2, unit))

    return list(set(strengths))


def is_strength_match(search_strengths, product_strengths):
    """
    Pharma logic:
    - If product has no strength → allow
    - If search has strength → at least ONE must match
    """
    if not search_strengths:
        return True

    if not product_strengths:
        return True

    for s in search_strengths:
        for p in product_strengths:
            if abs(p - s) / max(s, 1) <= 0.2:  # ±20%
                return True

    return False


# -----------------------------
# BRAND MATCH
# -----------------------------
def get_brand(name):
    return name.lower().split()[0]


def is_brand_match(search_name, product_name):
    brand = get_brand(search_name)
    return product_name.lower().startswith(brand)


# -----------------------------
# PARSER
# -----------------------------
def parse_product_text(text: str):
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    data = {
        "name": None,
        "pack": None,
        "discount_price": None,
        "original_price": None,
        "discount_percent": None
    }

    for line in lines:
        l = line.lower()

        if not data["name"] and any(x in l for x in ["tablet", "capsule", "rotacap", "syrup"]):
            data["name"] = line

        if "strip of" in l or "bottle of" in l:
            data["pack"] = line

        if "₹" in line:
            prices = re.findall(r"₹\d+\.?\d*", line)
            if prices:
                if not data["discount_price"]:
                    data["discount_price"] = prices[0]
                else:
                    data["original_price"] = prices[0]

        if "%" in line:
            data["discount_percent"] = line

    return data


# -----------------------------
# SCRAPER
# -----------------------------
async def scrape_medicine(page, medicine):
    print(f"\nSearching: {medicine}")

    search_strengths = extract_strength(medicine)
    best_match = None
    best_score = -1

    try:
        url = SEARCH_URL.format(medicine.replace(" ", "%20"))
        await page.goto(url, timeout=TIMEOUT)
        await page.wait_for_load_state("networkidle")

        products = await page.query_selector_all("a[href*='/drugs/'], a[href*='/otc/']")
        if not products:
            print("No products found on page")
            return None

        for product in products[:30]:
            text = await product.inner_text()
            parsed = parse_product_text(text)
            if not parsed["name"]:
                continue

            # 1. Brand match (strict)
            if not is_brand_match(medicine, parsed["name"]):
                continue

            # 2. Strength match (pharma logic)
            product_strengths = extract_strength(text)
            if not is_strength_match(search_strengths, product_strengths):
                continue

            # 3. Fuzzy ranking
            score = fuzz.token_set_ratio(medicine.lower(), parsed["name"].lower())
            if score < 50:
                continue

            href = await product.get_attribute("href")
            full_url = BASE_URL + href if href and not href.startswith("http") else href

            if score > best_score:
                best_score = score
                best_match = parsed
                best_match.update({
                    "search_name": medicine,
                    "product_url": full_url,
                    "endpoint": href
                })

    except PlaywrightTimeout:
        print("Timeout while searching")
    except Exception as e:
        print("Error:", e)

    if best_match:
        print("Found:", best_match)
        save_to_db(best_match)
        return best_match

    print(f"No match found: {medicine}")
    return None


# -----------------------------
# MAIN
# -----------------------------
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768}
        )
        page = await context.new_page()

        for med in MEDICINES:
            await scrape_medicine(page, med)
            await asyncio.sleep(random.uniform(2, 4))

        await browser.close()
        conn.close()
        print("\nData saved to medicines.db")


if __name__ == "__main__":
    asyncio.run(main())
