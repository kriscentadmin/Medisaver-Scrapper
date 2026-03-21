import asyncio
import sqlite3
import logging
import random
import re
from datetime import datetime
import sys
import io
from urllib.parse import urlparse  # ADDED for endpoint path extraction

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ================= CONFIG =================
BASE_URL = "https://www.truemeds.in"
DB_NAME = "truemed.db"
HEADLESS = True

MEDICINES = [
    "GLYCIPHAGE SR 1000 MG TABLET 10",
    "PANTOSEC 40 MG TABLET 10",
    "GEMER 2 MG TABLET 10",
    "GLUCONORM G 2 MG TABLET 15",
    "FORACORT 6/200 MCG ROTACAP 30",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")

# ================= DATABASE =================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS medicines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        searched_for TEXT,
        name TEXT,
        pack TEXT,
        price TEXT,
        original_price TEXT,
        discount TEXT,
        endpoint TEXT,
        product_url TEXT,
        scrape_time TEXT,
        UNIQUE(searched_for, endpoint)
    )
    """)
    conn.commit()
    conn.close()

def save_to_db(data):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO medicines 
        (searched_for, name, pack, price, original_price, discount, endpoint, product_url, scrape_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["searched_for"],
        data["name"],
        data["pack"],
        data["price"],
        data["original_price"],
        data["discount"],
        data["endpoint"],
        data["product_url"],
        data["scrape_time"]
    ))
    conn.commit()
    conn.close()

    orig_part = f" (orig {data['original_price']})" if data['original_price'] else ""
    disc_part = f" {data['discount']}" if data['discount'] else ""
    pack_part = f" | pack: {data['pack']}" if data['pack'] and data['pack'] != "N/A" else ""
    endpoint_part = f" | endpoint: {data['endpoint']}" if data['endpoint'] else ""
    url_part = f" | product_url: {data['product_url']}" if data['product_url'] else ""

    logging.info(f"SAVED → {data['name']} | {data['price']}{orig_part}{disc_part}{pack_part}{endpoint_part}{url_part}")

# ================= HELPERS =================
async def human_delay(min_sec=2.0, max_sec=6.0):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

def normalize_text(text: str) -> str:
    return re.sub(r'\s+', ' ', (text or "").strip().lower())

def extract_numbers(text: str):
    return re.findall(r'\d+', text)

def generate_search_terms(medicine: str):
    brand = medicine.split()[0].lower()
    q_lower = normalize_text(medicine)
    numbers = extract_numbers(medicine)
    form = 'rotacap' if 'rotacap' in q_lower else 'tablet' if 'tablet' in q_lower else ''

    terms = [medicine.lower()]
    if numbers:
        for num in numbers:
            if int(num) > 50:
                terms.append(f"{brand} {num} {form}".strip())
                if int(num) >= 1000:
                    gm = int(num) // 1000
                    terms.append(f"{brand} {gm}gm {form}".strip())
                    terms.append(f"{brand} {gm} gm {form}".strip())
        if len(numbers) >= 2:
            terms.append(f"{brand} {'/'.join(numbers[:2])} {form}".strip())
    terms.append(f"{brand} {form}".strip())
    terms.append(f"{brand} sr {form}".strip())
    terms.append(brand)

    return list(dict.fromkeys(t for t in terms if t))

def is_good_match(product_name: str, search_query: str) -> tuple[bool, str]:
    p = normalize_text(product_name)
    q = normalize_text(search_query)

    brand = search_query.split()[0].lower()
    if brand not in p:
        return False, "brand missing"

    forbidden = ["forte", "p ", "pg ", "ds ", "vg", "plus ", "triple ", "0.5", "0 5", "convistat", "olsem", "ceso"]
    for bad in forbidden:
        if bad in p and bad not in q:
            return False, f"rejected variant ({bad.strip()})"

    query_nums = [n for n in extract_numbers(search_query) if int(n) > 50]
    if query_nums:
        main_wanted = max(query_nums, key=int)
        found = str(main_wanted) in p

        if not found and int(main_wanted) >= 500:
            gm_val = int(main_wanted) // 1000 if int(main_wanted) % 1000 == 0 else main_wanted / 1000
            gm_str = f"{int(gm_val) if gm_val.is_integer() else gm_val}gm"
            gm_space = f"{int(gm_val) if gm_val.is_integer() else gm_val} gm"
            mg_alt = f"{main_wanted}mg"
            if gm_str in p or gm_space in p or mg_alt in p:
                found = True

        # Relax for Foracort (6/200 → accept 200)
        if "foracort" in q and "200" in p and "400" not in p:
            found = True

        if not found:
            return False, f"missing strength {main_wanted} (or gm equiv)"

    return True, "accepted"

# ================= SCRAPER =================
async def scrape_medicine(page, medicine: str):
    logging.info(f"→ {medicine}")

    search_terms = generate_search_terms(medicine)
    found = False

    for term in search_terms:
        if found:
            break

        logging.info(f" ├─ Trying: {term}")

        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
            await human_delay(2, 5)

            search_input = await page.wait_for_selector('#searchInput', timeout=20000)
            await search_input.click()
            await search_input.fill(term)
            await human_delay(3, 8)

            try:
                await page.wait_for_selector("div.sc-a35f2f57-2", timeout=20000, state="visible")
            except PlaywrightTimeoutError:
                logging.warning("No popup for term")
                continue

            items = await page.query_selector_all("div.sc-17296275-0")
            logging.info(f" │ Found {len(items)} suggestions")

            for item in items:
                try:
                    name_el = await item.query_selector("p.sc-17296275-3.eFNfxd")
                    name = (await name_el.inner_text()).strip() if name_el else ""
                    if not name:
                        continue

                    ok, reason = is_good_match(name, medicine)
                    if not ok:
                        logging.info(f" │ Rejected {name:<40} → {reason}")
                        continue

                    # FIXED: Extract full href, endpoint, and product_url
                    link_el = await item.query_selector('a')  # Broader selector to catch the link
                    href = ""
                    if link_el:
                        href = await link_el.get_attribute("href") or ""
                        href = href.strip()

                    # Clean and normalize href
                    if href:
                        if href.startswith("http"):
                            product_url = href
                        else:
                            product_url = f"{BASE_URL}{href}"
                        endpoint = urlparse(product_url).path if product_url else ""
                    else:
                        product_url = ""
                        endpoint = ""

                    orig_el = await item.query_selector("span.sc-17296275-6.gBsAGy")
                    original_price = (await orig_el.inner_text()).strip() if orig_el else "N/A"

                    disc_el = await item.query_selector("span.sc-17296275-7.dRtIYQ")
                    discount = (await disc_el.inner_text()).strip() if disc_el else "N/A"

                    price_el = await item.query_selector("p.sc-17296275-8.cgGPNE")
                    price_raw = (await price_el.inner_text()).strip() if price_el else "N/A"
                    price = re.sub(r'<span.*?</span>', '', price_raw).strip()

                    # Extract pack from name (last number)
                    pack = "N/A"
                    numbers_in_name = extract_numbers(name)
                    if numbers_in_name:
                        pack = numbers_in_name[-1]

                    result = {
                        "searched_for": medicine,
                        "name": name,
                        "pack": pack,
                        "price": price,
                        "original_price": original_price,
                        "discount": discount,
                        "endpoint": endpoint,
                        "product_url": product_url,
                        "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    save_to_db(result)
                    logging.info(f" └─ MATCH → {name} | pack: {pack} | {price} orig: {original_price} disc: {discount} | endpoint: {endpoint} | url: {product_url}")
                    found = True
                    break

                except Exception as e:
                    logging.debug(f"Item error: {str(e)}")
                    continue

        except Exception as e:
            logging.warning(f"Term '{term}' error: {e}")

    if not found:
        logging.warning(f" !! NO GOOD MATCH for {medicine}")

# ================= MAIN =================
async def main():
    init_db()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, slow_mo=random.randint(100, 300))
        for med in MEDICINES:
            context = await browser.new_context(
                viewport={"width": random.choice([1366, 1440]), "height": random.choice([768, 900])},
                user_agent=random.choice(USER_AGENTS),
                locale="en-IN",
                timezone_id="Asia/Kolkata",
                java_script_enabled=True,
                ignore_https_errors=True,
            )
            page = await context.new_page()
            try:
                await scrape_medicine(page, med)
            except Exception as e:
                logging.error(f"Critical error for {med}: {e}")
            finally:
                await page.close()
                await context.close()
                await asyncio.sleep(random.uniform(10, 25))
        await browser.close()
    logging.info("Scraping completed.")

if __name__ == "__main__":
    asyncio.run(main())