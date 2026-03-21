# /////////////////////////////////////////////////////////////
import asyncio
import sqlite3
import logging
import random
import re
from urllib.parse import quote, urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ================= CONFIG =================
BASE_URL = "https://www.netmeds.com"
SEARCH_URL = "https://www.netmeds.com/products?q={}"
DB_NAME = "netmeds.db"
HEADLESS = True  # Browser runs in background - no window opens

MEDICINES = [
    "GLYCIPHAGE SR 1000 MG TABLET 10",
    "PANTOSEC 40 MG TABLET 10",
    "GEMER 2 MG TABLET 10",
    "GLUCONORM G 2 MG TABLET 15",
    "FORACORT 6/200 MCG ROTACAP 30"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")


# ================= DATABASE =================
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS medicines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        search_name TEXT,
        name TEXT,
        pack TEXT,
        price TEXT,
        original_price TEXT,
        discount TEXT,
        product_url TEXT,
        endpoint TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    conn.close()


def save_to_db(data):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO medicines (
            search_name, name, pack, price,
            original_price, discount,
            product_url, endpoint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["search_name"],
        data["name"],
        data["pack"],
        data["price"],
        data["original_price"],
        data["discount"],
        data["product_url"],
        data["endpoint"]
    ))
    conn.commit()
    conn.close()
    orig_part = f" orig: {data['original_price']}" if data['original_price'] else ""
    disc_part = f" disc: {data['discount']}" if data['discount'] else ""
    logging.info(f"SAVED → {data['name']} | pack: {data['pack']} | ₹{data['price']}{orig_part}{disc_part}")


# ================= HELPERS =================
async def human_delay(min_sec=4.0, max_sec=12.0):
    await asyncio.sleep(random.uniform(min_sec, max_sec))


def get_endpoint(url):
    return urlparse(url).path if url else None


def normalize_text(text: str) -> str:
    return re.sub(r'\s+', ' ', (text or "").strip().lower())


def extract_numbers(query: str):
    return re.findall(r'\d+', query)


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

    forbidden = ["forte", "p ", "pg ", "pl ", "ds ", "v ", "plus ", "triple "]
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

        if not found:
            return False, f"missing wanted strength {main_wanted} (or gm equivalent)"

    if "foracort" in q:
        if "forte" in p:
            return False, "rejected forte variant"
        if "400" in p and "200" in q:
            return False, "wrong strength - 400 instead of 200"

    core = re.sub(r'\d+|\s+mg|\s+mcg|\s+gm|\s+tablet|\s+rotacap|\s+\d+$', '', q).strip()
    if core and core not in p:
        if brand in p:
            pass
        else:
            return False, "name too different"

    return True, "accepted"


# ================= SCRAPER =================
async def scrape_medicine(page, medicine: str):
    logging.info(f"→ {medicine}")

    search_terms = generate_search_terms(medicine)
    found = False

    for term in search_terms:
        if found:
            break
        url = SEARCH_URL.format(quote(term))
        logging.info(f" ├─ Trying: {term}")
        try:
            await page.goto(url, wait_until="networkidle", timeout=45000)
            await human_delay(4.0, 10.0)

            try:
                await page.wait_for_selector(".product-card-container", timeout=20000)
            except PlaywrightTimeoutError:
                continue

            if await page.is_visible(".no-results"):
                continue

            # Light scroll
            await page.evaluate("window.scrollBy(0, 200)")
            await human_delay(1.0, 3.0)

            cards = await page.query_selector_all(".product-card-container")
            logging.info(f" │ Found {len(cards)} cards")

            for card in cards[:12]:
                try:
                    name_el = await card.query_selector("h3")
                    name = (await name_el.inner_text() if name_el else "").strip()
                    if not name:
                        continue

                    ok, reason = is_good_match(name, medicine)
                    if not ok:
                        if any(k in reason for k in ["rejected", "missing", "wrong"]):
                            logging.info(f" │ Rejected {name:<38} → {reason}")
                        continue

                    # Get product URL from search card
                    link_el = await card.query_selector('a[href^="/product"]')
                    href = await link_el.get_attribute("href") if link_el else ""
                    product_url = BASE_URL + href if href else ""
                    if not product_url:
                        continue

                    # Go to product detail page
                    await page.goto(product_url, wait_until="networkidle", timeout=30000)
                    await human_delay(3.0, 6.0)

                    # Pack from product page
                    pack = "N/A"
                    pack_el = await page.query_selector(".jm-body-xxxs-bold, .jm-body-xxxs.jm-fc-primary-gray-80")
                    if pack_el:
                        pack_text = (await pack_el.inner_text()).strip()
                        # Clean pack (avoid MRP text)
                        if "MRP" not in pack_text:
                            pack = pack_text

                    # Price from product page - only current price (fixed)
                    price = None
                    price_el = await page.query_selector(".effective-price-div")
                    if price_el:
                        price_text = (await price_el.inner_text()).strip()
                        # Take only the current price part (before % OFF)
                        current_price_match = re.search(r'₹[\d,.]+', price_text)
                        if current_price_match:
                            price = current_price_match.group(0)

                    # Original price from product page (MRP)
                    original_price = ""
                    mrp_el = await page.query_selector(".marked-price")
                    if mrp_el:
                        original_price = (await mrp_el.inner_text()).strip()
                        if original_price and not original_price.startswith('₹'):
                            original_price = "₹" + original_price

                    # Discount from product page
                    discount = ""
                    discount_el = await page.query_selector(".jm-fc-light-sparkle-80")
                    if discount_el:
                        discount = (await discount_el.inner_text()).strip()

                    endpoint = get_endpoint(product_url)

                    result = {
                        "search_name": medicine,
                        "name": name,
                        "pack": pack,
                        "price": price or "",
                        "original_price": original_price or "",
                        "discount": discount or "",
                        "product_url": product_url,
                        "endpoint": endpoint
                    }

                    save_to_db(result)
                    logging.info(f" └─ MATCH → {name} | pack: {pack} | {price} orig: {original_price} disc: {discount}")
                    found = True
                    break

                except Exception as e:
                    logging.debug(f"Card error {name}: {str(e)}")
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
                viewport={"width": random.choice([1366, 1440, 1536]), "height": random.choice([768, 900, 1080])},
                user_agent=random.choice(USER_AGENTS),
                locale="en-IN",
                timezone_id="Asia/Kolkata",
                java_script_enabled=True,
                ignore_https_errors=True,
                device_scale_factor=1,
                is_mobile=False,
                has_touch=False,
                color_scheme="light",
                reduced_motion="no-preference",
            )
            page = await context.new_page()
            await page.set_extra_http_headers({"Accept-Language": "en-IN,en;q=0.9"})
            try:
                await scrape_medicine(page, med)
            except Exception as e:
                logging.error(f"Critical error for {med}: {e}")
            finally:
                await page.close()
                await context.close()
                await human_delay(10, 25)  # very long pause between medicines
        await browser.close()
    logging.info("Scraping completed.")


if __name__ == "__main__":
    asyncio.run(main())