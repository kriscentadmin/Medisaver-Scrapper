# import asyncio
# import sqlite3
# import logging
# import random
# import re
# from datetime import datetime
# from urllib.parse import urljoin

# from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# # ================= CONFIG =================
# BASE_URL = "https://www.apollopharmacy.in/"
# DB_NAME = "apollo.db"
# HEADLESS = False

# MEDICINES = [
#     "GLYCIPHAGE SR 1000 MG TABLET 10",
#     "PANTOSEC 40 MG TABLET 10",
#     "GEMER 2 MG TABLET 10",
#     "GLUCONORM G 2 MG TABLET 15",
#     "FORACORT 6/200 MCG ROTACAP 30",
# ]

# USER_AGENTS = [
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
#     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
#     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
# ]

# logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")

# # ================= DATABASE =================
# def init_db():
#     conn = sqlite3.connect(DB_NAME)
#     cur = conn.cursor()
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS medicines (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         searched_for TEXT,
#         name TEXT,
#         pack TEXT,
#         price TEXT,
#         endpoint TEXT,
#         product_url TEXT,
#         scrape_time TEXT,
#         UNIQUE(searched_for, product_url)
#     )
#     """)
#     conn.commit()
#     conn.close()

# def save_to_db(data):
#     conn = sqlite3.connect(DB_NAME)
#     cur = conn.cursor()
#     cur.execute("""
#         INSERT OR IGNORE INTO medicines 
#         (searched_for, name, pack, price, endpoint, product_url, scrape_time)
#         VALUES (?, ?, ?, ?, ?, ?, ?)
#     """, (
#         data["searched_for"],
#         data["name"],
#         data["pack"],
#         data["price"],
#         data["endpoint"],
#         data["product_url"],
#         data["scrape_time"]
#     ))
#     conn.commit()
#     conn.close()

#     pack_part = f" | pack: {data['pack']}" if data['pack'] and data['pack'] != "N/A" else ""
#     endpoint_part = f" | endpoint: {data['endpoint']}" if data['endpoint'] else ""
#     url_part = f" | url: {data['product_url']}" if data['product_url'] else ""

#     logging.info(f"SAVED → {data['name']} | {data['price']}{pack_part}{endpoint_part}{url_part}")

# # ================= HELPERS =================
# async def human_delay(min_sec=2.5, max_sec=7.0):
#     await asyncio.sleep(random.uniform(min_sec, max_sec))

# def normalize_text(text: str) -> str:
#     return re.sub(r'\s+', ' ', (text or "").strip().lower())

# def extract_pack(name: str) -> str:
#     # Look for patterns like "10's", "10 Tablet", "15 Tablet", "30 Rotacaps"
#     match = re.search(r'(\d+)\s*(Tablet|Tab|Rotacap|Capsule|\'s)?', name, re.IGNORECASE)
#     return match.group(1) if match else "N/A"

# def is_good_match(product_name: str, search_query: str) -> tuple[bool, str]:
#     p = normalize_text(product_name)
#     q = normalize_text(search_query)

#     brand = search_query.split()[0].lower()
#     if brand not in p:
#         return False, "brand missing"

#     forbidden = ["forte", "pg", "vg", "p ", "d ", "ds ", "extra", "g1", "g3", "g4", "0.5", "0 5"]
#     for bad in forbidden:
#         if bad in p and bad not in q:
#             return False, f"rejected variant ({bad.strip()})"

#     query_nums = [n for n in re.findall(r'\d+', search_query) if int(n) > 50]
#     if query_nums:
#         main_wanted = max(query_nums, key=int)
#         found = str(main_wanted) in p or f"{main_wanted}mg" in p

#         if not found and int(main_wanted) >= 1000:
#             gm = str(int(main_wanted) // 1000)
#             if f"{gm}gm" in p or f"{gm} gm" in p:
#                 found = True

#         # For Foracort: accept 200 even if query has 6/200
#         if "foracort" in q and "200" in p and "400" not in p:
#             found = True

#         if not found:
#             return False, f"missing strength {main_wanted}"

#     return True, "accepted"

# # ================= SCRAPER =================
# async def close_popup_if_present(page):
#     try:
#         popup_close = await page.query_selector('button[aria-label="Close"], button.close, [id*="close"], [class*="close"]')
#         if popup_close:
#             await popup_close.click()
#             logging.info("Popup closed")
#             await human_delay(1, 3)
#     except:
#         pass  # no popup or failed to close - continue

# async def scrape_medicine(page, medicine: str):
#     logging.info(f"→ {medicine}")

#     try:
#         await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=45000)
#         await human_delay(3, 8)

#         # Close initial popup if present
#         await close_popup_if_present(page)

#         # Type in search bar
#         search_input = await page.wait_for_selector('#searchProduct', timeout=20000)
#         await search_input.click()
#         await search_input.fill(medicine)
#         await human_delay(4, 10)

#         # Wait for suggestion list
#         try:
#             await page.wait_for_selector("div.MedicineAutoSearch_autoSearchPopover__YtZHq", timeout=25000, state="visible")
#             logging.info("Suggestions appeared")
#         except PlaywrightTimeoutError:
#             logging.warning("No suggestions popup")
#             return

#         # Scroll list to load more if needed (human-like)
#         await page.evaluate("window.scrollBy(0, 300)")
#         await human_delay(1.5, 4)

#         items = await page.query_selector_all("li.ProductSuggestion_suggestionList__mijPN")
#         logging.info(f"Found {len(items)} suggestion items")

#         best_match = None
#         best_reason = ""

#         for item in items:
#             try:
#                 name_el = await item.query_selector("h2.DL")
#                 name = (await name_el.inner_text()).strip() if name_el else ""
#                 if not name:
#                     continue

#                 ok, reason = is_good_match(name, medicine)
#                 if not ok:
#                     logging.info(f"Rejected {name[:50]}... → {reason}")
#                     continue

#                 # Get link
#                 link_el = await item.query_selector('a.cardAnchorStyle')
#                 href = await link_el.get_attribute("href") if link_el else ""
#                 endpoint = href if href else ""
#                 product_url = urljoin(BASE_URL, href) if href else ""

#                 # Price (current selling price)
#                 price_el = await item.query_selector("p.cV_")
#                 price = (await price_el.inner_text()).strip() if price_el else "N/A"

#                 # Pack from name
#                 pack = extract_pack(name)

#                 result = {
#                     "searched_for": medicine,
#                     "name": name,
#                     "pack": pack,
#                     "price": price,
#                     "endpoint": endpoint,
#                     "product_url": product_url,
#                     "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 }

#                 save_to_db(result)
#                 logging.info(f"MATCH → {name} | pack: {pack} | price: {price} | endpoint: {endpoint} | url: {product_url}")
#                 best_match = result
#                 break  # take first good match

#             except Exception as e:
#                 logging.debug(f"Item error: {str(e)}")
#                 continue

#         if not best_match:
#             logging.warning(f"!! NO GOOD MATCH for {medicine}")

#     except Exception as e:
#         logging.error(f"Critical error for {medicine}: {str(e)}")

# # ================= MAIN =================
# async def main():
#     init_db()
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=HEADLESS, slow_mo=random.randint(80, 200))
#         for med in MEDICINES:
#             context = await browser.new_context(
#                 viewport={"width": random.choice([1280, 1366, 1440]), "height": random.choice([720, 900, 1080])},
#                 user_agent=random.choice(USER_AGENTS),
#                 locale="en-IN",
#                 timezone_id="Asia/Kolkata",
#                 java_script_enabled=True,
#                 ignore_https_errors=True,
#             )
#             page = await context.new_page()
#             try:
#                 await scrape_medicine(page, med)
#             except Exception as e:
#                 logging.error(f"Medicine failed: {med} → {str(e)}")
#             finally:
#                 await context.close()
#                 await asyncio.sleep(random.uniform(12, 30))  # long human-like pause
#         await browser.close()
#     logging.info("=== Scraping completed ===")

# if __name__ == "__main__":
#     asyncio.run(main())

import asyncio
import sqlite3
import logging
import random
import re
from datetime import datetime
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ================= CONFIG =================
BASE_URL = "https://www.apollopharmacy.in/"
DB_NAME = "apollo.db"
HEADLESS = True
DEFAULT_PINCODE = "324001"  # Kota, Rajasthan (your location)

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
        endpoint TEXT,
        product_url TEXT,
        scrape_time TEXT,
        UNIQUE(searched_for, product_url)
    )
    """)
    conn.commit()
    conn.close()

def save_to_db(data):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO medicines 
        (searched_for, name, pack, price, endpoint, product_url, scrape_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data["searched_for"],
        data["name"],
        data["pack"],
        data["price"],
        data["endpoint"],
        data["product_url"],
        data["scrape_time"]
    ))
    conn.commit()
    conn.close()

    pack_part = f" | pack: {data['pack']}" if data['pack'] and data['pack'] != "N/A" else ""
    endpoint_part = f" | endpoint: {data['endpoint']}" if data['endpoint'] else ""
    url_part = f" | url: {data['product_url']}" if data['product_url'] else ""

    logging.info(f"SAVED → {data['name']} | {data['price']}{pack_part}{endpoint_part}{url_part}")

# ================= HELPERS =================
async def human_delay(min_sec=2.5, max_sec=8.0):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

def normalize_text(text: str) -> str:
    return re.sub(r'\s+', ' ', (text or "").strip().lower())

def extract_pack(name: str) -> str:
    match = re.search(r'(\d+)\s*(Tablet|Tab|Rotacap|Capsule|\'s)?', name, re.IGNORECASE)
    return match.group(1) if match else "N/A"

def is_good_match(product_name: str, search_query: str) -> tuple[bool, str]:
    p = normalize_text(product_name)
    q = normalize_text(search_query)

    brand = search_query.split()[0].lower()
    if brand not in p:
        return False, "brand missing"

    forbidden = ["forte", "pg", "vg", "p ", "d ", "ds ", "extra", "g1", "g3", "g4", "0.5", "0 5"]
    for bad in forbidden:
        if bad in p and bad not in q:
            return False, f"rejected variant ({bad.strip()})"

    query_nums = [n for n in re.findall(r'\d+', search_query) if int(n) > 50]
    if query_nums:
        main_wanted = max(query_nums, key=int)
        found = str(main_wanted) in p or f"{main_wanted}mg" in p

        if not found and int(main_wanted) >= 1000:
            gm = str(int(main_wanted) // 1000)
            if f"{gm}gm" in p or f"{gm} gm" in p:
                found = True

        if "foracort" in q and "200" in p and "400" not in p:
            found = True

        if not found:
            return False, f"missing strength {main_wanted}"

    return True, "accepted"

# ================= LOCATION & POPUP HANDLING =================
async def handle_location(page):
    logging.info("Handling location & popups (up to 90s)...")
    try:
        await human_delay(4, 12)

        # Allow browser geolocation prompt if it appears
        try:
            await page.wait_for_selector('button:has-text("Allow"), button[aria-label*="allow"], button:has-text("Allow while visiting the site")', timeout=20000)
            allow_btn = await page.query_selector('button:has-text("Allow"), button[aria-label*="allow"], button:has-text("Allow while visiting the site")')
            if allow_btn:
                await allow_btn.click()
                logging.info("Clicked 'Allow' on browser geolocation prompt")
                await human_delay(4, 10)
        except:
            logging.debug("No browser geolocation prompt found")

        # Apollo custom location popup
        location_triggers = [
            'button:has-text("Detect My Location")',
            'button:has-text("Use my location")',
            'button:has-text("Enter Pincode")',
            'button[class*="location"]',
            '[aria-label*="location"]',
        ]

        trigger_clicked = False
        for sel in location_triggers:
            try:
                btn = await page.wait_for_selector(sel, timeout=15000, state="visible")
                if btn:
                    await btn.click()
                    logging.info(f"Clicked location trigger: {sel}")
                    trigger_clicked = True
                    await human_delay(5, 12)
                    break
            except:
                continue

        # If no trigger clicked, look for pincode input directly
        if not trigger_clicked:
            pin_selectors = [
                'input[placeholder*="Pincode"]',
                'input[placeholder*="Enter PIN"]',
                'input[name="pincode"]',
                'input[id*="pincode"]',
                'input[type="text"][maxlength="6"]',
                'input[placeholder*="Enter your pincode"]',
                'input[autocomplete="postal-code"]',
            ]

            pin_input = None
            for sel in pin_selectors:
                try:
                    pin_input = await page.wait_for_selector(sel, timeout=20000, state="visible")
                    if pin_input:
                        break
                except:
                    continue

            if pin_input:
                logging.info("Pincode input found → filling 324001")
                await pin_input.fill(DEFAULT_PINCODE)
                await human_delay(1.5, 5)

                submit_selectors = [
                    'button:has-text("Submit")',
                    'button:has-text("Confirm")',
                    'button:has-text("Apply")',
                    'button:has-text("Save")',
                    'button:has-text("Continue")',
                    'button[type="submit"]',
                    'button[class*="submit"]',
                    'button[aria-label*="submit"]',
                    'button:has-text("Set Location")',
                ]

                for sel in submit_selectors:
                    try:
                        btn = await page.wait_for_selector(sel, timeout=15000)
                        if btn:
                            await btn.click()
                            logging.info("Pincode/location submitted")
                            await human_delay(10, 20)  # long wait for reload
                            break
                    except:
                        continue

        # Close ALL possible overlays/popups
        close_selectors = [
            'button[aria-label*="close"]',
            'button.close',
            'button[class*="close"]',
            '[class*="close-popup"]',
            'button:has-text("Close")',
            'button:has-text("Skip")',
            'button:has-text("No Thanks")',
            'button:has-text("Later")',
            '[role="dialog"] button[aria-label*="close"]',
            'button[aria-label*="dismiss"]',
            'div[class*="overlay"] button',
            'div[class*="modal"] button.close',
        ]

        for sel in close_selectors:
            try:
                btns = await page.query_selector_all(sel)
                for btn in btns:
                    if await btn.is_visible(timeout=4000):
                        await btn.click()
                        logging.info(f"Overlay closed: {sel}")
                        await human_delay(1, 4)
            except:
                pass

        # Final long wait for search bar
        for attempt in range(4):
            try:
                await page.wait_for_selector('#searchProduct', timeout=60000, state="visible")
                logging.info("Search bar ready after location handling")
                return
            except:
                logging.debug(f"Search bar wait attempt {attempt+1}/4 failed - retrying...")
                await human_delay(6, 12)

        logging.warning("Search bar still not visible after multiple retries")

    except Exception as e:
        logging.warning(f"Location handling error (continuing): {str(e)}")

# ================= SCRAPER =================
async def scrape_medicine(page, medicine: str):
    logging.info(f"→ {medicine}")

    await handle_location(page)

    try:
        search_input = await page.wait_for_selector('#searchProduct', timeout=60000)
        await search_input.click()
        await search_input.fill(medicine)
        await human_delay(5, 12)

        try:
            await page.wait_for_selector("div.MedicineAutoSearch_autoSearchPopover__YtZHq", timeout=40000, state="visible")
            logging.info("Suggestions popover appeared")
        except PlaywrightTimeoutError:
            logging.warning("No suggestions popover - location may not be set correctly")
            return

        await page.evaluate("window.scrollBy(0, 400)")
        await human_delay(2, 5)

        items = await page.query_selector_all("li.ProductSuggestion_suggestionList__mijPN")
        logging.info(f"Found {len(items)} suggestion items")

        best_match = None

        for item in items:
            try:
                name_el = await item.query_selector("h2.DL")
                name = (await name_el.inner_text()).strip() if name_el else ""
                if not name:
                    continue

                ok, reason = is_good_match(name, medicine)
                if not ok:
                    logging.info(f"Rejected {name[:60]}... → {reason}")
                    continue

                link_el = await item.query_selector('a.cardAnchorStyle')
                href = await link_el.get_attribute("href") if link_el else ""
                endpoint = href if href else ""
                product_url = urljoin(BASE_URL, href) if href else ""

                price_el = await item.query_selector("p.cV_")
                price = (await price_el.inner_text()).strip() if price_el else "N/A"

                pack = extract_pack(name)

                result = {
                    "searched_for": medicine,
                    "name": name,
                    "pack": pack,
                    "price": price,
                    "endpoint": endpoint,
                    "product_url": product_url,
                    "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                save_to_db(result)
                logging.info(f"MATCH → {name} | pack: {pack} | price: {price} | endpoint: {endpoint} | url: {product_url}")
                best_match = result
                break

            except Exception as e:
                logging.debug(f"Item error: {str(e)}")
                continue

        if not best_match:
            logging.warning(f"!! NO GOOD MATCH for {medicine}")

    except Exception as e:
        logging.error(f"Critical error for {medicine}: {str(e)}")

# ================= MAIN =================
async def main():
    init_db()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, slow_mo=random.randint(80, 200))
        for med in MEDICINES:
            context = await browser.new_context(
                viewport={"width": random.choice([1280, 1366, 1440]), "height": random.choice([720, 900, 1080])},
                user_agent=random.choice(USER_AGENTS),
                locale="en-IN",
                timezone_id="Asia/Kolkata",
                java_script_enabled=True,
                ignore_https_errors=True,
                
                # Automatically grant geolocation permission (no popup delay)
                permissions=["geolocation"],
                geolocation={"latitude": 25.2138, "longitude": 75.7873},  # Kota coordinates
            )
            page = await context.new_page()
            try:
                await scrape_medicine(page, med)
            except Exception as e:
                logging.error(f"Medicine failed: {med} → {str(e)}")
            finally:
                await context.close()
                await asyncio.sleep(random.uniform(15, 35))
        await browser.close()
    logging.info("=== Scraping completed ===")

if __name__ == "__main__":
    asyncio.run(main())