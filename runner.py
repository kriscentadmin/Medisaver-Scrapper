# import asyncio
# from prisma import Prisma
# from scrapers import netmed, pharmaeasy, onemg, truemeds

# db = Prisma()

# async def run():
#     await db.connect()

#     medicines = await db.medicine.find_many(
#         where={"approved": False},
#         take=1000
#     )

#     sem = asyncio.Semaphore(4)

#     async def run_one(med):
#         async with sem:
#             await asyncio.gather(
#                 netmed.scrape_netmeds( med),
#                 pharmaeasy.scrape_pharmeasy(med),
#                 onemg.scrape_1mg(med),
#                 truemeds.scrape_truemeds(med)
#             )

#     await asyncio.gather(*(run_one(m) for m in medicines))
#     await db.disconnect()

# asyncio.run(run())


# ////////////////////////////////////////////////////

# import asyncio
# import json
# from prisma import Prisma
# from playwright.async_api import async_playwright

# from scrapers import netmed, pharmaeasy, onemg, truemeds

# BATCH_SIZE = 10
# PROGRESS_FILE = "scraper_progress.json"

# db = Prisma()


# def get_last_index():
#     try:
#         with open(PROGRESS_FILE, "r") as f:
#             return json.load(f).get("last_index", 0)
#     except:
#         return 0


# def save_last_index(index):
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump({"last_index": index}, f)


# async def run():

#     await db.connect()

#     last_index = get_last_index()

#     medicines = await db.medicine.find_many(
#         where={"approved": False},
#         skip=last_index,
#         take=BATCH_SIZE,
#         order={"id": "asc"}
#     )

#     if not medicines:
#         print("All medicines scraped. Resetting progress.")
#         save_last_index(0)
#         await db.disconnect()
#         return

#     sem = asyncio.Semaphore(4)

#     async with async_playwright() as p:

#         browser = await p.chromium.launch(
#             headless=True,
#             args=["--no-sandbox", "--disable-dev-shm-usage"]
#         )

#         async def run_one(med):
#             async with sem:
#                 try:
#                     await asyncio.gather(
#                         netmed.scrape_netmeds(browser, med),
#                         pharmaeasy.scrape_pharmeasy(browser, med),
#                         onemg.scrape_1mg(browser, med),
#                         truemeds.scrape_truemeds(browser, med)
#                     )
#                 except Exception as e:
#                     print(f"Error scraping {med.canonicalName}: {e}")

#         await asyncio.gather(*(run_one(m) for m in medicines))

#         await browser.close()

#     new_index = last_index + len(medicines)
#     save_last_index(new_index)

#     print(f"Scraped {len(medicines)} medicines. Next index: {new_index}")

#     await db.disconnect()


# asyncio.run(run())

# import asyncio
# import json
# from prisma import Prisma
# from scrapers import netmed, pharmaeasy, onemg, truemeds

# BATCH_SIZE = 10
# PROGRESS_FILE = "scraper_progress.json"

# db = Prisma()


# def get_last_index():
#     try:
#         with open(PROGRESS_FILE, "r") as f:
#             return json.load(f).get("last_index", 0)
#     except:
#         return 0


# def save_last_index(index):
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump({"last_index": index}, f)


# # ------------------------------
# # SAVE PRODUCTS
# # ------------------------------

# async def save_products(medicine_id: int, products: list):
#     for product in products:
#         await db.product.upsert(
#             where={
#                 "medicineId_source": {
#                     "medicineId": medicine_id,
#                     "source": product["source"],
#                 }
#             },
#             data={
#                 "create": {
#                     "medicineId": medicine_id,
#                     **product,
#                 },
#                 "update": {
#                     "name": product["name"],
#                     "price": product.get("price"),
#                     "discount": product.get("discount"),
#                     "pack": product.get("pack"),
#                     "originalPrice": product.get("originalPrice"),
#                     "productUrl": product.get("productUrl"),
#                     "endpoint": product.get("endpoint"),
#                 },
#             },
#         )


# # ------------------------------
# # MAIN RUNNER
# # ------------------------------

# async def run():
#     await db.connect()

#     last_index = get_last_index()

#     medicines = await db.medicine.find_many(
#         where={"approved": False},
#         skip=last_index,
#         take=BATCH_SIZE,
#         order={"id": "asc"},
#     )

#     if not medicines:
#         print("All medicines scraped. Resetting progress.")
#         save_last_index(0)
#         await db.disconnect()
#         return

#     async def run_one(med):
#         print(f"\n🔍 Scraping: {med.canonicalName}")

#         tasks = [
#             netmed.scrape_netmeds(med),
#             pharmaeasy.scrape_pharmeasy(med),
#             onemg.scrape_1mg(med),
#             truemeds.scrape_truemeds(med),
#         ]

#         results = await asyncio.gather(*tasks, return_exceptions=True)

#         for site, result in zip(
#             ["NETMEDS", "PHARMEASY", "1MG", "TRUEMEDS"], results
#         ):
#             if isinstance(result, Exception):
#                 print(f"  ❌ {site} failed → {result}")
#                 continue

#             if not result:
#                 print(f"  ⚠ No data from {site}")
#                 continue

#             await save_products(med.id, result)

#             print(f"  ✅ Saved {len(result)} from {site}")

#     # Run medicines sequentially
#     for med in medicines:
#         await run_one(med)

#     new_index = last_index + len(medicines)
#     save_last_index(new_index)

#     print(f"\nScraped {len(medicines)} medicines. Next index: {new_index}")

#     await db.disconnect()


# asyncio.run(run())
# /////////////////////////////////////

# import asyncio
# import json
# from prisma import Prisma
# from scrapers import netmed, pharmaeasy, onemg, truemeds
# from playwright.async_api import async_playwright

# BATCH_SIZE = 10
# PROGRESS_FILE = "scraper_progress.json"

# db = Prisma()


# def get_last_index():
#     try:
#         with open(PROGRESS_FILE, "r") as f:
#             return json.load(f).get("last_index", 0)
#     except:
#         return 0


# def save_last_index(index):
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump({"last_index": index}, f)


# async def save_products(medicine_id: int, products: list):
#     for product in products:
#         print(f"  → DB SAVE: {product['source']} | {product.get('name')} | ₹{product.get('price')} | pack={product.get('pack')} | orig=₹{product.get('originalPrice')} | disc={product.get('discount')} | {product.get('productUrl')}")
        
#         await db.product.upsert(
#             where={
#                 "medicineId_source": {
#                     "medicineId": medicine_id,
#                     "source": product["source"],
#                 }
#             },
#             data={
#                 "create": {
#                     "medicineId": medicine_id,
#                     **product,
#                 },
#                 "update": {
#                     "name": product["name"],
#                     "price": product.get("price"),
#                     "discount": product.get("discount"),
#                     "pack": product.get("pack"),
#                     "originalPrice": product.get("originalPrice"),
#                     "productUrl": product.get("productUrl"),
#                     "endpoint": product.get("endpoint"),
#                 },
#             },
#         )


# async def run():
#     await db.connect()

#     last_index = get_last_index()

#     medicines = await db.medicine.find_many(
#         where={"approved": False},
#         skip=last_index,
#         take=BATCH_SIZE,
#         order={"id": "asc"},
#     )

#     if not medicines:
#         print("All medicines scraped. Resetting progress.")
#         save_last_index(0)
#         await db.disconnect()
#         return

#     async with async_playwright() as p:
#         browser = await p.chromium.launch(
#             headless=True,
#             args=[
#                 "--no-sandbox",
#                 "--disable-dev-shm-usage",
#                 "--disable-blink-features=AutomationControlled",
#                 "--disable-features=IsolateOrigins,site-per-process",
#                 "--disable-web-security",
#             ],
#         )
#         context = await browser.new_context(
#             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
#             locale="en-IN",
#             timezone_id="Asia/Kolkata",
#             viewport={"width": 1366, "height": 768},
#         )
#         page = await context.new_page()

#         # Initial load to handle any first-time stuff
#         await page.goto("https://www.1mg.com", wait_until="domcontentloaded", timeout=60000)
#         await asyncio.sleep(4)  # give time for popup

#         async def run_one(med):
#             print(f"\n🔍 Scraping: {med.canonicalName}")

#             # Only 1mg for now (uncomment others later)
#             try:
#                 result_1mg = await onemg.scrape_1mg(med, page)  # pass shared page
#                 if isinstance(result_1mg, Exception):
#                     print(f"  ❌ 1MG failed → {result_1mg}")
#                 elif result_1mg:
#                     await save_products(med.id, result_1mg)
#                     print(f"  ✅ Saved {len(result_1mg)} from 1MG")
#                 else:
#                     print("  ⚠ No data from 1MG")
#             except Exception as e:
#                 print(f"  ❌ 1MG exception: {e}")

#             # Add other sites here later (they can use same page or new if needed)
#             await asyncio.sleep(20)

#         for med in medicines:
#             await run_one(med)

#         await browser.close()

#     new_index = last_index + len(medicines)
#     save_last_index(new_index)
#     print(f"\nScraped {len(medicines)} medicines. Next index: {new_index}")

#     await db.disconnect()


# asyncio.run(run())

import asyncio
import json
from prisma import Prisma
from scrapers import pharmaeasy, onemg
from playwright.async_api import async_playwright

BATCH_SIZE = 10
CONCURRENT_TASKS = 3
PROGRESS_FILE = "scraper_progress.json"

db = Prisma()


def get_last_index():
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f).get("last_index", 0)
    except:
        return 0


def save_last_index(index):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_index": index}, f)


async def save_products(medicine_id: int, products: list):
    for product in products:
        print(
            f"  → DB SAVE: {product['source']} | {product.get('name')} | ₹{product.get('price')}"
        )

        await db.product.upsert(
            where={
                "medicineId_source": {
                    "medicineId": medicine_id,
                    "source": product["source"],
                }
            },
            data={
                "create": {
                    "medicineId": medicine_id,
                    **product,
                },
                "update": {
                    "name": product["name"],
                    "price": product.get("price"),
                    "discount": product.get("discount"),
                    "pack": product.get("pack"),
                    "originalPrice": product.get("originalPrice"),
                    "productUrl": product.get("productUrl"),
                    "endpoint": product.get("endpoint"),
                },
            },
        )


async def scrape_single_med(browser, med):
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        viewport={"width": 1366, "height": 768},
    )

    page_1mg = await context.new_page()
    page_pharma = await context.new_page()

    print(f"\n🔍 Scraping: {med.canonicalName}")

    try:
        # Run both sites in parallel
        task_1mg = onemg.scrape_1mg(med, page_1mg)
        task_pharma = pharmaeasy.scrape_pharmeasy(med, page_pharma)

        results = await asyncio.gather(task_1mg, task_pharma, return_exceptions=True)

        for res in results:
            if isinstance(res, Exception):
                print(f"  ❌ Error: {res}")
                continue

            if res:
                await save_products(med.id, res)
                print(f"  ✅ Saved {len(res)} products")

    finally:
        await page_1mg.close()
        await page_pharma.close()
        await context.close()


async def run():
    await db.connect()

    last_index = get_last_index()

    medicines = await db.medicine.find_many(
        where={"approved": False},
        skip=last_index,
        take=BATCH_SIZE,
        order={"id": "asc"},
    )

    if not medicines:
        print("All medicines scraped. Resetting progress.")
        save_last_index(0)
        await db.disconnect()
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

        async def limited_task(med):
            async with semaphore:
                await scrape_single_med(browser, med)

        # 🔥 PARALLEL EXECUTION
        await asyncio.gather(*[limited_task(med) for med in medicines])

        await browser.close()

    new_index = last_index + len(medicines)
    save_last_index(new_index)

    print(f"\n✅ Scraped {len(medicines)} medicines. Next index: {new_index}")

    await db.disconnect()


asyncio.run(run())

# import asyncio
# import json
# from prisma import Prisma
# from scrapers import netmed, pharmaeasy, onemg, truemeds

# BATCH_SIZE = 10
# PROGRESS_FILE = "scraper_progress.json"

# db = Prisma()


# def get_last_index():
#     try:
#         with open(PROGRESS_FILE, "r") as f:
#             return json.load(f).get("last_index", 0)
#     except:
#         return 0


# def save_last_index(index):
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump({"last_index": index}, f)


# # ------------------------------
# # SAVE PRODUCTS
# # ------------------------------

# async def save_products(medicine_id: int, products: list):
#     for product in products:
#         print(f"  → DB SAVE: {product['source']} | {product.get('name')} | ₹{product.get('price')} | pack={product.get('pack')} | orig=₹{product.get('originalPrice')} | disc={product.get('discount')} | {product.get('productUrl')}")
        
#         await db.product.upsert(
#             where={
#                 "medicineId_source": {
#                     "medicineId": medicine_id,
#                     "source": product["source"],
#                 }
#             },
#             data={
#                 "create": {
#                     "medicineId": medicine_id,
#                     **product,
#                 },
#                 "update": {
#                     "name": product["name"],
#                     "price": product.get("price"),
#                     "discount": product.get("discount"),
#                     "pack": product.get("pack"),
#                     "originalPrice": product.get("originalPrice"),
#                     "productUrl": product.get("productUrl"),
#                     "endpoint": product.get("endpoint"),
#                 },
#             },
#         )


# # ------------------------------
# # MAIN RUNNER
# # ------------------------------

# async def run():
#     await db.connect()

#     last_index = get_last_index()

#     medicines = await db.medicine.find_many(
#         where={"approved": False},
#         skip=last_index,
#         take=BATCH_SIZE,
#         order={"id": "asc"},
#     )

#     if not medicines:
#         print("All medicines scraped. Resetting progress.")
#         save_last_index(0)
#         await db.disconnect()
#         return

#     async def run_one(med):
#         print(f"\n🔍 Scraping: {med.canonicalName}")

#         tasks = [
#             # netmed.scrape_netmeds(med),
#             # pharmaeasy.scrape_pharmeasy(med),
#             onemg.scrape_1mg(med),
#             # truemeds.scrape_truemeds(med),
#         ]

#         results = await asyncio.gather(*tasks, return_exceptions=True)

#         for site, result in zip(
#             ["NETMEDS", "PHARMEASY", "1MG", "TRUEMEDS"], results
#         ):
#             if isinstance(result, Exception):
#                 print(f"  ❌ {site} failed → {result}")
#                 continue

#             if not result:
#                 print(f"  ⚠ No data from {site}")
#                 continue

#             await save_products(med.id, result)

#             print(f"  ✅ Saved {len(result)} from {site}")

#     # Run medicines sequentially
#     for med in medicines:
#         await run_one(med)
#         await asyncio.sleep(20)  # Production safety: 20s delay per medicine (IP-block safe for 700/day)

#     new_index = last_index + len(medicines)
#     save_last_index(new_index)

#     print(f"\nScraped {len(medicines)} medicines. Next index: {new_index}")

#     await db.disconnect()


# asyncio.run(run())