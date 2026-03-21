import sys
from pathlib import Path
import asyncio
from prisma import Prisma

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from scrapers.pharmaeasy import scrape_pharmeasy
from scrapers.netmed import scrape_netmeds
from scrapers.onemg import scrape_1mg  # renamed to match previous refactor
from scrapers.truemeds import scrape_truemeds

db = Prisma()

SCRAPERS = [
    ("PHARMEASY", scrape_pharmeasy),
    ("NETMEDS", scrape_netmeds),
    ("ONEMG", scrape_1mg),
    ("TRUEMEDS", scrape_truemeds),
]

async def save_products(medicine_id: int, products: list):
    for product in products:
        # Only update price and discount (optional: extend if needed)
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
            }
        )

async def main():
    await db.connect()

    # Get first 200 medicines
    medicines = await db.medicine.find_many(
        take=200,
        order={"id": "asc"}
    )

    for med in medicines:
        print(f"\n🔍 Scraping: {med.canonicalName}")

        for source, scraper in SCRAPERS:
            try:
                # Pass full medicine object, as scrapers expect it
                products = await scraper(med)

                if not products:
                    print(f"  ⚠ No data from {source}")
                    continue

                await save_products(med.id, products)
                print(f"  ✅ Saved {len(products)} from {source}")

            except Exception as e:
                print(f"  ❌ {source} failed → {e}")

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(main())


# import sys
# from pathlib import Path
# import asyncio
# from prisma import Prisma
# from playwright.async_api import async_playwright

# ROOT_DIR = Path(__file__).resolve().parents[1]
# sys.path.append(str(ROOT_DIR))

# from scrapers.pharmaeasy import scrape_pharmeasy
# from scrapers.netmed import scrape_netmeds
# from scrapers.onemg import scrape_1mg
# from scrapers.truemeds import scrape_truemeds

# db = Prisma()

# SCRAPERS = [
#     ("PHARMEASY", scrape_pharmeasy),
#     ("NETMEDS", scrape_netmeds),
#     ("ONEMG", scrape_1mg),
#     ("TRUEMEDS", scrape_truemeds),
# ]


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


# async def main():

#     await db.connect()

#     medicines = await db.medicine.find_many(
#         take=200,
#         order={"id": "asc"}
#     )

#     async with async_playwright() as p:

#         browser = await p.chromium.launch(
#             headless=True
#         )

#         for med in medicines:

#             print(f"\n🔍 Scraping: {med.canonicalName}")

#             for source, scraper in SCRAPERS:

#                 try:

#                     products = await scraper(browser, med)

#                     if not products:
#                         print(f"  ⚠ No data from {source}")
#                         continue

#                     await save_products(med.id, products)

#                     print(f"  ✅ Saved {len(products)} from {source}")

#                 except Exception as e:

#                     print(f"  ❌ {source} failed → {e}")

#         await browser.close()

#     await db.disconnect()


# if __name__ == "__main__":
#     asyncio.run(main())
