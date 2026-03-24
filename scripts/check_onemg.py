import asyncio
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright
from prisma import Prisma

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from scrapers.base import human_delay, random_ua  # noqa: E402
from scrapers.onemg import scrape_1mg  # noqa: E402

db = Prisma()
RESTART_DELAY_SECONDS = 30


async def get_named_medicine(canonical_name: str):
    return await db.medicine.find_first(
        where={"canonicalName": canonical_name.upper().strip()},
        include={"products": True},
    )


async def get_next_pending_medicine(last_seen_id: int | None):
    where = {"approved": False}
    if last_seen_id is not None:
        where = {"approved": False, "id": {"gt": last_seen_id}}

    return await db.medicine.find_first(
        where=where,
        order={"id": "asc"},
        include={"products": True},
    )


def normalize_numeric_string(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    cleaned = re.sub(r"(?i)(rs\.?|inr|off)", "", text)
    cleaned = cleaned.replace(",", "")
    cleaned = cleaned.replace("%", "")
    cleaned = cleaned.replace("\u20b9", "")
    cleaned = re.sub(r"[^\d.]", "", cleaned)

    if not cleaned or cleaned.count(".") > 1:
        return None

    return cleaned


async def save_products(medicine_id: int, products: list[dict]) -> None:
    for product in products:
        payload = {
            "source": product["source"],
            "name": product["name"],
            "pack": product.get("pack"),
            "price": normalize_numeric_string(product.get("price")),
            "originalPrice": normalize_numeric_string(product.get("originalPrice")),
            "discount": normalize_numeric_string(product.get("discount")),
            "productUrl": product.get("productUrl"),
            "endpoint": product.get("endpoint"),
        }
        await db.product.upsert(
            where={
                "medicineId_source": {
                    "medicineId": medicine_id,
                    "source": payload["source"],
                }
            },
            data={
                "create": {
                    "medicineId": medicine_id,
                    **payload,
                },
                "update": {
                    "name": payload["name"],
                    "pack": payload.get("pack"),
                    "price": payload.get("price"),
                    "originalPrice": payload.get("originalPrice"),
                    "discount": payload.get("discount"),
                    "productUrl": payload.get("productUrl"),
                    "endpoint": payload.get("endpoint"),
                },
            },
        )


def print_db_medicine(medicine) -> None:
    onemg_product = next(
        (
            product
            for product in getattr(medicine, "products", [])
            if getattr(getattr(product, "source", None), "name", str(getattr(product, "source", ""))) == "ONEMG"
        ),
        None,
    )

    print("\nDB medicine:")
    print(
        {
            "id": medicine.id,
            "brand": medicine.brand,
            "variant": medicine.variant,
            "strength": medicine.strength,
            "form": medicine.form,
            "canonicalName": medicine.canonicalName,
            "productUrl": getattr(onemg_product, "productUrl", None),
            "endpoint": getattr(onemg_product, "endpoint", None),
        }
    )


def print_results(results, final_url: str) -> None:
    print("\n1mg results:")
    if not results:
        print("No products matched.")
        print(f"Final page URL: {final_url}")
        print(f"Final endpoint: {urlparse(final_url).path if final_url else None}")
        return

    for result in results:
        print(result)


async def check_medicine(playwright, medicine) -> None:
    print_db_medicine(medicine)

    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    context = await browser.new_context(
        user_agent=random_ua(),
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        viewport={"width": 1366, "height": 768},
    )
    page = await context.new_page()

    try:
        results = await scrape_1mg(medicine, page)
        final_url = page.url
    finally:
        await page.close()
        await context.close()
        await browser.close()

    if results and int(results[0].get("_score", 0)) >= 100:
        await save_products(medicine.id, results)
        print(f"Saved {len(results)} ONEMG product(s) to DB.")

    print_results(results, final_url)


async def run_specific_medicine(playwright, canonical_name: str) -> None:
    medicine = await get_named_medicine(canonical_name)
    if not medicine:
        print("Medicine not found.")
        return

    while True:
        await check_medicine(playwright, medicine)
        await human_delay(5, 10)


async def run_pending_medicines(playwright) -> None:
    last_seen_id: int | None = None

    while True:
        medicine = await get_next_pending_medicine(last_seen_id)
        if not medicine:
            print(f"\nReached end of pending medicines. Restarting in {RESTART_DELAY_SECONDS}s.")
            last_seen_id = None
            await asyncio.sleep(RESTART_DELAY_SECONDS)
            continue

        last_seen_id = medicine.id
        await check_medicine(playwright, medicine)
        await human_delay(5, 10)


async def main() -> None:
    canonical_name = sys.argv[1] if len(sys.argv) > 1 else None

    await db.connect()
    try:
        async with async_playwright() as playwright:
            if canonical_name:
                await run_specific_medicine(playwright, canonical_name)
            else:
                await run_pending_medicines(playwright)
    finally:
        await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
