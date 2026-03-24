import asyncio
import sys
from pathlib import Path

import pandas as pd
from prisma import Prisma

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from utils.medicine_parser import parse_medicine  # noqa: E402

CSV_PATH = "data/Medicine_details.csv"
FAILED_LOG = "failed_medicines.txt"
ALLOWED_FORMS = {"TABLET", "CAPSULE", "SYRUP", "INJECTION"}

db = Prisma()


async def seed_medicines() -> None:
    await db.connect()

    df = pd.read_csv(CSV_PATH)
    df.columns = [column.strip().lower() for column in df.columns]

    if "medicine name" not in df.columns:
        raise Exception("Column 'Medicine Name' not found in CSV")

    name_column = "medicine name"

    seen_canonical_names: set[str] = set()
    inserted = 0
    skipped_wrong_form = 0
    skipped_duplicate_in_csv = 0
    skipped_existing_in_db = 0
    failed = 0

    for idx, raw_name in enumerate(df[name_column].dropna(), 1):
        raw_name = str(raw_name).strip()
        if not raw_name:
            continue

        try:
            parsed = await parse_medicine(raw_name)

            form = parsed["form"].upper()
            if form not in ALLOWED_FORMS:
                skipped_wrong_form += 1
                continue

            canonical = parsed["canonicalName"]
            if canonical in seen_canonical_names:
                skipped_duplicate_in_csv += 1
                continue

            seen_canonical_names.add(canonical)

            exists = await db.medicine.find_unique(where={"canonicalName": canonical})
            if exists:
                skipped_existing_in_db += 1
                continue

            await db.medicine.create(
                data={
                    "brand": parsed["brand"],
                    "strength": parsed["strength"],
                    "form": form,
                    "variant": parsed["variant"],
                    "canonicalName": canonical,
                    "approved": False,
                }
            )

            inserted += 1

            print(
                f"{idx}. CSV: {raw_name} -> Canonical: {canonical} "
                f"Brand: {parsed['brand']} Strength: {parsed['strength']} "
                f"Variant: {parsed['variant']} Form: {form}"
            )

            if inserted % 100 == 0:
                print(f"Inserted {inserted} medicines...")

        except Exception as exc:
            failed += 1
            print("\nFAILED ROW")
            print("Medicine:", raw_name)
            print("Reason:", str(exc))

            try:
                parsed_debug = await parse_medicine(raw_name, debug=True)
                print("Parsed Output:", parsed_debug)
            except Exception as parser_error:
                print("Parser Error:", parser_error)

            with open(FAILED_LOG, "a", encoding="utf-8") as file:
                file.write(f"{raw_name} -> {str(exc)}\n")

    await db.disconnect()

    print("\n===================================")
    print(f"Inserted medicines       : {inserted}")
    print(f"Skipped (wrong form)     : {skipped_wrong_form}")
    print(f"Duplicate in CSV         : {skipped_duplicate_in_csv}")
    print(f"Already in DB            : {skipped_existing_in_db}")
    print(f"Failed rows              : {failed}")
    print("===================================")


if __name__ == "__main__":
    asyncio.run(seed_medicines())
