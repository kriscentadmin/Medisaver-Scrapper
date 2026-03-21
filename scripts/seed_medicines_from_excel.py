# import sys
# from pathlib import Path
# import asyncio
# import pandas as pd
# from prisma import Prisma

# # -------------------------------------------------
# # Ensure project root is in PYTHONPATH
# # -------------------------------------------------
# ROOT_DIR = Path(__file__).resolve().parents[1]
# sys.path.append(str(ROOT_DIR))

# from utils.medicine_parser import parse_medicine  # noqa

# # -------------------------------------------------
# # CONFIG
# # -------------------------------------------------
# EXCEL_PATH = "data/Lis for RX and Non RX medicine (1).xlsx"
# MAX_MEDICINES = 200   # 🔒 HARD LIMIT

# db = Prisma()

# # -------------------------------------------------
# # MAIN SEED FUNCTION
# # -------------------------------------------------
# async def seed_medicines():
#     await db.connect()

#     df = pd.read_excel(EXCEL_PATH)

#     df.columns = [c.strip().lower() for c in df.columns]
#     name_column = df.columns[0]

#     seen_canonical_names = set()

#     inserted = 0
#     skipped_duplicate_in_csv = 0
#     skipped_existing_in_db = 0
#     failed = 0

#     for raw_name in df[name_column].dropna():
#         # 🔒 STOP WHEN LIMIT IS REACHED
#         if inserted >= MAX_MEDICINES:
#             break

#         try:
#             parsed = parse_medicine(str(raw_name))

#             # Keep your safety rule
#             if parsed["strength"] == "UNKNOWN":
#                 failed += 1
#                 continue

#             canonical = parsed["canonicalName"]

#             if canonical in seen_canonical_names:
#                 skipped_duplicate_in_csv += 1
#                 continue

#             seen_canonical_names.add(canonical)

#             exists = await db.medicine.find_unique(
#                 where={"canonicalName": canonical}
#             )
#             if exists:
#                 skipped_existing_in_db += 1
#                 continue

#             await db.medicine.create(
#                 data={
#                     "brand": parsed["brand"],
#                     "strength": parsed["strength"],
#                     "form": parsed["form"],
#                     "variant": parsed["variant"],
#                     "canonicalName": canonical,
#                     "approved": False,
#                 }
#             )

#             inserted += 1

#         except Exception as e:
#             print(f" Failed to insert: {raw_name} → {e}")
#             failed += 1

#     await db.disconnect()

#     print("===================================")
#     print(f"Inserted medicines (LIMITED) : {inserted}")
#     print(f"Skipped duplicate in CSV     : {skipped_duplicate_in_csv}")
#     print(f"Skipped existing in DB       : {skipped_existing_in_db}")
#     print(f"Failed rows                  : {failed}")
#     print("===================================")

# # -------------------------------------------------
# # ENTRY POINT
# # -------------------------------------------------
# if __name__ == "__main__":
#     asyncio.run(seed_medicines())

# import sys
# from pathlib import Path
# import asyncio
# import pandas as pd
# from prisma import Prisma

# # -------------------------------------------------
# # Ensure project root is in PYTHONPATH
# # -------------------------------------------------
# ROOT_DIR = Path(__file__).resolve().parents[1]
# sys.path.append(str(ROOT_DIR))

# from utils.medicine_parser import parse_medicine  # noqa

# # -------------------------------------------------
# # CONFIG
# # -------------------------------------------------
# CSV_PATH = "data/Medicine_details.csv"

# db = Prisma()

# # -------------------------------------------------
# # MAIN SEED FUNCTION
# # -------------------------------------------------
# async def seed_medicines():

#     await db.connect()

#     df = pd.read_csv(CSV_PATH)

#     # normalize column names
#     df.columns = [c.strip().lower() for c in df.columns]

#     if "medicine name" not in df.columns:
#         raise Exception("Column 'Medicine Name' not found in CSV")

#     name_column = "medicine name"

#     seen_canonical_names = set()

#     inserted = 0
#     skipped_duplicate_in_csv = 0
#     skipped_existing_in_db = 0
#     failed = 0

#     for raw_name in df[name_column].dropna():

#         try:

#             parsed = parse_medicine(str(raw_name))

#             # skip if strength unknown
#             if parsed["strength"] == "UNKNOWN":
#                 failed += 1
#                 continue

#             canonical = parsed["canonicalName"]

#             # skip duplicates in CSV
#             if canonical in seen_canonical_names:
#                 skipped_duplicate_in_csv += 1
#                 continue

#             seen_canonical_names.add(canonical)

#             # skip if already in DB
#             exists = await db.medicine.find_unique(
#                 where={"canonicalName": canonical}
#             )

#             if exists:
#                 skipped_existing_in_db += 1
#                 continue

#             await db.medicine.create(
#                 data={
#                     "brand": parsed["brand"],
#                     "strength": parsed["strength"],
#                     "form": parsed["form"],
#                     "variant": parsed["variant"],
#                     "canonicalName": canonical,
#                     "approved": False,
#                 }
#             )

#             inserted += 1

#             # progress log every 500
#             if inserted % 500 == 0:
#                 print(f"Inserted {inserted} medicines...")

#         except Exception as e:
#             print(f"❌ Failed to insert: {raw_name} → {e}")
#             failed += 1

#     await db.disconnect()

#     print("\n===================================")
#     print(f"Inserted medicines           : {inserted}")
#     print(f"Skipped duplicate in CSV     : {skipped_duplicate_in_csv}")
#     print(f"Skipped existing in DB       : {skipped_existing_in_db}")
#     print(f"Failed rows                  : {failed}")
#     print("===================================")


# # -------------------------------------------------
# # ENTRY POINT
# # -------------------------------------------------
# if __name__ == "__main__":
#     asyncio.run(seed_medicines())
import sys
from pathlib import Path
import asyncio
import pandas as pd
from prisma import Prisma

# -------------------------------------------------
# Ensure project root is in PYTHONPATH
# -------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from utils.medicine_parser import parse_medicine  # noqa

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
CSV_PATH = "data/Medicine_details.csv"
FAILED_LOG = "failed_medicines.txt"
ALLOWED_FORMS = {"TABLET", "CAPSULE", "SYRUP", "INJECTION"}
MAX_SAMPLE = 100  # sample size for testing

db = Prisma()

# -------------------------------------------------
# MAIN SEED FUNCTION
# -------------------------------------------------
async def seed_medicines():
    await db.connect()

    # -----------------------------
    # Load random sample
    # -----------------------------
    df = pd.read_csv(CSV_PATH).sample(n=MAX_SAMPLE)

    df.columns = [c.strip().lower() for c in df.columns]
    if "medicine name" not in df.columns:
        raise Exception("Column 'Medicine Name' not found in CSV")

    name_column = "medicine name"

    seen_canonical_names = set()
    inserted = 0
    skipped_wrong_form = 0
    skipped_duplicate_in_csv = 0
    skipped_existing_in_db = 0
    failed = 0

    for idx, raw_name in enumerate(df[name_column].dropna(), 1):
        raw_name = str(raw_name).strip()
        try:
            parsed = parse_medicine(raw_name)

            # -------------------------
            # Validation
            # -------------------------
            if not parsed["brand"]:
                raise ValueError("Brand not detected")

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

            # -------------------------
            # Insert
            # -------------------------
            await db.medicine.create(
                data={
                    "brand": parsed["brand"],
                    "strength": parsed["strength"] or "UNKNOWN",
                    "form": form,
                    "variant": parsed["variant"],
                    "canonicalName": canonical,
                    "approved": False,
                }
            )

            inserted += 1

            # -------------------------
            # Debug/Progress log
            # -------------------------
            print(f"{idx}. CSV: {raw_name} → Canonical: {canonical} Brand:{parsed["brand"]} Strength: {parsed["strength"]} Variant: {parsed["variant"]} Form: {form}")

            if inserted % 50 == 0:
                print(f"✅ Inserted {inserted} medicines...")

        except Exception as e:
            failed += 1
            print("\n❌ FAILED ROW")
            print("Medicine:", raw_name)
            print("Reason:", str(e))

            try:
                parsed_debug = parse_medicine(raw_name)
                print("Parsed Output:", parsed_debug)
            except Exception as parser_error:
                print("Parser Error:", parser_error)

            with open(FAILED_LOG, "a", encoding="utf-8") as f:
                f.write(f"{raw_name} -> {str(e)}\n")

    await db.disconnect()

    # -------------------------
    # Summary
    # -------------------------
    print("\n===================================")
    print(f"Inserted medicines       : {inserted}")
    print(f"Skipped (wrong form)     : {skipped_wrong_form}")
    print(f"Duplicate in CSV         : {skipped_duplicate_in_csv}")
    print(f"Already in DB            : {skipped_existing_in_db}")
    print(f"Failed rows              : {failed}")
    print("===================================")


# -------------------------------------------------
# ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    asyncio.run(seed_medicines())
