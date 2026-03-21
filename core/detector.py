# import re

# VARIANTS = ["FORTE", "XR", "SR", "PR", "ER", "CR", "MR"]

# FORMS = ["TABLET", "CAPSULE", "ROTACAP", "SYRUP"]

# def detect_medicine_parts(text: str):
#     t = text.upper()

#     brand = t.split()[0]

#     strength = None
#     m = re.search(r'(\d+)\s*(MG|MCG|G)', t)
#     if m:
#         strength = f"{m.group(1)} {m.group(2)}"

#     form = "TABLET"
#     for f in FORMS:
#         if f in t:
#             form = f
#             break

#     found_variants = [v for v in VARIANTS if v in t]
#     variant = "_".join(found_variants) if found_variants else "NORMAL"

#     canonical = f"{brand} {variant.replace('_',' ')} {strength} {form}"
#     canonical = re.sub(r'\s+', ' ', canonical).strip()

#     return {
#         "brand": brand,
#         "strength": strength,
#         "form": form,
#         "variant": variant,
#         "canonicalName": canonical
#     }

# import re

# VARIANTS = ["FORTE", "XR", "SR", "PR", "ER", "CR", "MR"]
# FORMS = ["TABLET", "CAPSULE", "ROTACAP", "SYRUP", "INJECTION"]

# def detect_medicine_parts(text: str):
#     t = re.sub(r"\s+", " ", text.upper()).strip()

#     # -----------------------------
#     # BRAND
#     # -----------------------------
#     brand = t.split()[0]

#     # -----------------------------
#     # STRENGTH
#     # -----------------------------
#     strength = None
#     m = re.search(r"\b(\d+(?:\.\d+)?)\s*(MG|MCG|G)\b", t)
#     if m:
#         strength = f"{m.group(1)} {m.group(2)}"

#     # -----------------------------
#     # FORM (explicit → fallback TABLET)
#     # -----------------------------
#     form = None
#     for f in FORMS:
#         if re.search(rf"\b{f}\b", t):
#             form = f
#             break
#     if not form:
#         form = "TABLET"

#     # -----------------------------
#     # VARIANT (STRICT WORD MATCH)
#     # -----------------------------
#     found_variants = []
#     for v in VARIANTS:
#         if re.search(rf"\b{v}\b", t):
#             found_variants.append(v)

#     variant = "_".join(found_variants) if found_variants else "NORMAL"

#     # -----------------------------
#     # CANONICAL NAME (STABLE)
#     # -----------------------------
#     canonical = f"{brand} {strength or ''} {form}"
#     canonical = re.sub(r"\s+", " ", canonical).strip()

#     return {
#         "brand": brand,
#         "strength": strength,
#         "form": form,
#         "variant": variant,
#         "canonicalName": canonical,
#     }
import re

# Expanded lists based on real 1mg product patterns (Telma variants, Ecosprin-AV, combo strengths, etc.)
VARIANTS = [
    "FORTE", "FORTE PR", "PLUS", "PLUS DS", "MAX", "ULTRA", "ACTIVE", "FAST", "RAPID",
    "SR", "ER", "XR", "CR", "MR", "PR", "IR", "XL",
    "DT", "MD", "MDT", "ODT",
    "OD", "TR", "RETARD", "LONG", "LA",
    "DS", "LS",
    "DUO", "TRIO", "COMBI",
    "LITE", "ADVANCE", "ADV",
    "KID", "JUNIOR", "PED",
    "NANO", "BOOST",

    "SP", "CV", "CV FORTE",
    "AZ", "MZ", "TZ", "DX", "DXT",

    "H", "CT", "AM", "AT", "AV",
    "LN", "BETA", "ACT", "BS", "NB",
    "MCT", "LNB", "LNC",
    "MT", "RS", "NC", "FIC", "HS",
    "C", "CP", "GP", "CL",

    "A", "D", "R",

    "G-IR", "G-SR", "G-ER"
]

FORMS = [
    "TABLET",
    "CAPSULE",
    "SOFTGEL",
    "INJECTION",
    "SYRUP",
    "SUSPENSION",
    "DROPS",
    "ORAL DROPS",
    "EYE DROPS",
    "EAR DROPS",
    "NASAL DROPS",
    "CREAM",
    "OINTMENT",
    "GEL",
    "LOTION",
    "SPRAY",
    "ORAL SPRAY",
    "POWDER",
    "GRANULES",
    "DRY SYRUP",
    "SOLUTION",
    "ORAL SOLUTION",
    "LIQUID",
    "ELIXIR",
    "EMULSION",
    "LOZENGE",
    "RESPULE",
    "ROTACAP",
    "INHALER",
    "NEBULIZER SOLUTION",
    "SUPPOSITORY",
    "LINIMENT",
    "MOUTHWASH",
    "GARGLE",
    "PATCH"
]


def detect_medicine_parts(text: str):
    """
    Improved medicine name parser for 1mg product list pages.
    
    Fixes:
    1. Ecosprin-AV 75 Capsule → brand="ECOSPRIN", variant="AV" (was wrongly putting everything in brand)
    2. Telma 40 Tablet / Telma 40mg Tablet → strength="40 MG" (unit was missing → now defaults to MG)
    3. Telma-CT 40/6.25 Tablet → strength="40/6.25 MG", variant="CT"
    4. Telma-Beta 25 Tablet ER → variant="BETA_ER"
    5. "40mg" (no space) and "40 MG" both work
    6. Hyphenated first word (Brand-Variant) is now cleanly split
    7. Canonical name is now consistent (always includes "MG" when unit is missing)
    
    This will make DB matching (brand + strength + form + variant) work correctly
    so the full scraped name no longer falls into the "name" column of Product.
    """
    t = re.sub(r"\s+", " ", text.upper()).strip()
    if not t:
        return {
            "brand": "",
            "strength": None,
            "form": "TABLET",
            "variant": "NORMAL",
            "canonicalName": "",
        }

    words = t.split()

    # ====================== BRAND + INITIAL VARIANT (from first word) ======================
    first_word = words[0]
    if "-" in first_word:
        parts = first_word.split("-")
        brand = parts[0]
        initial_variant = parts[1] if len(parts) > 1 else None
    else:
        brand = first_word
        initial_variant = None

    # ====================== FORM ======================
    form = None
    for f in FORMS:
        if re.search(rf"\b{f}\b", t):
            form = f
            break
    if not form:
        form = "TABLET"

    # ====================== STRENGTH (handles 75 MG, 40, 40mg, 40/6.25, 40/6.25 MG) ======================
    strength = None
    m = re.search(r"\b(\d+(?:\.\d+)?(?:/\d+(?:\.\d+)?)?)\s*(MG|MCG|G|ML|IU)?\b", t)
    if m:
        num = m.group(1)
        unit = m.group(2) or "MG"          # default to MG when unit is missing (Telma style)
        strength = f"{num} {unit}".strip()

    # ====================== VARIANT (hyphen + standard list) ======================
    found_variants = []
    if initial_variant:
        found_variants.append(initial_variant)

    for v in VARIANTS:
        if re.search(rf"\b{v}\b", t) and v not in found_variants:
            found_variants.append(v)

    variant = "_".join(sorted(found_variants)) if found_variants else "NORMAL"

    # ====================== CANONICAL NAME (consistent with DB) ======================
    canonical_parts = [brand]
    if strength:
        canonical_parts.append(strength)
    canonical_parts.append(form)
    canonical = re.sub(r"\s+", " ", " ".join(canonical_parts)).strip()

    return {
        "brand": brand,
        "strength": strength,
        "form": form,
        "variant": variant,
        "canonicalName": canonical,
    }