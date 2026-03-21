# import re

# VARIANT_KEYWORDS = {
#     " XR ": "XR",
#     " SR ": "SR",
#     " PR ": "PR",
#     " FORTE ": "FORTE",
#     " AV ": "AV",
#     " MET ": "MET",
# }

# FORMS = ["TABLET", "CAPSULE", "SYRUP", "INJECTION", "ROTACAP"]

# def normalize(text: str) -> str:
#     return re.sub(r"\s+", " ", text.upper().strip())

# def extract_brand(name: str) -> str:
#     return name.split()[0]

# def extract_strength(name: str) -> str:
#     matches = re.findall(r"(\d+(?:/\d+)?\s*(?:MG|MCG|G))", name)
#     return matches[0] if matches else "UNKNOWN"

# def extract_form(name: str) -> str:
#     for f in FORMS:
#         if f in name:
#             return f
#     return "OTHER"

# def extract_variant(name: str) -> str:
#     padded = f" {name} "
#     for key, val in VARIANT_KEYWORDS.items():
#         if key in padded:
#             return val
#     return "NORMAL"

# def build_canonical_name(brand, variant, strength, form):
#     parts = [brand]
#     if variant != "NORMAL":
#         parts.append(variant)
#     parts.append(strength)
#     parts.append(form)
#     return " ".join(parts)

# def parse_medicine(raw_name: str):
#     name = normalize(raw_name)

#     brand = extract_brand(name)
#     strength = extract_strength(name)
#     form = extract_form(name)
#     variant = extract_variant(name)

#     canonical_name = build_canonical_name(
#         brand=brand,
#         variant=variant,
#         strength=strength,
#         form=form
#     )

#     return {
#         "brand": brand,
#         "strength": strength,
#         "form": form,
#         "variant": variant,
#         "canonicalName": canonical_name,
#     }

# import re

# # -------- VARIANTS --------
# VARIANT_KEYWORDS = {

#     # Release Types
#     " XR ": "XR",          # Extended Release
#     " ER ": "ER",          # Extended Release
#     " SR ": "SR",          # Sustained Release
#     " CR ": "CR",          # Controlled Release
#     " PR ": "PR",          # Prolonged Release
#     " MR ": "MR",          # Modified Release
#     " IR ": "IR",          # Immediate Release
#     " DR ": "DR",          # Delayed Release
#     " TR ": "TR",          # Timed Release
#     " XL ": "XL",
#     " LA ": "LA",          # Long Acting
#     " OD ": "OD",          # Once Daily

#     # Strength / Power Variants
#     " FORTE ": "FORTE",
#     " PLUS ": "PLUS",
#     " MAX ": "MAX",
#     " EXTRA ": "EXTRA",
#     " ADVANCE ": "ADVANCE",
#     " ADVANCED ": "ADVANCED",
#     " ULTRA ": "ULTRA",
#     " SUPER ": "SUPER",
#     " POWER ": "POWER",

#     # Combination / Special Versions
#     " DUO ": "DUO",
#     " DUAL ": "DUAL",
#     " COMBI ": "COMBI",
#     " COMBO ": "COMBO",

#     # Common Indian Variant Codes (very common in brands)
#     " AM ": "AM",
#     " PM ": "PM",
#     " A ": "A",
#     " B ": "B",
#     " D ": "D",
#     " H ": "H",
#     " M ": "M",
#     " T ": "T",
#     " P ": "P",
#     " L ": "L",
#     " K ": "K",
#     " Z ": "Z",

#     # Hypertension / Combination codes
#     " AV ": "AV",
#     " AT ": "AT",
#     " HT ": "HT",
#     " CT ": "CT",
#     " CH ": "CH",
#     " MT ": "MT",
#     " TZ ": "TZ",
#     " AZ ": "AZ",
#     " CV ": "CV",
#     " V ": "V",

#     # Pediatric / Special
#     " KID ": "KID",
#     " JUNIOR ": "JUNIOR",
#     " PED ": "PED",
#     " BABY ": "BABY",

#     # Sugar / Diet Variants
#     " SUGAR FREE ": "SUGAR FREE",
#     " SF ": "SF",

#     # Injection / Special Form Variants
#     " DEPOT ": "DEPOT",
#     " RETARD ": "RETARD",
#     " DISC ": "DISC",
#     " RAPID ": "RAPID",
# }


# # -------- FORMS --------
# FORMS = [

#     # Solid Oral Forms
#     "TABLET",
#     "CAPSULE",
#     "SOFTGEL",
#     "CHEWABLE TABLET",
#     "DISPERSIBLE TABLET",
#     "EFFERVESCENT TABLET",
#     "SUBLINGUAL TABLET",
#     "BUCCAL TABLET",
#     "LOZENGE",
#     "PASTILLE",
#     "GRANULES",
#     "POWDER",
#     "SACHET",

#     # Liquid Oral Forms
#     "SYRUP",
#     "SUSPENSION",
#     "ORAL SUSPENSION",
#     "ORAL SOLUTION",
#     "SOLUTION",
#     "ELIXIR",
#     "DROPS",
#     "ORAL DROPS",

#     # Injectable Forms
#     "INJECTION",
#     "IV INJECTION",
#     "IM INJECTION",
#     "SC INJECTION",
#     "PREFILLED SYRINGE",
#     "VIAL",
#     "AMPOULE",
#     "INFUSION",

#     # Topical Forms
#     "CREAM",
#     "OINTMENT",
#     "GEL",
#     "LOTION",
#     "PASTE",
#     "FOAM",
#     "LINIMENT",

#     # Respiratory Forms
#     "INHALER",
#     "ROTACAP",
#     "RESPULE",
#     "NEBULE",
#     "NEBULIZER SOLUTION",
#     "INHALATION CAPSULE",
#     "INHALATION POWDER",

#     # Eye / Ear / Nasal
#     "EYE DROPS",
#     "EAR DROPS",
#     "NASAL DROPS",
#     "NASAL SPRAY",
#     "EYE OINTMENT",
#     "OPHTHALMIC SOLUTION",

#     # Rectal / Vaginal
#     "SUPPOSITORY",
#     "PESSARY",
#     "ENEMA",

#     # Transdermal / Others
#     "PATCH",
#     "TRANSDERMAL PATCH",
#     "SPRAY",
#     "MOUTHWASH",
#     "ORAL GEL",
#     "DENTAL GEL",
#     "SHAMPOO",
#     "SOAP",
# ]



# # -------- NORMALIZE --------
# def normalize(text: str) -> str:
#     text = text.upper()
#     text = re.sub(r"\s+", " ", text)
#     return text.strip()


# # -------- BRAND --------
# def extract_brand(name: str) -> str:
#     words = name.split()

#     # brand usually first word
#     return words[0]


# # -------- STRENGTH --------
# def extract_strength(name: str) -> str:
#     pattern = r"\d+(?:\.\d+)?(?:/\d+)?\s?(?:MG|MCG|G|ML|IU|%)"
#     matches = re.findall(pattern, name)

#     if matches:
#         return matches[0]

#     return "UNKNOWN"


# # -------- FORM --------
# def extract_form(name: str) -> str:
#     for form in FORMS:
#         if f" {form} " in f" {name} ":
#             return form

#     return "OTHER"


# # -------- VARIANT --------
# def extract_variant(name: str) -> str:
#     padded = f" {name} "

#     for key, val in VARIANT_KEYWORDS.items():
#         if key in padded:
#             return val

#     return "NORMAL"


# # -------- CANONICAL NAME --------
# def build_canonical_name(brand, variant, strength, form):

#     parts = [brand]

#     if variant != "NORMAL":
#         parts.append(variant)

#     if strength != "UNKNOWN":
#         parts.append(strength)

#     parts.append(form)

#     return " ".join(parts)


# # -------- MAIN PARSER --------
# def parse_medicine(raw_name: str):

#     name = normalize(raw_name)

#     brand = extract_brand(name)
#     strength = extract_strength(name)
#     form = extract_form(name)
#     variant = extract_variant(name)

#     canonical_name = build_canonical_name(
#         brand=brand,
#         variant=variant,
#         strength=strength,
#         form=form
#     )

#     return {
#         "brand": brand,
#         "strength": strength,
#         "form": form,
#         "variant": variant,
#         "canonicalName": canonical_name,
#     }


# import re


# # ---------------- NORMALIZE ----------------
# def normalize(text: str) -> str:
#     text = text.upper()

#     text = text.replace("-", " ")
#     text = text.replace("+", " + ")
#     text = text.replace("/", " / ")

#     text = re.sub(r"[()]", " ", text)
#     text = re.sub(r"\s+", " ", text)

#     return text.strip()


# # ---------------- REMOVE PACK INFO ----------------
# def remove_pack(text: str) -> str:

#     patterns = [
#         r"\d+\s*'S",
#         r"\d+\s*TABS?",
#         r"\d+\s*CAPS?",
#         r"STRIP OF \d+",
#         r"BOTTLE OF \d+",
#         r"PACK OF \d+",
#     ]

#     for p in patterns:
#         text = re.sub(p, "", text)

#     return text.strip()


# # ---------------- STRENGTH ----------------
# STRENGTH_REGEX = re.compile(
#     r"\d+(?:\.\d+)?\s?(?:MG|MCG|G|GM|ML|IU|%)"
#     r"(?:\s?/\s?\d+(?:\.\d+)?\s?(?:ML|MG|MCG))?"
# )

# COMBO_STRENGTH_REGEX = re.compile(
#     r"\d+(?:\.\d+)?\s?(?:MG|MCG|G|GM|ML)"
#     r"(?:\s?\+\s?\d+(?:\.\d+)?\s?(?:MG|MCG|G|GM|ML))+"
# )


# def extract_strength(text: str):

#     combo = COMBO_STRENGTH_REGEX.search(text)
#     if combo:
#         return combo.group().replace(" ", "")

#     match = STRENGTH_REGEX.search(text)
#     if match:
#         return match.group().replace(" ", "")

#     return None


# # ---------------- STRENGTH NORMALIZATION ----------------
# def normalize_strength(strength: str):

#     if not strength:
#         return None

#     strength = strength.upper()

#     strength = strength.replace(" ", "")

#     strength = strength.replace("GM", "G")

#     return strength


# # ---------------- FORMS ----------------
# FORMS = sorted([
#     "CHEWABLE TABLET",
#     "DISPERSIBLE TABLET",
#     "EFFERVESCENT TABLET",
#     "SUBLINGUAL TABLET",
#     "BUCCAL TABLET",
#     "ORAL SUSPENSION",
#     "EYE DROPS",
#     "EAR DROPS",
#     "NASAL SPRAY",
#     "TABLET",
#     "CAPSULE",
#     "SYRUP",
#     "SUSPENSION",
#     "SOLUTION",
#     "INJECTION",
#     "CREAM",
#     "OINTMENT",
#     "GEL",
#     "LOTION",
#     "DROPS",
#     "INHALER",
#     "ROTACAP",
#     "RESPULE",
#     "SPRAY",
#     "PATCH",
#     "SACHET"
# ], key=len, reverse=True)


# def extract_form(text: str):

#     padded = f" {text} "

#     for form in FORMS:
#         if f" {form} " in padded:
#             return form

#     return None


# # ---------------- VARIANTS ----------------
# VARIANTS = {
#     "XR","SR","CR","ER","MR","IR","DR",
#     "XL","LA","OD",
#     "FORTE","PLUS","MAX","EXTRA",
#     "DUO","DSR","LS","LC","LB","CV",
#     "AM","PM","AT","H","CT","MT","TZ"
# }


# def extract_variant(text: str):

#     words = text.split()

#     found = []

#     for w in words:
#         if w in VARIANTS:
#             found.append(w)

#     if found:
#         return " ".join(found)

#     return None


# # ---------------- BRAND ----------------
# def extract_brand(text, strength, form, variant):

#     words = text.split()

#     stop_words = set()

#     if strength:
#         stop_words.add(strength)

#     if form:
#         stop_words.update(form.split())

#     if variant:
#         stop_words.update(variant.split())

#     brand = []

#     for w in words:

#         if w in stop_words:
#             break

#         if re.match(r"\d", w):
#             break

#         brand.append(w)

#     return " ".join(brand)


# # ---------------- CANONICAL NAME ----------------
# def build_canonical(brand, variant, strength, form):

#     parts = []

#     if brand:
#         parts.append(brand)

#     if variant:
#         parts.append(variant)

#     if strength:
#         parts.append(strength)

#     if form:
#         parts.append(form)

#     return " ".join(parts)


# # ---------------- MAIN PARSER ----------------
# def parse_medicine(raw_name: str):

#     name = normalize(raw_name)

#     name = remove_pack(name)

#     strength = extract_strength(name)
#     strength = normalize_strength(strength)

#     form = extract_form(name)

#     variant = extract_variant(name)

#     brand = extract_brand(name, strength, form, variant)

#     canonical = build_canonical(
#         brand,
#         variant,
#         strength,
#         form
#     )

#     return {
#         "brand": brand,
#         "variant": variant,
#         "strength": strength,
#         "form": form,
#         "canonicalName": canonical
#     }


import csv
from pathlib import Path
import re

# ---------------- NORMALIZE ----------------
TYPO_FIX = {
    "TAB": "TABLET",
    "TABS": "TABLET",
    "CAP": "CAPSULE",
    "CAPS": "CAPSULE",
    "OINMENT": "OINTMENT",
    "OINT": "OINTMENT",
    "SYP": "SYRUP",
    "INJ": "INJECTION"
}

def normalize(text: str):
    text = text.upper()
    text = text.replace("-", " ")
    text = text.replace("+", " + ")
    text = text.replace("/", " / ")
    text = re.sub(r"[()]", " ", text)
    text = re.sub(r"\s+", " ", text)
    for k, v in TYPO_FIX.items():
        text = re.sub(rf"\b{k}\b", v, text)
    return text.strip()


# ---------------- REMOVE PACK INFO ----------------
PACK_PATTERNS = [
    r"\d+\s*'S",
    r"\d+\s*TABS?",
    r"\d+\s*CAPS?",
    r"\d+\s*TABLETS?",
    r"\d+\s*CAPSULES?",
    r"STRIP OF \d+",
    r"BOTTLE OF \d+",
    r"PACK OF \d+",
    r"\d+\s*ML BOTTLE",
    r"\d+\s*ML\b",
    r"\d+ML\b",
    r"\d+X\d+(?:\.\d+)?ML?",      # Handles 8X1ML, 2X0.8ML etc. (production fix)
    r"\d+ X \d+ ML?"
]

def remove_pack(text: str):
    for p in PACK_PATTERNS:
        text = re.sub(p, "", text)
    return re.sub(r"\s+", " ", text).strip()


# ---------------- TOKENIZER ----------------
def tokenize(text: str):
    return text.split()


# ---------------- CLEAN DUPLICATE STRENGTHS ----------------
def clean_duplicate_strengths(tokens):
    seen_strengths = set()
    cleaned_tokens = []
    for t in tokens:
        normalized = t.replace(" ", "").upper()
        if re.fullmatch(r"\d+(?:\.\d+)?(MG|MCG|G|ML|IU|%|MIU|MU)", normalized):
            if normalized in seen_strengths:
                continue
            seen_strengths.add(normalized)
            cleaned_tokens.append(normalized)
        else:
            cleaned_tokens.append(t)
    return cleaned_tokens


# ---------------- STRENGTH ----------------
UNIT_REGEX = r"(MG|MCG|G|GM|ML|IU|%|MIU|MU)"
STRENGTH_REGEX = re.compile(rf"\b\d+(?:\.\d+)?\s?{UNIT_REGEX}\b")
COMBO_STRENGTH_REGEX = re.compile(
    rf"\b\d+(?:\.\d+)?\s?{UNIT_REGEX}"
    rf"(?:\s?[+/]\s?\d+(?:\.\d+)?\s?{UNIT_REGEX})+"
)
SLASH_COMBO_REGEX = re.compile(
    rf"\b\d+(?:\.\d+)?(?:\s*/\s*\d+(?:\.\d+)?)*\s?{UNIT_REGEX}\b"
)

def extract_strength(text: str):
    # Priority: full combo with + (each salt has unit)
    combo = COMBO_STRENGTH_REGEX.search(text)
    if combo:
        return combo.group().replace(" ", "")
    
    # Slash combo (common in Indian multi-salt: 2/500/15MG or 10/20MG)
    slash = SLASH_COMBO_REGEX.search(text)
    if slash:
        return slash.group().replace(" ", "")
    
    # Single strength
    match = STRENGTH_REGEX.search(text)
    if match:
        return match.group().replace(" ", "")
    return None


def extract_numeric_strength(tokens):
    for i, t in enumerate(tokens):
        if re.fullmatch(r"\d{1,4}", t):
            if i == 0:
                continue
            prev = tokens[i - 1] if i > 0 else ""
            if re.fullmatch(r"[A-Z]\d+", prev):
                continue
            if prev in VARIANTS:
                continue
            if int(t) > 2000:
                continue
            return t
    return None


# ---------------- FORMS ----------------
FORMS = sorted([
    "CHEWABLE TABLET", "DISPERSIBLE TABLET", "EFFERVESCENT TABLET",
    "SUBLINGUAL TABLET", "BUCCAL TABLET", "FILM COATED TABLET",
    "TABLET", "HARD CAPSULE", "SOFT GEL CAPSULE", "SOFTGEL CAPSULE",
    "CAPSULE", "ORAL SUSPENSION", "ORAL SOLUTION", "SUSPENSION", "SYRUP",
    "SOLUTION", "EYE DROPS", "EAR DROPS", "NASAL DROPS", "DROPS",
    "NASAL SPRAY", "MOUTH SPRAY", "SPRAY", "CREAM", "OINTMENT", "GEL",
    "LOTION", "FACE WASH", "MOUTH WASH", "SHAMPOO", "SOAP", "SCRUB",
    "POWDER", "INHALER", "ROTACAP", "RESPULE", "VAPOCAP",
    "PREFILLED SYRINGE", "INJECTION", "SACHET", "GRANULES",
    "PASTILLE", "LOZENGE", "KIT", "PATCH", "BALM", "LINIMENT",
    "LIQUID", "GUM", "OINT", "SPRINKLE"
], key=len, reverse=True)

def extract_form(text):
    padded = f" {text} "
    for form in FORMS:
        if f" {form} " in padded:
            return form
    return None


# ---------------- VARIANTS ----------------
VARIANTS = {
    "XR","SR","CR","ER","MR","IR","DR","PR","TR","BR",
    "XL","LA","OD","BD","TD","D","DX","N","DT","AL","M","TG",
    "MD","ODT","FX","HP","FORTE","PLUS","MAX","EXTRA","ULTRA","SUPER",
    "DUO","DSR","LS","LC","LB","CV","AV","AZ","H","CT","AT","MT","TZ",
    "SP","MF","PG","VG","GM","G","O","OZ","OF","A","C","K","Z","B","L","AM","PM","FT","IT",
    "FLEX","SRX","EX","DS","XT",
    "D3"          # Added for production: fixes Celol D3+, common vitamin notation
}

def extract_variant(tokens):
    found = [t for i, t in enumerate(tokens) if t in VARIANTS and i != 0]
    return " ".join(found) if found else None


# ---------------- BRAND ----------------
def extract_brand(tokens, strength, form, variant):
    stop = set()
    if strength:
        stop.add(strength)
    if variant:
        stop.update(variant.split())
    if form:
        stop.update(form.split())
    brand = []
    for t in tokens:
        if t in stop:
            break
        if re.fullmatch(r"\d+", t):
            break
        brand.append(t)
    return " ".join(brand) if brand else "UNKNOWN"


# ---------------- CANONICAL NAME ----------------
def build_canonical(brand, variant, strength, form):
    parts = []
    brand = (brand or "UNKNOWN").strip().upper()
    variant = (variant or "NORMAL").strip().upper()
    strength = (strength or "UNKNOWN").strip().upper()
    form = (form or "NORMAL").strip().upper()

    if brand:
        parts.append(brand)
    if variant not in {"NORMAL", "UNKNOWN"} and variant not in brand:
        parts.append(variant)
    if strength not in {"NORMAL", "UNKNOWN"} and strength not in brand:
        parts.append(strength)
    if form not in {"NORMAL", "UNKNOWN"}:
        parts.append(form)

    canonical = []
    seen = set()
    for p in parts:
        if p not in seen:
            canonical.append(p)
            seen.add(p)
    return " ".join(canonical)


# ---------------- MAIN PARSER ----------------
def parse_medicine(raw_name: str, debug=False):
    name = normalize(raw_name)
    name = remove_pack(name)
    tokens = tokenize(name)
    tokens = clean_duplicate_strengths(tokens)

    strength = extract_strength(name)
    if not strength:
        strength = extract_numeric_strength(tokens)
    if strength:
        strength = strength.replace("GM", "G")

    form = extract_form(name)
    variant = extract_variant(tokens)
    brand = extract_brand(tokens, strength, form, variant)

    if not variant:
        variant = "NORMAL"
    if not form:
        form = "NORMAL"
    if not strength:
        strength = "UNKNOWN"

    canonical = build_canonical(brand, variant, strength, form)

    result = {
        "brand": brand,
        "variant": variant,
        "strength": strength,
        "form": form,
        "canonicalName": canonical
    }

    if debug:
        print(f"CSV MEDICINE: {raw_name}")
        print(f"PARSED CANONICAL: {canonical}")
        print("-" * 60)

    return result


# ---------------- CSV DEBUG ----------------
def parse_csv_debug(csv_path):
    csv_path = Path(csv_path)
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}")
        return []

    parsed_list = []
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            raw_name = row.get('Medicine') or row.get('medicine') or row.get('Name') or row.get('name')
            if not raw_name:
                continue
            parsed = parse_medicine(raw_name, debug=True)
            parsed_list.append(parsed)
    return parsed_list


# ---------------- TEST ----------------
if __name__ == "__main__":
    csv_file = "medicines.csv"
    all_parsed = parse_csv_debug(csv_file)
    print(f"Total medicines parsed: {len(all_parsed)}")