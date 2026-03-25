import re

from rapidfuzz import fuzz

DEBUG_MATCHING = True
AUTO_ACCEPT_SCORE = 100
NEAR_MATCH_SCORE = 90


def extract_numeric_strength(s: str) -> float | None:
    if not s or s.upper() == "UNKNOWN":
        return None
    match = re.search(r"(\d*\.?\d+)", s)
    if not match:
        return None
    try:
        val = float(match.group(1))
        unit_match = re.search(r"([a-zA-Zμ]+)", s.lower())
        if unit_match:
            unit = unit_match.group(1)
            if unit in ["mcg", "microgram", "μg"]:
                val /= 1000
            elif unit in ["g", "gm"]:
                val *= 1000
        return val
    except Exception:
        return None


def score_parsed(med: dict, prod: dict) -> int:
    score = 0
    debug_lines = []

    if DEBUG_MATCHING:
        debug_lines.append("DEBUG MATCHING")
        debug_lines.append(f"Ref (DB):  {med}")
        debug_lines.append(f"Scraped:   {prod}")

    brand_ref = (med.get("brand") or "").strip().upper()
    brand_prod = (prod.get("brand") or "").strip().upper()

    if brand_ref != brand_prod:
        if DEBUG_MATCHING:
            debug_lines.append(f"Brand REJECTED: {brand_ref} != {brand_prod}")
            print("\n".join(debug_lines))
        return -100

    score += 40
    debug_lines.append(f"Brand MATCH (+40) -> {brand_ref}")

    m_str = (med.get("strength") or "UNKNOWN").strip().upper()
    p_str = (prod.get("strength") or "UNKNOWN").strip().upper()

    if m_str != p_str:
        debug_lines.append(f"Strength REJECTED: {m_str} != {p_str}")
        if DEBUG_MATCHING:
            print("\n".join(debug_lines))
        return -100

    score += 30
    debug_lines.append(f"Strength EXACT MATCH (+30) -> {m_str}")

    m_var = (med.get("variant") or "NORMAL").strip().upper()
    p_var = (prod.get("variant") or "NORMAL").strip().upper()
    if m_var == p_var:
        score += 20
        debug_lines.append(f"Variant EXACT MATCH (+20) -> {m_var}")
    else:
        debug_lines.append(f"Variant DIFFERENT: {m_var} != {p_var}")

    m_form = (med.get("form") or "NORMAL").strip().upper()
    p_form = (prod.get("form") or "NORMAL").strip().upper()
    if m_form == p_form:
        score += 10
        debug_lines.append(f"Form EXACT MATCH (+10) -> {m_form}")
    else:
        debug_lines.append(f"Form DIFFERENT: {m_form} != {p_form}")

    m_can = (med.get("canonicalName") or "").strip().upper()
    p_can = (prod.get("canonicalName") or "").strip().upper()
    if m_can == p_can:
        score += 30
        debug_lines.append("Canonical EXACT MATCH (+30)")
    else:
        fuzzy = fuzz.token_sort_ratio(m_can, p_can)
        if fuzzy >= 95:
            score += 15
            debug_lines.append(f"Canonical VERY CLOSE (+15, {fuzzy}%)")
        else:
            debug_lines.append(f"Canonical mismatch -> no bonus ({fuzzy}%)")

    score = min(100, max(0, score))

    if DEBUG_MATCHING:
        if score >= AUTO_ACCEPT_SCORE:
            verdict = "AUTO ACCEPT"
        elif score >= NEAR_MATCH_SCORE:
            verdict = "NEAR MATCH"
        else:
            verdict = "REJECT"
        debug_lines.append(f"FINAL SCORE: {score}   {verdict}")
        print("\n".join(debug_lines))

    return score
