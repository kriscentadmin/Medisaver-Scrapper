import json
import re
import time
from datetime import UTC, date, datetime
from typing import Any

from playwright.async_api import Error, TimeoutError, async_playwright
from prisma import Prisma

from scrapers.base import human_delay, random_ua
from scrapers import netmed, onemg, pharmaeasy, truemeds

SITE_NAMES = ["PHARMEASY", "ONEMG", "NETMEDS", "TRUEMEDS"]
SITE_DELAY_RANGE = (4, 8)
MEDICINE_DELAY_RANGE = (8, 16)
FETCH_BATCH_SIZE = 25
BASE_SITE_COOLDOWN_SECONDS = 1800
MAX_SITE_COOLDOWN_SECONDS = 4 * 60 * 60
DAILY_RUNTIME_SECONDS = 8 * 60 * 60
DAILY_SAVE_LIMIT_PER_SITE = 500
STATE_ROW_ID = 1

db = Prisma()
site_cooldowns: dict[str, float] = {}
site_backoff_levels: dict[str, int] = {}
_table_ready = False


def today_key() -> str:
    return date.today().isoformat()


def to_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def default_progress() -> dict[str, Any]:
    return {
        "last_index": 0,
        "day": today_key(),
        "elapsed_seconds_today": 0.0,
        "saved_today": {site_name: 0 for site_name in SITE_NAMES},
        "total_saved_today": 0,
    }


async def ensure_runner_tables() -> None:
    global _table_ready
    if _table_ready:
        return

    if not db.is_connected():
        await db.connect()
    await db.execute_raw(
        """
        INSERT INTO scraper_state (id, day, saved_today, updated_at)
        VALUES ($1, $2, $3::jsonb, NOW())
        ON CONFLICT (id) DO NOTHING
        """,
        STATE_ROW_ID,
        today_key(),
        json.dumps({site_name: 0 for site_name in SITE_NAMES}),
    )
    _table_ready = True


async def load_progress() -> dict[str, Any]:
    await ensure_runner_tables()
    row = await db.query_first(
        """
        SELECT last_index, day, elapsed_seconds_today, saved_today, total_saved_today
        FROM scraper_state
        WHERE id = $1
        """,
        STATE_ROW_ID,
    )
    if not row:
        return default_progress()

    progress = default_progress()
    progress["last_index"] = int(row.get("last_index") or 0)
    progress["day"] = row.get("day") or today_key()
    progress["elapsed_seconds_today"] = float(row.get("elapsed_seconds_today") or 0.0)
    saved_today = row.get("saved_today") or {}
    if isinstance(saved_today, str):
        try:
            saved_today = json.loads(saved_today)
        except Exception:
            saved_today = {}
    progress["saved_today"] = {
        site_name: int(saved_today.get(site_name, 0))
        for site_name in SITE_NAMES
    }
    progress["total_saved_today"] = int(row.get("total_saved_today") or 0)
    return progress


async def save_progress(progress: dict[str, Any]) -> None:
    await ensure_runner_tables()
    await db.execute_raw(
        """
        UPDATE scraper_state
        SET last_index = $2,
            day = $3,
            elapsed_seconds_today = $4,
            saved_today = $5::jsonb,
            total_saved_today = $6,
            updated_at = NOW()
        WHERE id = $1
        """,
        STATE_ROW_ID,
        int(progress["last_index"]),
        str(progress["day"]),
        float(progress["elapsed_seconds_today"]),
        json.dumps(progress["saved_today"]),
        int(progress["total_saved_today"]),
    )


async def update_run_status(
    *,
    running: bool,
    started_at: str | None = None,
    finished_at: str | None = None,
    summary: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    await ensure_runner_tables()
    await db.execute_raw(
        """
        UPDATE scraper_state
        SET running = $2,
            started_at = COALESCE($3::timestamptz, started_at),
            finished_at = $4::timestamptz,
            summary_json = $5::jsonb,
            error_text = $6,
            updated_at = NOW()
        WHERE id = $1
        """,
        STATE_ROW_ID,
        running,
        started_at,
        finished_at,
        json.dumps(summary) if summary is not None else None,
        error,
    )


async def get_runner_status() -> dict[str, Any]:
    try:
        await ensure_runner_tables()
        row = await db.query_first(
            """
            SELECT running, started_at, finished_at, summary_json, error_text
            FROM scraper_state
            WHERE id = $1
            """,
            STATE_ROW_ID,
        )
    except Exception as exc:
        return {
            "running": False,
            "startedAt": None,
            "finishedAt": None,
            "summary": None,
            "error": f"status_unavailable: {exc}",
        }
    if not row:
        return {
            "running": False,
            "startedAt": None,
            "finishedAt": None,
            "summary": None,
            "error": None,
        }

    summary = row.get("summary_json")
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = None

    return {
        "running": bool(row.get("running")),
        "startedAt": to_iso_datetime(row.get("started_at")),
        "finishedAt": to_iso_datetime(row.get("finished_at")),
        "summary": summary,
        "error": row.get("error_text"),
    }


async def recover_interrupted_run() -> None:
    await ensure_runner_tables()
    row = await db.query_first(
        """
        SELECT running
        FROM scraper_state
        WHERE id = $1
        """,
        STATE_ROW_ID,
    )
    if not row or not row.get("running"):
        return

    await update_run_status(
        running=False,
        finished_at=date_time_iso(),
        summary=None,
        error="scraper process stopped unexpectedly and was reset on startup",
    )


async def ensure_today_progress(progress: dict[str, Any]) -> dict[str, Any]:
    if progress.get("day") == today_key():
        return progress
    progress["day"] = today_key()
    progress["elapsed_seconds_today"] = 0.0
    progress["saved_today"] = {site_name: 0 for site_name in SITE_NAMES}
    progress["total_saved_today"] = 0
    await save_progress(progress)
    return progress


def site_limit_reached(progress: dict[str, Any], site_name: str) -> bool:
    return int(progress["saved_today"].get(site_name, 0)) >= DAILY_SAVE_LIMIT_PER_SITE


def all_site_limits_reached(progress: dict[str, Any]) -> bool:
    return all(site_limit_reached(progress, site_name) for site_name in SITE_NAMES)


def current_runtime_today(progress: dict[str, Any], session_started_at: float) -> float:
    return float(progress["elapsed_seconds_today"]) + max(0.0, time.time() - session_started_at)


def runtime_limit_reached(progress: dict[str, Any], session_started_at: float) -> bool:
    return current_runtime_today(progress, session_started_at) >= DAILY_RUNTIME_SECONDS


def print_daily_status(progress: dict[str, Any], session_started_at: float) -> None:
    runtime_hours = current_runtime_today(progress, session_started_at) / 3600
    print(
        "Daily status: "
        f"day={progress['day']} | "
        f"runtime_today={runtime_hours:.2f}h/{DAILY_RUNTIME_SECONDS / 3600:.0f}h | "
        f"saved_today={progress['saved_today']} | "
        f"total_saved_today={progress['total_saved_today']} | "
        f"last_index={progress['last_index']}"
    )


async def get_progress_snapshot() -> dict[str, Any]:
    try:
        progress = await ensure_today_progress(await load_progress())
    except Exception as exc:
        return {
            "day": today_key(),
            "lastIndex": 0,
            "elapsedSecondsToday": 0.0,
            "savedToday": {site_name: 0 for site_name in SITE_NAMES},
            "totalSavedToday": 0,
            "cooldowns": {
                site_name: max(0, int(site_cooldowns.get(site_name, 0) - time.time()))
                for site_name in SITE_NAMES
                if site_cooldowns.get(site_name, 0) > time.time()
            },
            "error": f"progress_unavailable: {exc}",
        }
    cooldowns = {
        site_name: max(0, int(site_cooldowns.get(site_name, 0) - time.time()))
        for site_name in SITE_NAMES
        if site_cooldowns.get(site_name, 0) > time.time()
    }
    return {
        "day": progress["day"],
        "lastIndex": progress["last_index"],
        "elapsedSecondsToday": progress["elapsed_seconds_today"],
        "savedToday": progress["saved_today"],
        "totalSavedToday": progress["total_saved_today"],
        "cooldowns": cooldowns,
    }


def normalize_numeric_string(value: Any) -> str | None:
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


async def save_products(medicine_id: int, products: list[dict[str, Any]], progress: dict[str, Any]) -> None:
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
            where={"medicineId_source": {"medicineId": medicine_id, "source": payload["source"]}},
            data={
                "create": {"medicineId": medicine_id, **payload},
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
        source = payload["source"]
        progress["saved_today"][source] = int(progress["saved_today"].get(source, 0)) + 1
        progress["total_saved_today"] = int(progress["total_saved_today"]) + 1


def describe_product_fields(product: dict[str, Any]) -> str:
    fields = ["price", "originalPrice", "discount", "productUrl", "endpoint"]
    missing = [field for field in fields if not product.get(field)]
    return "complete" if not missing else f"missing={','.join(missing)}"


def is_site_cooling_down(site_name: str) -> bool:
    return site_cooldowns.get(site_name, 0) > time.time()


def reset_site_backoff(site_name: str) -> None:
    site_backoff_levels[site_name] = 0
    site_cooldowns.pop(site_name, None)


def mark_site_cooldown(site_name: str, reason: str) -> int:
    level = site_backoff_levels.get(site_name, 0) + 1
    site_backoff_levels[site_name] = level
    seconds = min(BASE_SITE_COOLDOWN_SECONDS * (2 ** (level - 1)), MAX_SITE_COOLDOWN_SECONDS)
    site_cooldowns[site_name] = time.time() + seconds
    print(f"  {site_name}: cooldown {seconds}s due to {reason}")
    return seconds


def looks_like_block(error: Exception | str) -> bool:
    text = str(error).lower()
    patterns = [
        "captcha", "verify you are human", "robot", "bot detection",
        "security check", "access denied", "forbidden", "temporarily blocked",
        "too many requests", "unusual traffic", "rate limit", "intercepts pointer events",
        "challenge", "cf-chl",
    ]
    return any(pattern in text for pattern in patterns)


async def detect_page_block(page) -> str | None:
    try:
        url = (page.url or "").lower()
        title = (await page.title()).lower()
        body_text = (await page.locator("body").inner_text(timeout=3000)).lower()[:4000]
    except Exception:
        return None
    combined = " ".join([url, title, body_text])
    indicators = [
        "captcha", "verify you are human", "security check", "access denied",
        "forbidden", "temporarily blocked", "too many requests", "unusual traffic",
        "press and hold", "are you a robot", "cf-chl", "/captcha", "/challenge",
    ]
    for indicator in indicators:
        if indicator in combined:
            return indicator
    return None


async def create_site_session(browser, site_name: str) -> dict[str, Any]:
    context = await browser.new_context(
        user_agent=random_ua(),
        locale="en-IN",
        timezone_id="Asia/Kolkata",
        viewport={"width": 1366, "height": 768},
    )
    page = await context.new_page()
    print(f"{site_name}: opened persistent session")
    return {"context": context, "page": page}


async def close_site_session(site_name: str, session: dict[str, Any] | None) -> None:
    if not session:
        return
    try:
        await session["page"].close()
    except Exception:
        pass
    try:
        await session["context"].close()
    except Exception:
        pass
    print(f"{site_name}: closed session")


async def ensure_site_session(browser, site_name: str, site_sessions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    session = site_sessions.get(site_name)
    if session:
        return session
    session = await create_site_session(browser, site_name)
    site_sessions[site_name] = session
    return session


async def reset_site_session(browser, site_name: str, site_sessions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    await close_site_session(site_name, site_sessions.get(site_name))
    session = await create_site_session(browser, site_name)
    site_sessions[site_name] = session
    return session


async def run_site_scraper(site_name: str, medicine: Any, browser: Any, site_sessions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if is_site_cooling_down(site_name):
        remaining = max(0, int(site_cooldowns[site_name] - time.time()))
        print(f"  {site_name}: cooldown active ({remaining}s left), skipping")
        return []
    session = await ensure_site_session(browser, site_name, site_sessions)
    page = session["page"]
    try:
        if site_name == "NETMEDS":
            results = await netmed.scrape_netmeds(medicine, page)
        elif site_name == "TRUEMEDS":
            results = await truemeds.scrape_truemeds(medicine, page)
        elif site_name == "ONEMG":
            results = await onemg.scrape_1mg(medicine, page)
        elif site_name == "PHARMEASY":
            results = await pharmaeasy.scrape_pharmeasy(medicine, page)
        else:
            results = []
        block_reason = await detect_page_block(page)
        if block_reason:
            raise RuntimeError(f"captcha/block page detected: {block_reason}")
        return results
    except (TimeoutError, Error, RuntimeError, Exception) as exc:
        await reset_site_session(browser, site_name, site_sessions)
        raise exc


async def scrape_medicine(medicine: Any, browser: Any, site_sessions: dict[str, dict[str, Any]], progress: dict[str, Any], warnings: list[dict[str, Any]]) -> None:
    print(f"\nScraping: {medicine.canonicalName}")
    for site_name in SITE_NAMES:
        if site_limit_reached(progress, site_name):
            print(f"  {site_name}: daily limit reached ({DAILY_SAVE_LIMIT_PER_SITE}), skipping")
            continue
        try:
            result = await run_site_scraper(site_name, medicine, browser, site_sessions)
        except Exception as exc:
            print(f"  {site_name}: failed -> {exc}")
            warnings.append({
                "site": site_name,
                "medicine": medicine.canonicalName,
                "message": str(exc),
                "blocked": looks_like_block(exc),
                "timestamp": int(time.time()),
            })
            if looks_like_block(exc):
                mark_site_cooldown(site_name, str(exc))
            await human_delay(*SITE_DELAY_RANGE)
            continue
        if not result:
            print(f"  {site_name}: no products")
            await human_delay(*SITE_DELAY_RANGE)
            continue
        top_product = result[0]
        score = int(top_product.get("_score", 0))
        print(
            f"  {site_name}: best score={score} | {describe_product_fields(top_product)} | "
            f"name={top_product.get('name')} | pack={top_product.get('pack')} | "
            f"price={top_product.get('price')} | originalPrice={top_product.get('originalPrice')} | "
            f"discount={top_product.get('discount')} | productUrl={top_product.get('productUrl')} | "
            f"endpoint={top_product.get('endpoint')}"
        )
        if score < 90:
            print(f"  {site_name}: below save threshold")
            await human_delay(*SITE_DELAY_RANGE)
            continue
        await save_products(medicine.id, result, progress)
        await save_progress(progress)
        reset_site_backoff(site_name)
        print(f"  {site_name}: saved {'exact match' if score >= 100 else 'near match'}")
        await human_delay(*SITE_DELAY_RANGE)


async def fetch_medicines(start_index: int) -> list[Any]:
    return await db.medicine.find_many(where={"approved": False}, skip=start_index, take=FETCH_BATCH_SIZE, order={"id": "asc"})


async def run() -> dict[str, Any]:
    await ensure_runner_tables()
    progress = await ensure_today_progress(await load_progress())
    session_started_at = time.time()
    initial_last_index = int(progress["last_index"])
    initial_saved_today = {site_name: int(progress["saved_today"].get(site_name, 0)) for site_name in SITE_NAMES}
    warnings: list[dict[str, Any]] = []
    summary = {
        "status": "running",
        "startedAt": int(session_started_at),
        "finishedAt": None,
        "sessionRuntimeSeconds": 0,
        "dayRuntimeSeconds": 0,
        "lastIndexStart": initial_last_index,
        "lastIndex": initial_last_index,
        "medicinesProcessed": 0,
        "siteSavedThisRun": {site_name: 0 for site_name in SITE_NAMES},
        "warnings": warnings,
        "progress": await get_progress_snapshot(),
    }
    await update_run_status(running=True, started_at=date_time_iso(), finished_at=None, summary=summary, error=None)

    if runtime_limit_reached(progress, session_started_at):
        summary["status"] = "daily_runtime_exhausted"
        summary["finishedAt"] = int(time.time())
        summary["progress"] = await get_progress_snapshot()
        await update_run_status(running=False, finished_at=date_time_iso(), summary=summary, error=None)
        return summary
    if all_site_limits_reached(progress):
        summary["status"] = "daily_limits_reached"
        summary["finishedAt"] = int(time.time())
        summary["progress"] = await get_progress_snapshot()
        await update_run_status(running=False, finished_at=date_time_iso(), summary=summary, error=None)
        return summary

    if not db.is_connected():
        await db.connect()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
            )
            site_sessions: dict[str, dict[str, Any]] = {}
            try:
                while True:
                    if runtime_limit_reached(progress, session_started_at) or all_site_limits_reached(progress):
                        break
                    medicines = await fetch_medicines(int(progress["last_index"]))
                    if not medicines:
                        progress["last_index"] = 0
                        await save_progress(progress)
                        break
                    for medicine in medicines:
                        if runtime_limit_reached(progress, session_started_at) or all_site_limits_reached(progress):
                            break
                        await scrape_medicine(medicine, browser, site_sessions, progress, warnings)
                        progress["last_index"] = int(progress["last_index"]) + 1
                        summary["medicinesProcessed"] = int(summary["medicinesProcessed"]) + 1
                        summary["lastIndex"] = int(progress["last_index"])
                        summary["sessionRuntimeSeconds"] = int(max(0, time.time() - session_started_at))
                        summary["dayRuntimeSeconds"] = int(current_runtime_today(progress, session_started_at))
                        summary["siteSavedThisRun"] = {
                            site_name: int(progress["saved_today"].get(site_name, 0)) - initial_saved_today[site_name]
                            for site_name in SITE_NAMES
                        }
                        summary["progress"] = await get_progress_snapshot()
                        await save_progress(progress)
                        await update_run_status(
                            running=True,
                            summary=summary,
                            error=None,
                        )
                        await human_delay(*MEDICINE_DELAY_RANGE)
            finally:
                for site_name in SITE_NAMES:
                    await close_site_session(site_name, site_sessions.get(site_name))
                await browser.close()
    finally:
        progress["elapsed_seconds_today"] = current_runtime_today(progress, session_started_at)
        await save_progress(progress)

    summary["status"] = "completed"
    summary["finishedAt"] = int(time.time())
    summary["sessionRuntimeSeconds"] = int(max(0, time.time() - session_started_at))
    summary["dayRuntimeSeconds"] = int(progress["elapsed_seconds_today"])
    summary["lastIndex"] = int(progress["last_index"])
    summary["siteSavedThisRun"] = {
        site_name: int(progress["saved_today"].get(site_name, 0)) - initial_saved_today[site_name]
        for site_name in SITE_NAMES
    }
    summary["progress"] = await get_progress_snapshot()
    await update_run_status(running=False, finished_at=date_time_iso(), summary=summary, error=None)
    return summary


def date_time_iso() -> str:
    return datetime.now(UTC).isoformat()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
