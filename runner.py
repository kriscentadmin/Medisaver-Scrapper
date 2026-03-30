import json
import os
import re
import socket
import time
import asyncio
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlparse
from datetime import datetime
from zoneinfo import ZoneInfo
from playwright.async_api import Error, TimeoutError, async_playwright
from prisma import Prisma

from scrapers.base import human_delay, random_ua
from scrapers import netmed, onemg, pharmaeasy, truemeds

SITE_NAMES = ["PHARMEASY", "ONEMG", "NETMEDS", "TRUEMEDS"]
SITE_DELAY_RANGE = (5, 10)
MEDICINE_DELAY_RANGE = (10, 20)
FETCH_BATCH_SIZE = 10
MAX_MEDICINES_PER_RUN = 10
BASE_SITE_COOLDOWN_SECONDS = 1800
MAX_SITE_COOLDOWN_SECONDS = 4 * 60 * 60
DAILY_RUNTIME_SECONDS = 8 * 60 * 60
DAILY_SAVE_LIMIT_PER_SITE = 500
STATE_ROW_ID = 1

db = Prisma()
site_cooldowns: dict[str, float] = {}
site_backoff_levels: dict[str, int] = {}
_table_ready = False
DB_RETRY_ATTEMPTS = 3
DB_RETRY_DELAY_SECONDS = 2
DB_RECOVERY_WAIT_SECONDS = 15
DB_RECOVERY_MAX_WAIT_SECONDS = 60
NETWORK_PROBE_TIMEOUT_SECONDS = 5


def today_key() -> str:
    return date.today().isoformat()


def to_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


APP_TIMEZONE = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Kolkata"))


def format_display_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = to_iso_datetime(value)
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=APP_TIMEZONE)
    else:
        dt = dt.astimezone(APP_TIMEZONE)
    return dt.strftime("%d %b, %I:%M:%S %p").lower()


def pick_status_time(summary: Any, key: str, fallback: Any) -> Any:
    if isinstance(summary, dict):
        value = summary.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return fallback


def default_progress() -> dict[str, Any]:
    return {
        "last_index": 0,
        "day": today_key(),
        "elapsed_seconds_today": 0.0,
        "saved_today": {site_name: 0 for site_name in SITE_NAMES},
        "total_saved_today": 0,
    }


def is_prisma_connection_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = [
        "connecterror",
        "connection attempts failed",
        "connection refused",
        "connection reset",
        "server disconnected",
        "could not connect",
        "can't reach database server",
        "timed out",
        "pool is closed",
    ]
    return any(marker in text for marker in markers)


def get_database_target() -> tuple[str | None, int | None]:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        return (None, None)

    parsed = urlparse(database_url)
    host = parsed.hostname
    port = parsed.port

    if host and port:
        return (host, port)

    # Fallback for unusual URLs where urlparse cannot infer cleanly.
    host_match = re.search(r"@([^:/?#]+)", database_url)
    port_match = re.search(r":(\d+)", database_url.rsplit("@", 1)[-1])
    return (
        host or (host_match.group(1) if host_match else None),
        port or (int(port_match.group(1)) if port_match else None),
    )


async def probe_tcp(host: str, port: int, timeout: float = NETWORK_PROBE_TIMEOUT_SECONDS) -> bool:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def probe_dns(host: str) -> bool:
    try:
        loop = asyncio.get_running_loop()
        await loop.getaddrinfo(host, None)
        return True
    except socket.gaierror:
        return False
    except Exception:
        return False


async def diagnose_connectivity_problem(exc: Exception) -> str:
    db_host, db_port = get_database_target()
    if not db_host or not db_port:
        return "DATABASE_URL missing or invalid"

    internet_ok = await probe_tcp("1.1.1.1", 53)
    if not internet_ok:
        return f"internet/network issue: outbound connectivity unavailable while reaching {db_host}:{db_port}"

    dns_ok = await probe_dns(db_host)
    if not dns_ok:
        return f"dns issue: cannot resolve database host {db_host}"

    db_port_ok = await probe_tcp(db_host, db_port)
    if not db_port_ok:
        return f"database host reachable by DNS but port {db_port} is not reachable on {db_host}"

    return (
        f"database network path looks reachable for {db_host}:{db_port}; "
        f"likely Prisma/query-engine/database-side issue ({type(exc).__name__})"
    )


async def reconnect_db() -> None:
    global _table_ready
    try:
        if db.is_connected():
            await db.disconnect()
    except Exception:
        pass
    await db.connect()
    _table_ready = False


async def wait_for_db_recovery(action_name: str, exc: Exception) -> None:
    delay = DB_RECOVERY_WAIT_SECONDS
    diagnosis = await diagnose_connectivity_problem(exc)
    print(
        f"DB {action_name} paused: waiting for internet/database recovery after error: {exc}\n"
        f"DB diagnosis: {diagnosis}"
    )
    while True:
        try:
            await reconnect_db()
            print(f"DB {action_name} resumed after connection recovery")
            return
        except Exception as reconnect_exc:
            diagnosis = await diagnose_connectivity_problem(reconnect_exc)
            print(
                f"DB {action_name} still unavailable; retrying reconnect in {delay}s: "
                f"{reconnect_exc}\n"
                f"DB diagnosis: {diagnosis}"
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, DB_RECOVERY_MAX_WAIT_SECONDS)


async def execute_raw_with_retry(query: str, *args: Any) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            if not db.is_connected():
                await db.connect()
            return await db.execute_raw(query, *args)
        except Exception as exc:
            last_exc = exc
            if attempt >= DB_RETRY_ATTEMPTS or not is_prisma_connection_error(exc):
                if not is_prisma_connection_error(exc):
                    raise
                await wait_for_db_recovery("execute_raw", exc)
                return await execute_raw_with_retry(query, *args)
            print(f"DB execute_raw retry {attempt}/{DB_RETRY_ATTEMPTS} after error: {exc}")
            try:
                await reconnect_db()
            except Exception as reconnect_exc:
                print(f"DB execute_raw reconnect attempt failed: {reconnect_exc}")
            await asyncio.sleep(DB_RETRY_DELAY_SECONDS)
    raise last_exc if last_exc else RuntimeError("unknown execute_raw failure")


async def query_first_with_retry(query: str, *args: Any) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            if not db.is_connected():
                await db.connect()
            return await db.query_first(query, *args)
        except Exception as exc:
            last_exc = exc
            if attempt >= DB_RETRY_ATTEMPTS or not is_prisma_connection_error(exc):
                if not is_prisma_connection_error(exc):
                    raise
                await wait_for_db_recovery("query_first", exc)
                return await query_first_with_retry(query, *args)
            print(f"DB query_first retry {attempt}/{DB_RETRY_ATTEMPTS} after error: {exc}")
            try:
                await reconnect_db()
            except Exception as reconnect_exc:
                print(f"DB query_first reconnect attempt failed: {reconnect_exc}")
            await asyncio.sleep(DB_RETRY_DELAY_SECONDS)
    raise last_exc if last_exc else RuntimeError("unknown query_first failure")


async def upsert_product_with_retry(*, where: dict[str, Any], data: dict[str, Any]) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            if not db.is_connected():
                await db.connect()
            return await db.product.upsert(where=where, data=data)
        except Exception as exc:
            last_exc = exc
            if attempt >= DB_RETRY_ATTEMPTS or not is_prisma_connection_error(exc):
                if not is_prisma_connection_error(exc):
                    raise
                await wait_for_db_recovery("product_upsert", exc)
                return await upsert_product_with_retry(where=where, data=data)
            print(f"DB product upsert retry {attempt}/{DB_RETRY_ATTEMPTS} after error: {exc}")
            try:
                await reconnect_db()
            except Exception as reconnect_exc:
                print(f"DB product upsert reconnect attempt failed: {reconnect_exc}")
            await asyncio.sleep(DB_RETRY_DELAY_SECONDS)
    raise last_exc if last_exc else RuntimeError("unknown upsert failure")


async def find_many_medicines_with_retry(start_index: int, take: int = FETCH_BATCH_SIZE) -> list[Any]:
    last_exc: Exception | None = None
    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            if not db.is_connected():
                await db.connect()
            return await db.medicine.find_many(
                where={"approved": False},
                skip=start_index,
                take=take,
                order={"id": "asc"},
            )
        except Exception as exc:
            last_exc = exc
            if attempt >= DB_RETRY_ATTEMPTS or not is_prisma_connection_error(exc):
                if not is_prisma_connection_error(exc):
                    raise
                await wait_for_db_recovery("medicine_find_many", exc)
                return await find_many_medicines_with_retry(start_index, take)
            print(f"DB medicine.find_many retry {attempt}/{DB_RETRY_ATTEMPTS} after error: {exc}")
            try:
                await reconnect_db()
            except Exception as reconnect_exc:
                print(f"DB medicine.find_many reconnect attempt failed: {reconnect_exc}")
            await asyncio.sleep(DB_RETRY_DELAY_SECONDS)
    raise last_exc if last_exc else RuntimeError("unknown medicine.find_many failure")


async def ensure_runner_tables() -> None:
    global _table_ready
    if _table_ready:
        return

    if not db.is_connected():
        await db.connect()
    await execute_raw_with_retry(
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
    row = await query_first_with_retry(
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
    await execute_raw_with_retry(
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
    updated_at: str | None = None,
) -> None:
    await ensure_runner_tables()
    status_updated_at = updated_at or date_time_iso()
    normalized_summary = None
    if summary is not None:
        normalized_summary = dict(summary)
        normalized_summary["updatedAt"] = status_updated_at
    await execute_raw_with_retry(
        """
        UPDATE scraper_state
        SET running = $2,
            started_at = COALESCE($3::timestamptz, started_at),
            finished_at = $4::timestamptz,
            summary_json = $5::jsonb,
            error_text = $6,
            updated_at = $7::timestamptz
        WHERE id = $1
        """,
        STATE_ROW_ID,
        running,
        started_at,
        finished_at,
        json.dumps(normalized_summary) if normalized_summary is not None else None,
        error,
        status_updated_at,
    )


async def get_runner_status() -> dict[str, Any]:
    try:
        await ensure_runner_tables()
        row = await query_first_with_retry(
            """
            SELECT running, started_at, finished_at, updated_at, summary_json, error_text
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
            "updatedAt": None,
            "summary": None,
            "error": f"status_unavailable: {exc}",
        }
    if not row:
        return {
            "running": False,
            "startedAt": None,
            "finishedAt": None,
            "updatedAt": None,
            "summary": None,
            "error": None,
        }

    summary = row.get("summary_json")
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = None

    started_value = pick_status_time(summary, "startedAt", row.get("started_at"))
    finished_value = pick_status_time(summary, "finishedAt", row.get("finished_at"))
    updated_value = pick_status_time(summary, "updatedAt", row.get("updated_at"))

    return {
        "running": bool(row.get("running")),
        "startedAt": to_iso_datetime(started_value),
        "startedAtDisplay": format_display_datetime(started_value),
        "finishedAt": to_iso_datetime(finished_value),
        "finishedAtDisplay": format_display_datetime(finished_value),
        "updatedAt": to_iso_datetime(updated_value),
        "updatedAtDisplay": format_display_datetime(updated_value),
        "summary": summary,
        "error": row.get("error_text"),
    }


async def recover_interrupted_run() -> None:
    await ensure_runner_tables()
    row = await query_first_with_retry(
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
        await upsert_product_with_retry(
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


def log_scraper(message: str) -> None:
    print(f"[SCRAPER] {message}", flush=True)


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
    # print(f"  {site_name}: cooldown {seconds}s due to {reason}")
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
    # print(f"{site_name}: opened persistent session")
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
    # print(f"{site_name}: closed session")


async def ensure_site_session(browser, site_name: str, site_sessions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    session = await create_site_session(browser, site_name)
    return session


async def reset_site_session(browser, site_name: str, site_sessions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    await close_site_session(site_name, site_sessions.get(site_name))
    return await create_site_session(browser, site_name)


async def run_site_scraper(site_name: str, medicine: Any, browser: Any, site_sessions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if is_site_cooling_down(site_name):
        remaining = max(0, int(site_cooldowns[site_name] - time.time()))
        log_scraper(f"{site_name}: cooldown active ({remaining}s left), skipping")
        return []
    session = await ensure_site_session(browser, site_name, site_sessions)
    page = session["page"]
    try:
        log_scraper(f"{site_name}: searching for {medicine.canonicalName}")
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
        raise exc
    finally:
        await close_site_session(site_name, session)


async def scrape_medicine(medicine: Any, browser: Any, site_sessions: dict[str, dict[str, Any]], progress: dict[str, Any], warnings: list[dict[str, Any]]) -> None:
    log_scraper(
        f"Medicine #{int(progress['last_index']) + 1}: {medicine.canonicalName} "
        f"({medicine.brand} {medicine.strength} {medicine.form})"
    )
    for site_name in SITE_NAMES:
        if site_limit_reached(progress, site_name):
            log_scraper(f"{site_name}: daily limit reached ({DAILY_SAVE_LIMIT_PER_SITE}), skipping")
            continue
        try:
            result = await run_site_scraper(site_name, medicine, browser, site_sessions)
        except Exception as exc:
            log_scraper(f"{site_name}: failed -> {exc}")
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
            log_scraper(f"{site_name}: no products")
            await human_delay(*SITE_DELAY_RANGE)
            continue
        top_product = result[0]
        score = int(top_product.get("_score", 0))
        log_scraper(
            f"{site_name}: best score={score} | {describe_product_fields(top_product)} | "
            f"name={top_product.get('name')} | pack={top_product.get('pack')} | "
            f"price={top_product.get('price')} | originalPrice={top_product.get('originalPrice')} | "
            f"discount={top_product.get('discount')} | productUrl={top_product.get('productUrl')}"
        )
        if score < 90:
            log_scraper(f"{site_name}: below save threshold")
            await human_delay(*SITE_DELAY_RANGE)
            continue
        await save_products(medicine.id, result, progress)
        await save_progress(progress)
        reset_site_backoff(site_name)
        log_scraper(f"{site_name}: saved {'exact match' if score >= 100 else 'near match'}")
        await human_delay(*SITE_DELAY_RANGE)


async def fetch_medicines(start_index: int, remaining: int = MAX_MEDICINES_PER_RUN) -> list[Any]:
    batch_size = max(0, min(FETCH_BATCH_SIZE, remaining))
    if batch_size == 0:
        return []
    return await find_many_medicines_with_retry(start_index, batch_size)


async def run() -> dict[str, Any]:
    await ensure_runner_tables()
    progress = await ensure_today_progress(await load_progress())
    session_started_at = time.time()
    session_started_at_iso = date_time_iso()
    MAX_RUN_TIME = 8 * 60  # 8 minutes
    initial_last_index = int(progress["last_index"])
    initial_saved_today = {site_name: int(progress["saved_today"].get(site_name, 0)) for site_name in SITE_NAMES}
    warnings: list[dict[str, Any]] = []
    summary = {
        "status": "running",
        "runnerPid": os.getpid(),
        "startedAt": session_started_at_iso,
        "startedAtEpoch": int(session_started_at),
        "finishedAt": None,
        "finishedAtEpoch": None,
        "sessionRuntimeSeconds": 0,
        "dayRuntimeSeconds": 0,
        "lastIndexStart": initial_last_index,
        "lastIndex": initial_last_index,
        "medicinesProcessed": 0,
        "siteSavedThisRun": {site_name: 0 for site_name in SITE_NAMES},
        "warnings": warnings,
        "progress": await get_progress_snapshot(),
    }
    medicines_processed_this_run = 0
    await update_run_status(running=True, started_at=session_started_at_iso, finished_at=None, summary=summary, error=None)

    if runtime_limit_reached(progress, session_started_at):
        finished_at_iso = date_time_iso()
        summary["status"] = "daily_runtime_exhausted"
        summary["finishedAt"] = finished_at_iso
        summary["finishedAtEpoch"] = int(time.time())
        summary["progress"] = await get_progress_snapshot()
        await update_run_status(running=False, finished_at=finished_at_iso, summary=summary, error=None)
        return summary
    if all_site_limits_reached(progress):
        finished_at_iso = date_time_iso()
        summary["status"] = "daily_limits_reached"
        summary["finishedAt"] = finished_at_iso
        summary["finishedAtEpoch"] = int(time.time())
        summary["progress"] = await get_progress_snapshot()
        await update_run_status(running=False, finished_at=finished_at_iso, summary=summary, error=None)
        return summary

    if not db.is_connected():
        await db.connect()
    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--disable-features=Translate,BackForwardCache,AcceptCHFrame",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--mute-audio",
                ],
            )
            try:
                while True:
                    await asyncio.sleep(0) 
                    # if time.time() - run_start > MAX_RUN_TIME:
                    #  print("⏹ Cron limit reached, stopping run safely")
                    #  break

                    if (
                        runtime_limit_reached(progress, session_started_at)
                        or all_site_limits_reached(progress)
                        or medicines_processed_this_run >= MAX_MEDICINES_PER_RUN
                    ):
                        break
                    medicines = await fetch_medicines(
                        int(progress["last_index"]),
                        MAX_MEDICINES_PER_RUN - medicines_processed_this_run,
                    )
                    if not medicines:
                        progress["last_index"] = 0
                        await save_progress(progress)
                        break
                    for medicine in medicines:
                        # await asyncio.sleep(0)
                        # if time.time() - run_start > MAX_RUN_TIME:
                            # print("⏹ Cron limit reached inside batch, stopping safely")
                            # break
                        if (
                            runtime_limit_reached(progress, session_started_at)
                            or all_site_limits_reached(progress)
                            or medicines_processed_this_run >= MAX_MEDICINES_PER_RUN
                        ):
                            break
                        await scrape_medicine(medicine, browser, {}, progress, warnings)
                        medicines_processed_this_run += 1
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
                await browser.close()
    finally:
        progress["elapsed_seconds_today"] = current_runtime_today(progress, session_started_at)
        await save_progress(progress)

    finished_at_iso = date_time_iso()
    summary["status"] = "completed"
    summary["finishedAt"] = finished_at_iso
    summary["finishedAtEpoch"] = int(time.time())
    summary["sessionRuntimeSeconds"] = int(max(0, time.time() - session_started_at))
    summary["dayRuntimeSeconds"] = int(progress["elapsed_seconds_today"])
    summary["lastIndex"] = int(progress["last_index"])
    summary["siteSavedThisRun"] = {
        site_name: int(progress["saved_today"].get(site_name, 0)) - initial_saved_today[site_name]
        for site_name in SITE_NAMES
    }
    summary["progress"] = await get_progress_snapshot()
    await update_run_status(running=False, finished_at=finished_at_iso, summary=summary, error=None)
    return summary


BROWSER_TIMEZONE = ZoneInfo("Asia/Kolkata")

def date_time_iso() -> str:
    return datetime.now(APP_TIMEZONE).isoformat()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
