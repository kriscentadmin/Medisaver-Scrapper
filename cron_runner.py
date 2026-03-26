import asyncio
import time
import runner

MAX_RUN_TIME = 8 * 60  # 8 minutes


async def cron_run():
    print("🚀 Cron scraper started")

    start_time = time.time()

    try:
        # Start runner
        task = asyncio.create_task(runner.run())

        while True:
            await asyncio.sleep(5)

            elapsed = time.time() - start_time

            if elapsed > MAX_RUN_TIME:
                print("⏹ Cron limit reached, stopping safely...")
                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    print("✅ Runner cancelled safely")

                break

    except Exception as e:
        print("❌ CRON CRITICAL ERROR:", str(e))

        # 🔥 FAIL-SAFE (IMPORTANT)
        try:
            await runner.update_run_status(
                running=False,
                finished_at=runner.date_time_iso(),
                summary=None,
                error=str(e)
            )
        except Exception as inner:
            print("❌ Failed to update DB status:", inner)

    print("🏁 Cron scraper finished")


if __name__ == "__main__":
    asyncio.run(cron_run())

