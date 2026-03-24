CREATE TABLE IF NOT EXISTS "scraper_state" (
    "id" INTEGER NOT NULL,
    "last_index" INTEGER NOT NULL DEFAULT 0,
    "day" TEXT NOT NULL,
    "elapsed_seconds_today" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "saved_today" JSONB NOT NULL DEFAULT '{}'::jsonb,
    "total_saved_today" INTEGER NOT NULL DEFAULT 0,
    "running" BOOLEAN NOT NULL DEFAULT false,
    "started_at" TIMESTAMPTZ,
    "finished_at" TIMESTAMPTZ,
    "summary_json" JSONB,
    "error_text" TEXT,
    "updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT "scraper_state_pkey" PRIMARY KEY ("id")
);
