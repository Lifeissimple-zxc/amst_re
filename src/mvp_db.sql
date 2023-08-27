CREATE TABLE IF NOT EXISTS seen_ads (
    ad_url TEXT PRIMARY KEY,
    search_url TEXT NOT NULL,
    seen_on INT NOT NULL,
    run_uuid TEXT
);
