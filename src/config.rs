use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub cache_ttl_sec: u64,
    pub mongo_prevent_ms: u64,
    pub crawler_prevent_ms: u64,
    pub inflight_ttl_sec: u64,
}

impl Config {
    pub fn from_env() -> Self {
        let cache_ttl_sec = env::var("CACHE_TTL_SEC").ok().and_then(|v| v.parse().ok()).unwrap_or(3600);
        let mongo_prevent_ms = env::var("MONGO_PREVENT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
        let crawler_prevent_ms = env::var("CRAWLER_PREVENT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(900_000);
        let inflight_ttl_sec = env::var("INFLIGHT_TTL_SEC").ok().and_then(|v| v.parse().ok()).unwrap_or(86_400);
        Self { cache_ttl_sec, mongo_prevent_ms, crawler_prevent_ms, inflight_ttl_sec }
    }
}

