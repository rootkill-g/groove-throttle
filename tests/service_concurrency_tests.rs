use load_reducer_poc::service::LoadReducerService;
use load_reducer_poc::config::Config;
use load_reducer_poc::ports::{RedisPort, MongoPort, CrawlerPort, BoxError};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

// Mock Redis with coordination to simulate race window
#[derive(Clone)]
struct CoordinatedRedis {
    store: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl CoordinatedRedis {
    fn new() -> Self {
        Self { store: Arc::new(Mutex::new(HashMap::new())), counter: Arc::new(AtomicUsize::new(0)), notify: Arc::new(Notify::new()) }
    }
}

#[async_trait]
impl RedisPort for CoordinatedRedis {
    async fn multi_hgetall(&self, keys: &[String]) -> Result<Vec<HashMap<String, String>>, BoxError> {
        let store = self.store.lock().unwrap();
        let mut res = Vec::new();
        for k in keys {
            res.push(store.get(k).cloned().unwrap_or_default());
        }
        Ok(res)
    }

    async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, BoxError> {
        // Increment counter; if we're the second caller, wait until notified (so first can write inflight)
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("[CoordinatedRedis] hgetall called for key={} nth={}", key, n);
        if n == 2 {
            // Check if inflight fields already present to avoid missed-notify hang
            {
                let store = self.store.lock().unwrap();
                if let Some(entry) = store.get(key) {
                    if entry.contains_key("last_crawler_send") || entry.contains_key("last_mongo_fetch") {
                        eprintln!("[CoordinatedRedis] inflight already present, not waiting");
                        let cloned = entry.clone();
                        return Ok(cloned);
                    }
                }
            }
            eprintln!("[CoordinatedRedis] second caller waiting for notify...");
            // wait until first calls set_inflight_fields
            self.notify.notified().await;
            eprintln!("[CoordinatedRedis] second caller resumed after notify");
        }
        let store = self.store.lock().unwrap();
        Ok(store.get(key).cloned().unwrap_or_default())
    }

    async fn write_cache_and_clear(&self, key: &str, data: &str, _cache_ttl: u64) -> Result<(), BoxError> {
        eprintln!("[CoordinatedRedis] write_cache_and_clear key={}", key);
        let mut store = self.store.lock().unwrap();
        let entry = store.entry(key.to_string()).or_default();
        entry.insert("data".to_string(), data.to_string());
        entry.remove("last_mongo_fetch");
        entry.remove("last_crawler_send");
        Ok(())
    }

    async fn set_inflight_fields(&self, key: &str, last_mongo: Option<u64>, last_crawler: Option<u64>, _inflight_ttl: u64) -> Result<(), BoxError> {
        eprintln!("[CoordinatedRedis] set_inflight_fields key={} last_mongo={:?} last_crawler={:?}", key, last_mongo, last_crawler);
        let mut store = self.store.lock().unwrap();
        let entry = store.entry(key.to_string()).or_default();
        if let Some(m) = last_mongo {
            entry.insert("last_mongo_fetch".to_string(), m.to_string());
        }
        if let Some(c) = last_crawler {
            entry.insert("last_crawler_send".to_string(), c.to_string());
        }
        // Notify any waiter (the second hgetall) that inflight fields are set
        eprintln!("[CoordinatedRedis] notifying waiters");
        self.notify.notify_waiters();
        Ok(())
    }
}

// Mock Mongo returns empty (missing)
#[derive(Clone)]
struct MockMongo;

#[async_trait]
impl MongoPort for MockMongo {
    async fn find_by_urls(&self, _urls: &[String]) -> Result<HashMap<String, String>, BoxError> {
        Ok(HashMap::new())
    }
}

// Mock Crawler will record sends
#[derive(Clone)]
struct MockCrawler { pub sent_count: Arc<Mutex<usize>> }

impl MockCrawler { fn new() -> Self { Self { sent_count: Arc::new(Mutex::new(0)) } } }

#[async_trait]
impl CrawlerPort for MockCrawler {
    async fn send_batch(&self, urls: &[String]) -> Result<(), BoxError> {
        let mut s = self.sent_count.lock().unwrap();
        *s += 1;
        // assert that urls length is >=1
        assert!(!urls.is_empty());
        Ok(())
    }
}

#[tokio::test]
async fn concurrency_inflight_suppression_single_crawler_call() {
    eprintln!("[TEST] starting concurrency_inflight_suppression_single_crawler_call");
    let redis = CoordinatedRedis::new();
    let mongo = MockMongo;
    let crawler = MockCrawler::new();

    let mut config = Config::from_env();
    config.crawler_prevent_ms = 0; // allow immediate sends

    let service = Arc::new(LoadReducerService::new(redis.clone(), mongo.clone(), crawler.clone(), config));

    let url = "https://example.com/concurrent".to_string();

    // Launch two concurrent calls sharing the same service via Arc
    let s1 = service.clone();
    let s2 = service.clone();
    let u1 = url.clone();
    let u2 = url.clone();

    eprintln!("[TEST] spawning tasks");
    let h1 = tokio::spawn(async move { s1.process(vec![u1]).await.unwrap() });
    let h2 = tokio::spawn(async move { s2.process(vec![u2]).await.unwrap() });

    eprintln!("[TEST] awaiting tasks");
    let _r1 = h1.await.unwrap();
    let _r2 = h2.await.unwrap();

    eprintln!("[TEST] tasks completed, checking crawler calls");
    // Ensure crawler was called exactly once
    let sent = crawler.sent_count.lock().unwrap();
    assert_eq!(*sent, 1, "Expected exactly one crawler batch send, got {}", *sent);
}
