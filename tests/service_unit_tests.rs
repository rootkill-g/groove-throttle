use load_reducer_poc::service::LoadReducerService;
use load_reducer_poc::config::Config;
use load_reducer_poc::ports::{RedisPort, MongoPort, CrawlerPort, BoxError};
use load_reducer_poc::domain::UrlData;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Mock Redis adapter
#[derive(Clone)]
struct MockRedis {
    // key -> hash map
    store: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
}

impl MockRedis {
    fn new() -> Self {
        Self { store: Arc::new(Mutex::new(HashMap::new())) }
    }
}

#[async_trait]
impl RedisPort for MockRedis {
    async fn multi_hgetall(&self, keys: &[String]) -> Result<Vec<HashMap<String, String>>, BoxError> {
        let store = self.store.lock().unwrap();
        let mut res = Vec::new();
        for k in keys {
            res.push(store.get(k).cloned().unwrap_or_default());
        }
        Ok(res)
    }

    async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, BoxError> {
        let store = self.store.lock().unwrap();
        Ok(store.get(key).cloned().unwrap_or_default())
    }

    async fn write_cache_and_clear(&self, key: &str, data: &str, _cache_ttl: u64) -> Result<(), BoxError> {
        let mut store = self.store.lock().unwrap();
        let entry = store.entry(key.to_string()).or_default();
        entry.insert("data".to_string(), data.to_string());
        entry.remove("last_mongo_fetch");
        entry.remove("last_crawler_send");
        Ok(())
    }

    async fn set_inflight_fields(&self, key: &str, last_mongo: Option<u64>, last_crawler: Option<u64>, _inflight_ttl: u64) -> Result<(), BoxError> {
        let mut store = self.store.lock().unwrap();
        let entry = store.entry(key.to_string()).or_default();
        if let Some(m) = last_mongo {
            entry.insert("last_mongo_fetch".to_string(), m.to_string());
        }
        if let Some(c) = last_crawler {
            entry.insert("last_crawler_send".to_string(), c.to_string());
        }
        Ok(())
    }
}

// Mock Mongo adapter
#[derive(Clone)]
struct MockMongo {
    data: Arc<Mutex<HashMap<String, String>>>,
}

impl MockMongo {
    fn new() -> Self { Self { data: Arc::new(Mutex::new(HashMap::new())) } }
}

#[async_trait]
impl MongoPort for MockMongo {
    async fn find_by_urls(&self, urls: &[String]) -> Result<HashMap<String, String>, BoxError> {
        let data = self.data.lock().unwrap();
        let mut res = HashMap::new();
        for u in urls {
            if let Some(d) = data.get(u) { res.insert(u.clone(), d.clone()); }
        }
        Ok(res)
    }
}

// Mock Crawler adapter
#[derive(Clone)]
struct MockCrawler {
    sent: Arc<Mutex<Vec<Vec<String>>>>,
}

impl MockCrawler {
    fn new() -> Self { Self { sent: Arc::new(Mutex::new(Vec::new())) } }
}

#[async_trait]
impl CrawlerPort for MockCrawler {
    async fn send_batch(&self, urls: &[String]) -> Result<(), BoxError> {
        let mut s = self.sent.lock().unwrap();
        s.push(urls.to_vec());
        Ok(())
    }
}

#[tokio::test]
async fn test_cache_hit_no_mongo_no_crawler() {
    let redis = MockRedis::new();
    let mongo = MockMongo::new();
    let crawler = MockCrawler::new();

    // Pre-populate redis with data
    {
        let mut store = redis.store.lock().unwrap();
        let mut hash = HashMap::new();
        hash.insert("data".to_string(), "cached-value".to_string());
        store.insert("rcs::https://example.com/a".to_string(), hash);
    }

    let config = Config::from_env();
    let service = LoadReducerService::new(redis.clone(), mongo.clone(), crawler.clone(), config);

    let res = service.process(vec!["https://example.com/a".to_string()]).await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].data, "cached-value");

    // ensure mongo and crawler untouched
    let mongo_data = mongo.data.lock().unwrap();
    assert!(mongo_data.is_empty());
    let sent = crawler.sent.lock().unwrap();
    assert!(sent.is_empty());
}

#[tokio::test]
async fn test_mongo_fetch_and_cache_write() {
    let redis = MockRedis::new();
    let mongo = MockMongo::new();
    let crawler = MockCrawler::new();

    // Populate mongo
    {
        let mut data = mongo.data.lock().unwrap();
        data.insert("https://example.com/b".to_string(), "mongo-value".to_string());
    }

    let config = Config::from_env();
    let service = LoadReducerService::new(redis.clone(), mongo.clone(), crawler.clone(), config);

    let res = service.process(vec!["https://example.com/b".to_string()]).await.unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].data, "mongo-value");

    // Redis should now have cached value
    let store = redis.store.lock().unwrap();
    let hash = store.get("rcs::https://example.com/b").unwrap();
    assert_eq!(hash.get("data").unwrap(), "mongo-value");
}

#[tokio::test]
async fn test_missing_triggers_crawler_and_inflight_update() {
    let redis = MockRedis::new();
    let mongo = MockMongo::new();
    let crawler = MockCrawler::new();

    let config = Config::from_env();
    let mut cfg = config.clone();
    // make crawler prevent small so it sends immediately in test
    cfg.crawler_prevent_ms = 0;

    let service = LoadReducerService::new(redis.clone(), mongo.clone(), crawler.clone(), cfg);

    let url = "https://example.com/missing".to_string();
    let res = service.process(vec![url.clone()]).await.unwrap();
    assert_eq!(res.len(), 0); // no data

    // crawler should have been called
    let sent = crawler.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0][0], url.clone());

    // redis should have inflight fields
    let store = redis.store.lock().unwrap();
    let hash = store.get(&format!("rcs::{}", url)).unwrap();
    assert!(hash.contains_key("last_crawler_send") || hash.contains_key("last_mongo_fetch"));
}

