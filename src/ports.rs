use std::collections::HashMap;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub trait RedisPort: Send + Sync {
    async fn multi_hgetall(
        &self,
        keys: &[String],
    ) -> Result<Vec<HashMap<String, String>>, BoxError>;
    async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, BoxError>;
    async fn write_cache_and_clear(
        &self,
        key: &str,
        data: &str,
        cache_ttl: u64,
    ) -> Result<(), BoxError>;
    async fn set_inflight_fields(
        &self,
        key: &str,
        last_mongo: Option<u64>,
        last_crawler: Option<u64>,
        inflight_ttl: u64,
    ) -> Result<(), BoxError>;
}

pub trait MongoPort: Send + Sync {
    async fn find_by_urls(&self, urls: &[String]) -> Result<HashMap<String, String>, BoxError>;
}

pub trait CrawlerPort: Send + Sync {
    async fn send_batch(&self, urls: &[String]) -> Result<(), BoxError>;
}
