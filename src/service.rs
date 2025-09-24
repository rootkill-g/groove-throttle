use crate::domain::UrlData;
use crate::ports::{BoxError, RedisPort, MongoPort, CrawlerPort};
use crate::config::Config;
use std::collections::{HashMap, HashSet};
use chrono::Utc;

pub struct LoadReducerService<R, M, C>
where
    R: RedisPort,
    M: MongoPort,
    C: CrawlerPort,
{
    pub redis: R,
    pub mongo: M,
    pub crawler: C,
    pub config: Config,
}

impl<R, M, C> LoadReducerService<R, M, C>
where
    R: RedisPort,
    M: MongoPort,
    C: CrawlerPort,
{
    pub fn new(redis: R, mongo: M, crawler: C, config: Config) -> Self {
        Self { redis, mongo, crawler, config }
    }

    pub async fn process(&self, urls: Vec<String>) -> Result<Vec<UrlData>, BoxError> {
        let now_ms = Utc::now().timestamp_millis() as u64;
        let cache_keys: Vec<String> = urls.iter().map(|u| format!("rcs::{}", u)).collect();

        // Fetch hashes from Redis in one pipeline
        let hashes = self.redis.multi_hgetall(&cache_keys).await?;

        // Process hashes
        let mut data_map: HashMap<String, String> = HashMap::new();
        let mut to_query_mongo: HashSet<String> = HashSet::new();
        let mut assumed_missing: Vec<String> = Vec::new();

        for (i, hash) in hashes.into_iter().enumerate() {
            let url = urls[i].clone();
            if let Some(data) = hash.get("data") {
                data_map.insert(url, data.clone());
                continue;
            }

            let last_mongo_str = hash.get("last_mongo_fetch").cloned().unwrap_or_default();
            let last_mongo: u64 = last_mongo_str.parse().unwrap_or(0);
            if now_ms.saturating_sub(last_mongo) >= self.config.mongo_prevent_ms {
                to_query_mongo.insert(url);
            } else {
                assumed_missing.push(url);
            }
        }

        // Add URLs not present in Redis at all
        let all_urls_set: HashSet<String> = urls.iter().cloned().collect();
        let processed_urls: HashSet<String> = data_map
            .keys()
            .cloned()
            .chain(assumed_missing.iter().cloned())
            .collect();
        for url in all_urls_set.difference(&processed_urls) {
            to_query_mongo.insert(url.clone());
        }

        // Query Mongo in batch
        let mut mongo_found: HashMap<String, String> = HashMap::new();
        if !to_query_mongo.is_empty() {
            let to_query_vec: Vec<String> = to_query_mongo.iter().cloned().collect();
            let found = self.mongo.find_by_urls(&to_query_vec).await?;
            mongo_found = found;
        }

        // Process mongo results and write cache
        for (url, data) in mongo_found.into_iter() {
            let key = format!("rcs::{}", url);
            self.redis
                .write_cache_and_clear(&key, &data, self.config.cache_ttl_sec)
                .await?;
            data_map.insert(url, data);
        }

        // Build missing lists
        let queried_not_found: Vec<String> = to_query_mongo
            .iter()
            .filter(|u| !data_map.contains_key(*u))
            .cloned()
            .collect();
        let all_missing = queried_not_found
            .iter()
            .chain(assumed_missing.iter())
            .cloned()
            .collect::<Vec<_>>();

        // Determine which to send to crawler and update inflight fields
        let mut to_crawler: Vec<String> = Vec::new();
        for url in all_missing.iter() {
            let key = format!("rcs::{}", url);
            let hash = self.redis.hgetall(&key).await?;
            let last_crawler_str = hash.get("last_crawler_send").cloned().unwrap_or_default();
            let last_crawler: u64 = last_crawler_str.parse().unwrap_or(0);
            let should_send_crawler = now_ms.saturating_sub(last_crawler) >= self.config.crawler_prevent_ms;

            let is_queried_not_found = queried_not_found.contains(url);
            let last_mongo = if is_queried_not_found { Some(now_ms) } else { None };
            let last_crawler_opt = if should_send_crawler { Some(now_ms) } else { None };

            if should_send_crawler {
                to_crawler.push(url.clone());
            }

            if last_mongo.is_some() || last_crawler_opt.is_some() {
                self.redis
                    .set_inflight_fields(&key, last_mongo, last_crawler_opt, self.config.inflight_ttl_sec)
                    .await?;
            }
        }

        if !to_crawler.is_empty() {
            self.crawler.send_batch(&to_crawler).await?;
        }

        // Build response preserving order
        let response: Vec<UrlData> = urls
            .into_iter()
            .filter_map(|url| data_map.get(&url).map(|data| UrlData { url: url.clone(), data: data.clone() }))
            .collect();

        Ok(response)
    }
}
