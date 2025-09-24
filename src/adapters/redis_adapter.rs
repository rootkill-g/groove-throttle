use crate::ports::BoxError;
use crate::ports::RedisPort;
use async_trait::async_trait;
use deadpool_redis::{Pool, redis::{cmd, pipe}};
use std::collections::HashMap;

#[derive(Clone)]
pub struct DeadpoolRedisAdapter {
    pub pool: Pool,
}

#[async_trait]
impl RedisPort for DeadpoolRedisAdapter {
    async fn multi_hgetall(&self, keys: &[String]) -> Result<Vec<HashMap<String, String>>, BoxError> {
        let mut conn = self.pool.get().await?;
        let mut rpipe = pipe();
        for key in keys {
            rpipe.cmd("HGETALL").arg(key);
        }
        let hashes: Vec<HashMap<String, String>> = rpipe.query_async(&mut conn).await?;
        Ok(hashes)
    }

    async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, BoxError> {
        let mut conn = self.pool.get().await?;
        let hash: HashMap<String, String> = cmd("HGETALL").arg(key).query_async(&mut conn).await?;
        Ok(hash)
    }

    async fn write_cache_and_clear(&self, key: &str, data: &str, cache_ttl: u64) -> Result<(), BoxError> {
        let mut conn = self.pool.get().await?;
        let mut rpipe = pipe();
        rpipe.cmd("HSET").arg(key).arg("data").arg(data);
        rpipe.cmd("HDEL").arg(key).arg("last_mongo_fetch");
        rpipe.cmd("HDEL").arg(key).arg("last_crawler_send");
        rpipe.cmd("EXPIRE").arg(key).arg(cache_ttl);
        let _ : () = rpipe.query_async(&mut conn).await?;
        Ok(())
    }

    async fn set_inflight_fields(&self, key: &str, last_mongo: Option<u64>, last_crawler: Option<u64>, inflight_ttl: u64) -> Result<(), BoxError> {
        let mut conn = self.pool.get().await?;
        let mut pipe_cmd = pipe();
        if let Some(m) = last_mongo {
            pipe_cmd.cmd("HSET").arg(key).arg("last_mongo_fetch").arg(m.to_string());
        }
        if let Some(c) = last_crawler {
            pipe_cmd.cmd("HSET").arg(key).arg("last_crawler_send").arg(c.to_string());
        }
        if last_mongo.is_some() || last_crawler.is_some() {
            pipe_cmd.cmd("EXPIRE").arg(key).arg(inflight_ttl);
            let _ : () = pipe_cmd.query_async(&mut conn).await?;
        }
        Ok(())
    }
}

