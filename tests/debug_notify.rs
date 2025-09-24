use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use load_reducer_poc::ports::{RedisPort, BoxError};

#[derive(Clone)]
struct CoordinatedRedisSimple {
    store: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl CoordinatedRedisSimple {
    fn new() -> Self { Self { store: Arc::new(Mutex::new(HashMap::new())), counter: Arc::new(AtomicUsize::new(0)), notify: Arc::new(Notify::new()) } }
}

#[async_trait]
impl RedisPort for CoordinatedRedisSimple {
    async fn multi_hgetall(&self, _keys: &[String]) -> Result<Vec<HashMap<String, String>>, BoxError> { Ok(vec![]) }
    async fn hgetall(&self, key: &str) -> Result<HashMap<String, String>, BoxError> {
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!("[CoordSimple] hgetall called nth={}", n);
        if n == 2 {
            eprintln!("[CoordSimple] waiting for notify");
            self.notify.notified().await;
            eprintln!("[CoordSimple] resumed after notify");
        }
        Ok(HashMap::new())
    }
    async fn write_cache_and_clear(&self, _key: &str, _data: &str, _cache_ttl: u64) -> Result<(), BoxError> { Ok(()) }
    async fn set_inflight_fields(&self, _key: &str, _last_mongo: Option<u64>, _last_crawler: Option<u64>, _inflight_ttl: u64) -> Result<(), BoxError> {
        eprintln!("[CoordSimple] set_inflight_fields called, notifying");
        self.notify.notify_waiters();
        Ok(())
    }
}

#[tokio::test]
async fn debug_notify_works() {
    let r = CoordinatedRedisSimple::new();
    let r1 = r.clone();
    let r2 = r.clone();

    let t1 = tokio::spawn(async move {
        r1.hgetall("k").await.unwrap();
        // then set inflight
        r1.set_inflight_fields("k", None, Some(1), 10).await.unwrap();
        eprintln!("[t1] done set_inflight");
    });

    let t2 = tokio::spawn(async move {
        r2.hgetall("k").await.unwrap();
        eprintln!("[t2] done hgetall");
    });

    t1.await.unwrap();
    t2.await.unwrap();
    eprintln!("[debug_notify_works] both tasks completed");
}

