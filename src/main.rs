use actix_web::{App, HttpResponse, HttpServer, Responder, middleware::Logger, post, web};
use chrono::Utc;
use deadpool_redis::{
    Pool,
    redis::{self, cmd, pipe},
};
use env_logger::Env;
use futures::stream::TryStreamExt;
use mongodb::{
    Client as MongoClient, Collection,
    bson::{Document, doc},
    options::ClientOptions,
};
use reqwest;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::Arc;

const CACHE_TTL_SEC: u64 = 3600; // 1 hour for positive cache
const MONGO_PREVENT_MS: u64 = 10_000; // 10 seconds
const CRAWLER_PREVENT_MS: u64 = 900_000; // 15 minutes
const INFLIGHT_TTL_SEC: u64 = 86_400; // 24 hours for inflight cleanup

#[derive(Clone)]
struct AppState {
    redis: Pool,
    mongo: Collection<Document>,
    crawler_url: String,
}

#[derive(Serialize, Deserialize)]
struct UrlData {
    url: String,
    data: String,
}

#[post("/api")]
async fn handler(urls: web::Json<Vec<String>>, state: web::Data<Arc<AppState>>) -> impl Responder {
    let urls = urls.0;
    let mut conn = match state.redis.get().await {
        Ok(conn) => conn,
        Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
    };
    let now_ms = Utc::now().timestamp_millis() as u64;

    // Pipeline to fetch all hashes
    let cache_keys: Vec<String> = urls.iter().map(|u| format!("rcs::{}", u)).collect();
    let mut rpipe = pipe();
    for key in &cache_keys {
        rpipe.cmd("HGETALL").arg(key);
    }
    let hashes: Vec<HashMap<String, String>> = match rpipe.query_async(&mut conn).await {
        Ok(h) => h,
        Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
    };

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

        // Inflight state
        let last_mongo_str = hash.get("last_mongo_fetch").cloned().unwrap_or_default();
        let last_mongo: u64 = last_mongo_str.parse().unwrap_or(0);

        if now_ms.saturating_sub(last_mongo) >= MONGO_PREVENT_MS {
            to_query_mongo.insert(url);
        } else {
            assumed_missing.push(url);
        }
    }

    // Add any URLs not in Redis at all to to_query_mongo
    let all_urls_set: HashSet<String> = urls.iter().cloned().collect();
    let processed_urls: HashSet<String> = data_map
        .keys()
        .cloned()
        .chain(assumed_missing.iter().cloned())
        .collect();
    for url in all_urls_set.difference(&processed_urls) {
        to_query_mongo.insert(url.clone());
    }

    // Query MongoDB in batch for to_query_mongo
    let mut mongo_found: HashMap<String, String> = HashMap::new();
    if !to_query_mongo.is_empty() {
        let filter = doc! { "url": { "$in": to_query_mongo.iter().cloned().collect::<Vec<_>>() } };
        // let options = FindOptions::builder().build();
        let cursor_res = state.mongo.find(filter).await;

        let mut cursor = match cursor_res {
            Ok(c) => c,
            Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
        };

        while let Ok(Some(doc)) = cursor.try_next().await {
            // let doc = match doc_res {
            //     Ok(Some(d)) => d,
            //     Ok(None) => break,
            //     Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
            // };
            let url = match doc.get_str("url") {
                Ok(u) => u.to_string(),
                Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
            };
            let data = match doc.get_str("data") {
                Ok(d) => d.to_string(),
                Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
            };
            mongo_found.insert(url, data);
        }
    }

    // Process mongo results
    for (url, data) in mongo_found {
        let key = format!("rcs::{}", url);
        let mut rpipe = pipe();
        rpipe.cmd("HSET").arg(&key).arg("data").arg(&data);
        rpipe.cmd("HDEL").arg(&key).arg("last_mongo_fetch");
        rpipe.cmd("HDEL").arg(&key).arg("last_crawler_send");
        rpipe.cmd("EXPIRE").arg(&key).arg(CACHE_TTL_SEC);
        let _ = match rpipe.query_async::<redis::Value>(&mut conn).await {
            Ok(_) => (),
            Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
        };
        data_map.insert(url, data);
    }

    // Collect all missing: queried_not_found + assumed_missing
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

    // For all_missing, determine to_crawler and update timestamps
    let mut to_crawler: Vec<String> = Vec::new();
    for url in all_missing {
        let key = format!("rcs::{}", url);

        // Fetch current hash again for this url (to handle potential races)
        let hash: HashMap<String, String> =
            match cmd("HGETALL").arg(&key).query_async(&mut conn).await {
                Ok(h) => h,
                Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
            };

        let last_crawler_str = hash.get("last_crawler_send").cloned().unwrap_or_default();
        let last_crawler: u64 = last_crawler_str.parse().unwrap_or(0);
        let should_send_crawler = now_ms.saturating_sub(last_crawler) >= CRAWLER_PREVENT_MS;

        // Always update last_mongo_fetch if this was a queried_not_found
        let is_queried_not_found = queried_not_found.contains(&url);
        let mut pipe = pipe();
        if is_queried_not_found {
            pipe.cmd("HSET")
                .arg(&key)
                .arg("last_mongo_fetch")
                .arg(now_ms.to_string());
        }
        if should_send_crawler {
            pipe.cmd("HSET")
                .arg(&key)
                .arg("last_crawler_send")
                .arg(now_ms.to_string());
            to_crawler.push(url.clone());
        }
        if is_queried_not_found || should_send_crawler {
            pipe.cmd("EXPIRE").arg(&key).arg(INFLIGHT_TTL_SEC);
            let _: () = match pipe.query_async::<redis::Value>(&mut conn).await {
                Ok(_) => (),
                Err(e) => return HttpResponse::InternalServerError().body(e.to_string()),
            };
        }
    }

    // Send batch to crawler if any
    if !to_crawler.is_empty() {
        let client = reqwest::Client::new();
        let res = client
            .post(&state.crawler_url)
            .json(&to_crawler)
            .send()
            .await;
        if let Err(e) = res {
            return HttpResponse::InternalServerError().body(e.to_string());
        }
    }

    // Build response in original order, only including URLs with data
    let response: Vec<UrlData> = urls
        .into_iter()
        .filter_map(|url| {
            data_map.get(&url).map(|data| UrlData {
                url: url.clone(),
                data: data.clone(),
            })
        })
        .collect();

    HttpResponse::Ok().json(response)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    // Load environment variables
    let redis_url = env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".to_string());
    let mongo_url = env::var("MONGO_URL").unwrap_or("mongodb://localhost:27017".to_string());
    let crawler_url = env::var("CRAWLER_URL").unwrap_or("http://localhost:8081/crawl".to_string());

    // Setup Redis
    let redis_cfg = deadpool_redis::Config::from_url(redis_url);
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .unwrap();

    // Setup MongoDB
    let mongo_options = ClientOptions::parse(&mongo_url).await.unwrap();
    let mongo_client = MongoClient::with_options(mongo_options).unwrap();
    let db = mongo_client.database("my_database");
    let coll = db.collection::<Document>("url_data");

    let state = web::Data::new(Arc::new(AppState {
        redis: redis_pool,
        mongo: coll,
        crawler_url,
    }));

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .service(handler)
    })
    .bind(("0.0.0.0", 8000))?
    .run()
    .await
}
