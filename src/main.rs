use actix_web::{App, HttpResponse, HttpServer, Responder, middleware::Logger, post, web};
use env_logger::Env;
use std::env;
use std::sync::Arc;

mod domain;
mod ports;
mod adapters;
mod service;
mod config;

use crate::adapters::redis_adapter::DeadpoolRedisAdapter;
use crate::adapters::mongo_adapter::MongoAdapter;
use crate::adapters::crawler_adapter::ReqwestCrawlerAdapter;
use crate::service::LoadReducerService;
use crate::config::Config;

type ConcreteService = LoadReducerService<DeadpoolRedisAdapter, MongoAdapter, ReqwestCrawlerAdapter>;

#[post("/api")]
async fn handler(urls: web::Json<Vec<String>>, svc: web::Data<Arc<ConcreteService>>) -> impl Responder {
    match svc.process(urls.0).await {
        Ok(res) => HttpResponse::Ok().json(res),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    // Load environment variables
    let redis_url = env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".to_string());
    let mongo_url = env::var("MONGO_URL").unwrap_or("mongodb://localhost:27017".to_string());
    let crawler_url = env::var("CRAWLER_URL").unwrap_or("http://localhost:8081/crawl".to_string());

    let config = Config::from_env();

    // Setup Redis
    let redis_cfg = deadpool_redis::Config::from_url(redis_url);
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .unwrap();

    // Setup MongoDB
    let mongo_options = mongodb::options::ClientOptions::parse(&mongo_url).await.unwrap();
    let mongo_client = mongodb::Client::with_options(mongo_options).unwrap();
    let db = mongo_client.database("my_database");
    let coll = db.collection::<mongodb::bson::Document>("url_data");

    // Create adapters
    let redis_adapter = DeadpoolRedisAdapter { pool: redis_pool.clone() };
    let mongo_adapter = MongoAdapter { coll: coll.clone() };
    let crawler_adapter = ReqwestCrawlerAdapter { client: reqwest::Client::new(), url: crawler_url.clone() };

    // Create service with config
    let service = LoadReducerService::new(redis_adapter, mongo_adapter, crawler_adapter, config);

    let service_data: web::Data<Arc<ConcreteService>> = web::Data::new(Arc::new(service));

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(service_data.clone())
            .service(handler)
    })
    .bind(("0.0.0.0", 8000))?
    .run()
    .await
}
