use crate::ports::{BoxError, CrawlerPort};
use async_trait::async_trait;

#[derive(Clone)]
pub struct ReqwestCrawlerAdapter {
    pub client: reqwest::Client,
    pub url: String,
}

#[async_trait]
impl CrawlerPort for ReqwestCrawlerAdapter {
    async fn send_batch(&self, urls: &[String]) -> Result<(), BoxError> {
        let res = self.client.post(&self.url).json(&urls).send().await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(format!("crawler returned status {}", res.status()).into())
        }
    }
}

