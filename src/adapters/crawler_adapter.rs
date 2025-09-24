use crate::ports::{BoxError, CrawlerPort};

#[derive(Clone)]
pub struct ReqwestCrawlerAdapter {
    pub client: reqwest::Client,
    pub url: String,
}

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
