use crate::ports::{BoxError, MongoPort};
use futures::stream::TryStreamExt;
use mongodb::{
    Collection,
    bson::{Document, doc},
};
use std::collections::HashMap;

#[derive(Clone)]
pub struct MongoAdapter {
    pub coll: Collection<Document>,
}

impl MongoPort for MongoAdapter {
    async fn find_by_urls(&self, urls: &[String]) -> Result<HashMap<String, String>, BoxError> {
        let filter = doc! { "url": { "$in": urls.iter().cloned().collect::<Vec<_>>() } };
        let mut cursor = self.coll.find(filter).await?;
        let mut map = HashMap::new();
        while let Some(doc) = cursor.try_next().await? {
            if let Ok(u) = doc.get_str("url") {
                if let Ok(d) = doc.get_str("data") {
                    map.insert(u.to_string(), d.to_string());
                }
            }
        }
        Ok(map)
    }
}
