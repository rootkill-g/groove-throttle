use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UrlData {
    pub url: String,
    pub data: String,
}

