use eyre::{eyre, WrapErr};
use std::sync::Arc;

fn try_get_env_var(key: &str) -> eyre::Result<String> {
    std::env::var(key).wrap_err(eyre!("Unable to get {key} env var"))
}

pub struct Config {
    pub input: GoogleStorageObjectRequestParameter,
    pub output_dataset: String,
    pub output_table: String,
}

impl Config {
    pub fn try_new() -> eyre::Result<Arc<Self>> {
        Ok(Arc::new(Config {
            input: GoogleStorageObjectRequestParameter {
                bucket: try_get_env_var("INPUT_BUCKET")?,
                object: try_get_env_var("INPUT_OBJECT")?,
            },
            output_dataset: try_get_env_var("OUTPUT_DATASET")?,
            output_table: try_get_env_var("OUTPUT_TABLE")?,
        }))
    }
}

#[derive(Debug)]
pub struct GoogleStorageObjectRequestParameter {
    bucket: String,
    object: String,
}

impl From<&GoogleStorageObjectRequestParameter>
    for google_cloud_storage::http::objects::get::GetObjectRequest
{
    fn from(parameters: &GoogleStorageObjectRequestParameter) -> Self {
        google_cloud_storage::http::objects::get::GetObjectRequest {
            bucket: parameters.bucket.clone(),
            object: parameters.object.clone(),
            ..Default::default()
        }
    }
}
