use eyre::{Context, eyre, OptionExt};
use google_cloud_storage::http::objects::download::Range;
use std::sync::Arc;
use config::GoogleStorageObjectRequestParameter;

pub mod config;

pub async fn get_gcs_client() -> eyre::Result<Arc<google_cloud_storage::client::Client>> {
    let gcs_client_config = google_cloud_storage::client::ClientConfig::default()
        .with_auth()
        .await
        .wrap_err("Unable to create Google Cloud Storage Client")?;
    Ok(Arc::new(google_cloud_storage::client::Client::new(
        gcs_client_config,
    )))
}

pub async fn get_big_query_client(
) -> eyre::Result<(Arc<google_cloud_bigquery::client::Client>, Arc<str>)> {
    let (config, maybe_project_id) = google_cloud_bigquery::client::ClientConfig::new_with_auth()
        .await
        .wrap_err("Unable to build Big Query Config")?;
    let client = google_cloud_bigquery::client::Client::new(config)
        .await
        .wrap_err("Unable to build Big Query Client")?;

    let project_id = maybe_project_id.ok_or_eyre("Unable to get a project ID")?;
    let project_id = Arc::from(project_id.as_str());

    Ok((Arc::new(client), project_id))
}

pub async fn download_file(
    client: &google_cloud_storage::client::Client,
    object_config: &GoogleStorageObjectRequestParameter,
) -> eyre::Result<Vec<u8>> {
    client
        .download_object(&object_config.into(), &Range::default())
        .await
        .wrap_err(eyre!(
            "Unable to download object with parameters {object_config:?}"
        ))
}
