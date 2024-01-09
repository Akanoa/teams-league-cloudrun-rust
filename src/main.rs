#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::net::SocketAddr;

use bytes::Bytes;
use google_cloud_bigquery::client::{Client as bigQueryClient, ClientConfig as bigQueryClientConfig};
use google_cloud_bigquery::http::tabledata::insert_all::InsertAllRequest;
use google_cloud_storage::client::{Client as gcsClient, ClientConfig as gcsClientConfig};
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use http_body_util::Full;
use hyper::{Request, Response};
use hyper::body::Body;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use time::OffsetDateTime;
use tokio::net::TcpListener;

use domain::team_stats_mapper::TeamStatsMapper;
use team_stats_bq_row_mapper::TeamStatsBQRowMapper;

mod team_stats_bq_row_mapper;

mod domain;

lazy_static! {
    static ref INGESTION_DATE: Option<OffsetDateTime> = Some(OffsetDateTime::now_utc());
}

fn get_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Env var {key} was not set "))
}

async fn raw_to_team_stats_domain_and_load_result_bq(req: Request<impl Body>) -> Result<Response<Full<Bytes>>, Infallible> {
    println!("######################Request URI######################");
    println!("{:#?}", req.uri());
    println!("######################");

    if req.uri().eq("/favicon.ico") {
        return Ok(Response::new(Full::new(Bytes::from("Not the expected URI, no treatment in this case"))));
    }

    println!("Reading team stats raw data from Cloud Storage...");

    let input_bucket = get_env_var("INPUT_BUCKET");
    let input_object = get_env_var("INPUT_OBJECT");
    let output_dataset = get_env_var("OUTPUT_DATASET");
    let output_table = get_env_var("OUTPUT_TABLE");

    let team_slogans = HashMap::from([
        ("PSG", "Paris est magique"),
        ("Real", "Hala Madrid"),
    ]);

    let gcs_client_config = gcsClientConfig::default().with_auth().await.unwrap();
    let gcs_client = gcsClient::new(gcs_client_config);

    let input_file_as_bytes_res = gcs_client.download_object(&GetObjectRequest {
        bucket: input_bucket.to_string(),
        object: input_object.to_string(),
        ..Default::default()
    }, &Range::default()).await;

    let result_file_as_bytes = match input_file_as_bytes_res {
        Ok(v) => v,
        Err(e) => panic!("Error when reading the input file: {}", e),
    };

    let team_stats_domain_list = TeamStatsMapper::map_to_team_stats_domains(
        *INGESTION_DATE,
        team_slogans,
        result_file_as_bytes,
    );

    let (config, project_id) = bigQueryClientConfig::new_with_auth().await.unwrap();
    let client = bigQueryClient::new(config).await.unwrap();

    let team_stats_table_bq_rows = TeamStatsBQRowMapper::map_to_team_stats_bigquery_rows(team_stats_domain_list);

    let request = InsertAllRequest {
        rows: team_stats_table_bq_rows,
        ..Default::default()
    };
    let result = client.tabledata().insert(
        project_id.unwrap().as_str(),
        output_dataset.as_str(),
        output_table.as_str(),
        &request,
    ).await.unwrap();

    let bigquery_insert_errors = result.insert_errors;
    match bigquery_insert_errors {
        None => Ok(Response::new(Full::new(Bytes::from("The Team Stats domain data was correctly loaded to BigQuery")))),
        Some(e) => panic!("Error when trying to load the Team Stats domain data to BigQuery : {:#?}", e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(raw_to_team_stats_domain_and_load_result_bq))
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
