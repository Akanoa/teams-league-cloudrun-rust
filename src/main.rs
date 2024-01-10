#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use eyre::{eyre, Context};
use google_cloud_bigquery::client::Client as bigQueryClient;
use google_cloud_bigquery::http::tabledata::insert_all::InsertAllRequest;
use google_cloud_storage::client::Client as gcsClient;
use http_body_util::Full;
use hyper::body::Body;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use time::OffsetDateTime;
use tokio::net::TcpListener;

use domain::team_stats_mapper::TeamStatsMapper;
use team_stats_bq_row_mapper::TeamStatsBQRowMapper;
use teams_league_cloudrun_rust::{
    config::Config, download_file, get_big_query_client, get_gcs_client,
};

mod team_stats_bq_row_mapper;

mod domain;

lazy_static! {
    static ref INGESTION_DATE: Option<OffsetDateTime> = Some(OffsetDateTime::now_utc());
}

async fn raw_to_team_stats_domain_and_load_result_bq(
    req: Request<impl Body>,
    gcs_client: Arc<gcsClient>,
    big_query_client: Arc<bigQueryClient>,
    project_id: Arc<str>,
    config: Arc<Config>,
) -> eyre::Result<Response<Full<Bytes>>> {
    println!("######################Request URI######################");
    println!("{:#?}", req.uri());
    println!("######################");

    // Router

    if req.uri() == "/favicon.ico" {
        return Ok(Response::new(Full::new(Bytes::from(
            "Not the expected URI, no treatment in this case",
        ))));
    }

    // Ingest data

    println!("Reading team stats raw data from Cloud Storage...");

    let team_slogans = HashMap::from([("PSG", "Paris est magique"), ("Real", "Hala Madrid")]);

    let file_bytes = download_file(&gcs_client, &config.input).await?;

    // Insert data

    let team_stats_domain_list =
        TeamStatsMapper::map_to_team_stats_domains(*INGESTION_DATE, team_slogans, file_bytes);

    let team_stats_table_bq_rows =
        TeamStatsBQRowMapper::map_to_team_stats_bigquery_rows(team_stats_domain_list);

    let request = InsertAllRequest {
        rows: team_stats_table_bq_rows,
        ..Default::default()
    };
    let result = big_query_client
        .tabledata()
        .insert(
            project_id.deref(),
            &config.output_dataset,
            &config.output_table,
            &request,
        )
        .await
        .wrap_err("Unable to insert data")?;

    if let Some(err) = result.insert_errors {
        return Err(eyre!(
            "Error when trying to load the Team Stats domain data to BigQuery : {err:#?}"
        ));
    }

    Ok(Response::new(Full::new(Bytes::from(
        "The Team Stats domain data was correctly loaded to BigQuery",
    ))))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    let gcs_client = get_gcs_client().await?;
    let (big_query_client, project_id) = get_big_query_client().await?;
    let config = Config::try_new()?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        let gcs_client = gcs_client.clone();
        let big_query_client = big_query_client.clone();
        let project_id = project_id.clone();
        let config = config.clone();

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(
                    io,
                    service_fn(|req| {
                        raw_to_team_stats_domain_and_load_result_bq(
                            req,
                            gcs_client.clone(),
                            big_query_client.clone(),
                            project_id.clone(),
                            config.clone(),
                        )
                    }),
                )
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}
