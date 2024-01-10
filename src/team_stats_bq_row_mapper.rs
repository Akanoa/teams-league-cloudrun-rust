use google_cloud_bigquery::http::tabledata::insert_all::Row;

use crate::domain::team_stats_structs::TeamStats;

pub fn map_to_team_stats_bigquery_rows(
    team_stats_domain_list: Vec<TeamStats>,
) -> Vec<Row<TeamStats>> {
    team_stats_domain_list
        .into_iter()
        .map(Row::from)
        .collect::<Vec<Row<TeamStats>>>()
}
