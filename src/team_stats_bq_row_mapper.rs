use google_cloud_bigquery::http::tabledata::insert_all::Row;

use crate::domain::team_stats_structs::TeamStats;

pub struct TeamStatsBQRowMapper;

impl TeamStatsBQRowMapper {
    pub fn map_to_team_stats_bigquery_rows(
        team_stats_domain_list: Vec<TeamStats>,
    ) -> Vec<Row<TeamStats>> {
        team_stats_domain_list
            .iter()
            .map(|team_stats| {
                TeamStatsBQRowMapper::map_to_team_stats_bigquery_row(TeamStats::clone(team_stats))
            })
            .collect::<Vec<Row<TeamStats>>>()
    }

    fn map_to_team_stats_bigquery_row(team_stats: TeamStats) -> Row<TeamStats> {
        Row {
            insert_id: None,
            json: team_stats,
        }
    }
}
