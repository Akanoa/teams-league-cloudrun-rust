use eyre::Context;
use std::collections::HashMap;
use std::str;

use time::OffsetDateTime;

use crate::domain;
use crate::domain::team_stats_structs::TeamScorerRaw;
use crate::domain::team_stats_structs::TeamStats;
use crate::domain::team_stats_structs::TeamStatsRaw;

pub fn map_to_team_stats_domains(
    ingestion_date: Option<OffsetDateTime>,
    team_slogans: &HashMap<&str, &str>,
    file_bytes: Vec<u8>,
) -> eyre::Result<Vec<TeamStats>> {
    String::from_utf8(file_bytes)?
        .split('\n')
        .filter(|team_stats_raw| !team_stats_raw.is_empty())
        .map(deserialize_to_team_stats_raw_object)
        .map(|x| {
            x.map(|team_stats_raw| {
                map_to_team_stats_domain(ingestion_date, team_slogans, team_stats_raw)
            })
        })
        .collect::<Result<Vec<_>, _>>()
}

fn deserialize_to_team_stats_raw_object(team_stats_raw_str: &str) -> eyre::Result<TeamStatsRaw> {
    serde_json::from_str(team_stats_raw_str)
        .wrap_err("Couldn't deserialize the team stats str to object")
}

fn map_to_team_stats_domain(
    ingestion_date: Option<OffsetDateTime>,
    team_slogans: &HashMap<&str, &str>,
    team_stats_raw: TeamStatsRaw,
) -> TeamStats {
    let team_total_goals: i64 = team_stats_raw
        .scorers
        .iter()
        .map(|scorer| scorer.goals)
        .sum();

    // todo: allows to compile the code
    let team_name = "unknown";

    let top_scorer_raw: &TeamScorerRaw = team_stats_raw
        .scorers
        .iter()
        .max_by_key(|scorer| scorer.goals)
        .unwrap_or_else(|| panic!("Top scorer not found for the team !! {team_name}"));

    let best_passer_raw: &TeamScorerRaw = team_stats_raw
        .scorers
        .iter()
        .max_by_key(|scorer| scorer.goal_assists)
        .unwrap_or_else(|| panic!("Best passer not found for the team !! {team_name}"));

    let top_scorer = domain::team_stats_structs::TopScorerStats {
        first_name: top_scorer_raw.scorer_first_name.to_string(),
        last_name: top_scorer_raw.scorer_last_name.to_string(),
        goals: top_scorer_raw.goals,
        games: top_scorer_raw.games,
    };

    let best_passer = domain::team_stats_structs::BestPasserStats {
        first_name: best_passer_raw.scorer_first_name.to_string(),
        last_name: best_passer_raw.scorer_last_name.to_string(),
        goal_assists: best_passer_raw.goal_assists,
        games: best_passer_raw.games,
    };

    let team_name = team_stats_raw.team_name;

    let team_slogan = team_slogans
        .get(team_name.as_str())
        .unwrap_or_else(|| panic!("Slogan not found for the team {team_name}"));

    TeamStats {
        team_name,
        team_score: team_stats_raw.team_score,
        team_total_goals,
        team_slogan: team_slogan.to_string(),
        top_scorer_stats: top_scorer,
        best_passer_stats: best_passer,
        ingestion_date,
    }
}
