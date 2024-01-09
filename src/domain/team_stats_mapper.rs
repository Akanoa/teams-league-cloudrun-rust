use std::collections::HashMap;
use std::str;

use time::OffsetDateTime;

use crate::domain;
use crate::domain::team_stats_structs::TeamScorerRaw;
use crate::domain::team_stats_structs::TeamStats;
use crate::domain::team_stats_structs::TeamStatsRaw;

pub struct TeamStatsMapper;

impl TeamStatsMapper {
    pub fn map_to_team_stats_domains(
        ingestion_date: Option<OffsetDateTime>,
        team_slogans: HashMap<&str, &str>,
        result_file_as_bytes: Vec<u8>,
    ) -> Vec<TeamStats> {
        str::from_utf8(result_file_as_bytes.as_slice())
            .expect("")
            .split('\n')
            .filter(|team_stats_raw| !team_stats_raw.is_empty())
            .map(TeamStatsMapper::deserialize_to_team_stats_raw_object)
            .map(|team_stats_raw| {
                TeamStatsMapper::map_to_team_stats_domain(
                    ingestion_date,
                    team_slogans.clone(),
                    team_stats_raw,
                )
            })
            .collect::<Vec<TeamStats>>()
    }

    fn deserialize_to_team_stats_raw_object(team_stats_raw_str: &str) -> TeamStatsRaw {
        let res_deserialization = serde_json::from_str(team_stats_raw_str);

        match res_deserialization {
            Ok(team_stats_raw) => team_stats_raw,
            Err(e) => panic!("couldn't deserialize the team stats str to object: {}", e),
        }
    }

    fn map_to_team_stats_domain(
        ingestion_date: Option<OffsetDateTime>,
        team_slogans: HashMap<&str, &str>,
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
            .max_by_key(|scorer| scorer.goalAssists)
            .unwrap_or_else(|| panic!("Best passer not found for the team !! {team_name}"));

        let top_scorer = domain::team_stats_structs::TopScorerStats {
            firstName: top_scorer_raw.scorerFirstName.to_string(),
            lastName: top_scorer_raw.scorerLastName.to_string(),
            goals: top_scorer_raw.goals,
            games: top_scorer_raw.games,
        };

        let best_passer = domain::team_stats_structs::BestPasserStats {
            firstName: best_passer_raw.scorerFirstName.to_string(),
            lastName: best_passer_raw.scorerLastName.to_string(),
            goalAssists: best_passer_raw.goalAssists,
            games: best_passer_raw.games,
        };

        let team_name = team_stats_raw.teamName;

        let team_slogan = team_slogans
            .get(team_name.as_str())
            .unwrap_or_else(|| panic!("Slogan not found for the team {team_name}"));

        TeamStats {
            teamName: team_name,
            teamScore: team_stats_raw.teamScore,
            teamTotalGoals: team_total_goals,
            teamSlogan: team_slogan.to_string(),
            topScorerStats: top_scorer,
            bestPasserStats: best_passer,
            ingestionDate: ingestion_date,
        }
    }
}
