use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct TeamScorerRaw {
    pub scorer_first_name: String,
    pub scorer_last_name: String,
    pub goals: i64,
    pub goal_assists: i64,
    pub games: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct TeamStatsRaw {
    pub team_name: String,
    pub team_score: i64,
    pub scorers: Vec<TeamScorerRaw>,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct TopScorerStats {
    pub first_name: String,
    pub last_name: String,
    pub goals: i64,
    pub games: i64,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct BestPasserStats {
    pub first_name: String,
    pub last_name: String,
    pub goal_assists: i64,
    pub games: i64,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all(serialize = "camelCase"))]
pub struct TeamStats {
    pub team_name: String,
    pub team_score: i64,
    pub team_total_goals: i64,
    pub team_slogan: String,
    pub top_scorer_stats: TopScorerStats,
    pub best_passer_stats: BestPasserStats,
    #[serde(with = "time::serde::rfc3339::option")]
    pub ingestion_date: Option<OffsetDateTime>,
}
