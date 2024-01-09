use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Serialize, Deserialize)]
pub struct TeamScorerRaw {
    pub scorerFirstName: String,
    pub scorerLastName: String,
    pub goals: i64,
    pub goalAssists: i64,
    pub games: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TeamStatsRaw {
    pub teamName: String,
    pub teamScore: i64,
    pub scorers: Vec<TeamScorerRaw>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TopScorerStats {
    pub firstName: String,
    pub lastName: String,
    pub goals: i64,
    pub games: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BestPasserStats {
    pub firstName: String,
    pub lastName: String,
    pub goalAssists: i64,
    pub games: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TeamStats {
    pub teamName: String,
    pub teamScore: i64,
    pub teamTotalGoals: i64,
    pub teamSlogan: String,
    pub topScorerStats: TopScorerStats,
    pub bestPasserStats: BestPasserStats,
    #[serde(with = "time::serde::rfc3339::option")]
    pub ingestionDate: Option<OffsetDateTime>,
}
