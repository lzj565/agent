//! Reads sing-box's local V2Ray-compatible StatsService without resetting its
//! lifetime counters. The hub owns all accumulation and reset semantics.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::Serialize;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tonic_prost::ProstCodec;

const QUERY_STATS: &str = "/v2ray.core.app.stats.command.StatsService/QueryStats";
const INBOUND_UP: &str = ">>>traffic>>>uplink";
const INBOUND_DOWN: &str = ">>>traffic>>>downlink";

#[derive(Clone, PartialEq, ::prost::Message)]
struct QueryStatsRequest {
    #[prost(string, tag = "1")]
    pattern: String,
    #[prost(bool, tag = "2")]
    reset: bool,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct Stat {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(int64, tag = "2")]
    value: i64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct QueryStatsResponse {
    #[prost(message, repeated, tag = "1")]
    stat: Vec<Stat>,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Counters {
    uplink: i64,
    downlink: i64,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    users: HashMap<String, Counters>,
    inbounds: HashMap<String, Counters>,
}

#[derive(Clone)]
pub struct Collector {
    channel: Channel,
}

impl Default for Collector {
    fn default() -> Self {
        Self { channel: Endpoint::from_static("http://127.0.0.1:9001").connect_lazy() }
    }
}

impl Collector {
    pub async fn collect(&self) -> Result<Snapshot> {
        let mut grpc = tonic::client::Grpc::new(self.channel.clone());
        grpc.ready().await.map_err(|e| anyhow!("StatsService channel is not ready: {e}"))?;
        let response: tonic::Response<QueryStatsResponse> = grpc
            .unary(
                Request::new(QueryStatsRequest { pattern: String::new(), reset: false }),
                PathAndQuery::from_static(QUERY_STATS),
                ProstCodec::default(),
            )
            .await
            .map_err(|status: Status| anyhow!("QueryStats failed: {status}"))?;
        snapshot(response.into_inner().stat)
    }
}

fn snapshot(stats: Vec<Stat>) -> Result<Snapshot> {
    let (mut users, mut inbounds) = (HashMap::<String, Counters>::new(), HashMap::<String, Counters>::new());
    for stat in stats {
        if stat.value < 0 {
            anyhow::bail!("StatsService returned a negative counter for {}", stat.name);
        }
        if let Some(rest) = stat.name.strip_prefix("user>>>") {
            if let Some(name) = rest.strip_suffix(INBOUND_UP) {
                users.entry(name.to_owned()).or_default().uplink = stat.value;
            } else if let Some(name) = rest.strip_suffix(INBOUND_DOWN) {
                users.entry(name.to_owned()).or_default().downlink = stat.value;
            }
        } else if let Some(rest) = stat.name.strip_prefix("inbound>>>") {
            if let Some(tag) = rest.strip_suffix(INBOUND_UP).filter(|tag| tag.starts_with("proxy-")) {
                inbounds.entry(tag.to_owned()).or_default().uplink = stat.value;
            } else if let Some(tag) = rest.strip_suffix(INBOUND_DOWN).filter(|tag| tag.starts_with("proxy-"))
            {
                inbounds.entry(tag.to_owned()).or_default().downlink = stat.value;
            }
        }
    }
    Ok(Snapshot { users, inbounds })
}
