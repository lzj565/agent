//! Reads sing-box's local V2Ray-compatible StatsService without resetting its
//! lifetime counters. The hub owns all accumulation and reset semantics.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::Serialize;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tonic_prost::ProstCodec;

const QUERY_STATS: &str = "/v2ray.core.app.stats.command.StatsService/QueryStats";
const INBOUND_UP: &str = ">>>traffic>>>uplink";
const INBOUND_DOWN: &str = ">>>traffic>>>downlink";
const QUERY_TIMEOUT: Duration = Duration::from_secs(3);
const INTERVAL: Duration = Duration::from_secs(5);

/// One query at a time, with at most one queued result. Dropping a session
/// cancels its worker, so reconnecting never accumulates polling loops.
pub struct Worker {
    results: tokio::sync::mpsc::Receiver<Result<Snapshot>>,
    task: tokio::task::JoinHandle<()>,
}

impl Worker {
    pub fn new(port: u16) -> Self {
        let collector = Collector::new(port);
        let (tx, results) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let reading = tokio::time::timeout(QUERY_TIMEOUT, collector.collect())
                    .await
                    .unwrap_or_else(|_| Err(anyhow!("query timed out after 3s")));
                if tx.send(reading).await.is_err() {
                    return;
                }
            }
        });
        Self { results, task }
    }

    pub async fn recv(&mut self) -> Option<Result<Snapshot>> {
        self.results.recv().await
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

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

impl Collector {
    pub fn new(port: u16) -> Self {
        let endpoint = Endpoint::from_shared(endpoint_uri(port)).expect("validated API port");
        Self { channel: endpoint.connect_lazy() }
    }

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

fn endpoint_uri(port: u16) -> String {
    format!("http://{}", crate::api_config::listen_address(port))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_uses_the_configured_local_api_port() {
        assert_eq!(endpoint_uri(9002), "http://127.0.0.1:9002");
        assert_eq!(endpoint_uri(9005), "http://127.0.0.1:9005");
    }

    #[tokio::test]
    async fn a_stalled_api_times_out_and_dropping_the_worker_stops_polling() {
        let api = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut worker = Worker::new(api.local_addr().unwrap().port());
        let (_stalled, _) = api.accept().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(4), worker.recv()).await.unwrap().unwrap();
        assert!(result.unwrap_err().to_string().contains("timed out after 3s"));
        let task = worker.task.abort_handle();
        drop(worker);
        tokio::task::yield_now().await;
        assert!(task.is_finished(), "session drop must not leave a traffic worker running");
    }
}
