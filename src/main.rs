//! monitor-agent: reports one Linux host to a monitor hub over WebSocket.

mod api_config;
mod collect;
mod commands;
mod components;
mod metric_worker;
mod proxy_traffic;
mod singbox;

use std::sync::Arc;
use std::time::Duration;

// Shares the clock `tokio::time::timeout` and `sleep` read, so deadline
// arithmetic cannot drift from the timers enforcing it, and tests can advance
// it. Outside a paused runtime this is the monotonic clock.
use tokio::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{Sink, SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use collect::{Collector, Facts};
use singbox::Manager;

/// Small installer-only commands. Keeping JSON and `/proc` handling in this
/// binary avoids adding jq, Python, or other runtime dependencies to nodes.
fn internal_command(args: &[String]) -> Option<i32> {
    if args.first().map(String::as_str) != Some("internal") {
        return None;
    }
    let result = match args.get(1).map(String::as_str) {
        Some("api-port-from-config") if args.len() == 3 => {
            match std::fs::read_to_string(&args[2])
                .map_err(|e| e.to_string())
                .and_then(|content| api_config::port_from_config(&content))
            {
                Ok(Some(port)) => {
                    println!("{port}");
                    return Some(0);
                }
                Ok(None) => return Some(3),
                Err(error) => Err(error),
            }
        }
        Some("set-api-port-in-config") if args.len() == 4 => {
            let port = api_config::parse_port(&args[3]).ok_or_else(|| "invalid API port".to_owned());
            port.and_then(|port| api_config::set_config_file_port(std::path::Path::new(&args[2]), port))
        }
        Some("tcp-port-listening") if args.len() == 3 => {
            let port = api_config::parse_port(&args[2]).ok_or_else(|| "invalid API port".to_owned());
            match port.and_then(api_config::tcp_port_listening) {
                Ok(true) => return Some(0),
                Ok(false) => return Some(1),
                Err(error) => Err(error),
            }
        }
        _ => Err("invalid internal command".to_owned()),
    };
    match result {
        Ok(()) => Some(0),
        Err(error) => {
            eprintln!("monitor-agent internal command: {error}");
            Some(2)
        }
    }
}

struct Args {
    server: String,
    token: String,
    interval: u64,
    ifaces: collect::Ifaces,
    /// Permits plain HTTP to a hub reached at ip:port with no TLS in front.
    /// Off by default: the token would otherwise travel in the clear.
    insecure: bool,
}

fn usage() -> ! {
    eprintln!(
        "monitor-agent {}\n\n\
         Usage: monitor-agent --server <url> --token <token> [options]\n\n\
         Options:\n  \
           --server <url>       Hub base URL, e.g. https://hub.example.com\n  \
           --token <token>      Node token from the hub panel\n  \
           --interval <secs>    Report interval (default 1)\n  \
           --iface <list>       Count traffic on these interfaces alone, e.g.\n                       \
                                eth1,pppoe-wan. `-name` removes an interface\n                       \
                                from what would be counted. Full names only.\n  \
           --insecure           Allow plain ws:// to a remote hub; the token\n                       \
                                travels in the clear. Only for a hub reached\n                       \
                                at ip:port with no TLS in front.\n",
        env!("CARGO_PKG_VERSION")
    );
    std::process::exit(2)
}

fn parse_args() -> Result<Args> {
    let (mut server, mut token, mut interval, mut iface, mut insecure) = (None, None, 1u64, None, false);
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--server" => server = Some(value()),
            "--token" => token = Some(value()),
            "--interval" => interval = value().parse().unwrap_or_else(|_| usage()),
            "--iface" => iface = Some(value()),
            "--insecure" => insecure = true,
            "-h" | "--help" => usage(),
            other => bail!("unknown argument: {other}"),
        }
    }
    let server = server.or_else(|| std::env::var("MONITOR_SERVER").ok()).unwrap_or_else(|| usage());
    let token = token.or_else(|| std::env::var("MONITOR_TOKEN").ok()).unwrap_or_else(|| usage());
    let iface = iface.or_else(|| std::env::var("MONITOR_IFACE").ok()).unwrap_or_default();
    let ifaces = collect::Ifaces::parse(&iface).map_err(anyhow::Error::msg)?;
    Ok(Args { server, token, interval: interval.clamp(1, 3600), ifaces, insecure })
}

/// `https://host/path` -> `wss://host/path/api/agent/ws`. The token travels in
/// an Authorization header rather than the query string, keeping it out of
/// reverse-proxy access logs.
///
/// `insecure` declares that the hub has no TLS. It permits plain ws:// to a
/// remote hub and suppresses the bare-host upgrade to TLS; without the latter
/// the flag would dial a port that cannot complete a TLS handshake.
fn ws_url(server: &str, insecure: bool) -> Result<String> {
    let base = server.trim_end_matches('/');
    let scheme = if insecure { "ws" } else { "wss" };
    // Schemes are case-insensitive (RFC 3986), and a phone keyboard capitalises
    // a hand-typed `Https://`; read as a bare host it would never resolve.
    let (given, rest) = base.split_once("://").unwrap_or(("", base));
    let base = match given.to_ascii_lowercase().as_str() {
        "https" => format!("wss://{rest}"),
        "http" => format!("ws://{rest}"),
        ws @ ("wss" | "ws") => format!("{ws}://{rest}"),
        _ => format!("{scheme}://{base}"),
    };
    // RFC 3986 places userinfo before the host, so `127.0.0.1:28080@evil.example.com`
    // reads as loopback to any check that splits at the first colon while the
    // connection goes to the name following it -- bypassing both the refusal below
    // and `--insecure`. A hub address never needs userinfo, so it is rejected.
    let authority = base.split("://").nth(1).unwrap_or("").split('/').next().unwrap_or("");
    if authority.contains('@') {
        bail!("server URL must not contain '@': the host is whatever follows it, not what precedes it");
    }
    if base.starts_with("ws://") && !insecure && !is_loopback(&base) {
        bail!(
            "refusing plaintext ws:// to a remote hub; the token would travel in the clear. \
             Pass --insecure if that hub really has no TLS"
        );
    }
    Ok(format!("{base}/api/agent/ws"))
}

/// Parses the host rather than prefix-matching it: `127.attacker.example`
/// begins with the loopback net but resolves elsewhere. IPv6 literals are
/// bracketed, so the port is not split off at the first colon. Anything that is
/// not a literal loopback address falls to the plaintext refusal, including
/// `::ffff:127.0.0.1`.
///
/// `ws_url` has already rejected an authority containing `@`, so the first
/// colon here is the port separator.
fn is_loopback(url: &str) -> bool {
    let authority = url.split("://").nth(1).unwrap_or("").split('/').next().unwrap_or("");
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => authority.split(':').next().unwrap_or(""),
    };
    host.parse::<std::net::IpAddr>().map_or(host == "localhost", |ip| ip.is_loopback())
}

#[derive(Deserialize)]
struct Rpc {
    method: String,
    #[serde(default)]
    params: serde_json::Value,
}

#[derive(Deserialize, Clone)]
struct PingTask {
    id: i64,
    target: String,
    interval: u64,
}

fn notify(method: &str, params: serde_json::Value) -> Message {
    Message::Text(
        serde_json::json!({"jsonrpc": "2.0", "method": method, "params": params}).to_string().into(),
    )
}

/// Writes under a deadline drawn from the remaining silence budget.
///
/// Reads and writes share one `select!` loop, so a socket that never drains
/// also stalls the watchdog; the kernel abandons such a socket only after
/// tcp_retries2, roughly fifteen minutes.
///
/// Charging the write against the remaining budget rather than a fresh
/// [`HUB_SILENCE`] bounds a stall at the end of a quiet stretch: two full
/// budgets would exceed the 120s after which the hub drops the node. An
/// exhausted budget fails the write and ends the session, as the watchdog
/// would have.
///
/// A timed-out write leaves a partial frame in the stream; every caller ends
/// the session on the error, discarding it with the socket.
async fn send(
    ws: &mut (impl Sink<Message, Error = WsError> + Unpin),
    m: Message,
    budget: Duration,
) -> Result<()> {
    tokio::time::timeout(budget, ws.send(m))
        .await
        .map_err(|_| anyhow!("write stalled for {}s", budget.as_secs()))?
        .context("write")
}

/// An error and its causes joined by `: ` as `{:#}` prints them, skipping a
/// cause whose text the line already ends with. Tungstenite folds its source
/// into its own message, so `{:#}` alone would print the cause twice:
/// `IO error: Connection reset by peer (os error 104): Connection reset by peer (os error 104)`.
fn causes(e: &anyhow::Error) -> String {
    let mut line = String::new();
    for cause in e.chain().map(ToString::to_string) {
        if line.ends_with(&cause) {
            continue;
        }
        if !line.is_empty() {
            line.push_str(": ");
        }
        line.push_str(&cause);
    }
    line
}

/// Remaining silence budget, measured from the last sign of life.
///
/// Every write draws from this single window, so no sequence of writes can
/// push the give-up point beyond one [`HUB_SILENCE`] past the last frame.
fn remaining(last_frame: Instant) -> Duration {
    HUB_SILENCE.saturating_sub(last_frame.elapsed())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(code) = internal_command(&cli) {
        std::process::exit(code);
    }
    let args = parse_args()?;
    let api_port = api_config::port_from_env().map_err(anyhow::Error::msg)?;
    let url = ws_url(&args.server, args.insecure)?;
    // Reported once at startup. install.sh hardens this unit with
    // ProtectHome=yes and a user of its own, which keep some disks out of the
    // totals. The unit file owns that decision, but the discrepancy must not go
    // unreported. PID 1's table names every disk the sandbox hides; where /proc
    // hides PID 1, this namespace's own table still shows a covered /home.
    let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap_or_default();
    match std::fs::read_to_string("/proc/1/mounts") {
        Ok(host) => {
            for mount in collect::hidden_mounts(&host, &mounts) {
                eprintln!("{mount} is not visible to this service and is not counted toward disk totals");
            }
        }
        Err(_) => {
            for mount in collect::shadowed_mounts(&mounts) {
                eprintln!("{mount} is covered by another mount and is not counted toward disk totals");
            }
        }
    }
    for (mount, e) in collect::unreadable_mounts(&mounts) {
        eprintln!("{mount} cannot be read ({e}) and is not counted toward disk totals");
    }
    let collector = Collector::new(args.ifaces);
    // Reported once at startup: an interface summed twice otherwise shows only
    // as a total twice the real one. One listed in --iface but absent here,
    // such as a PPPoE link not yet dialled, is counted once it appears.
    let counted = collector.counted_ifaces();
    eprintln!(
        "counting traffic on: {}",
        if counted.is_empty() { "none".to_owned() } else { counted.join(" ") }
    );
    let collector = metric_worker::Worker::new(collector)?;
    let mut wait = 0u64;
    let manager = Manager::with_api_port(api_port);
    let command_slots = Arc::new(Semaphore::new(commands::MAX_RUNNING));

    loop {
        // Set by `session` once the handshake completes, so a connect that
        // never finished cannot pass its CONNECT_DEADLINE off as a session
        // that ran. `None` means never connected, which keeps the backoff
        // doubling.
        let mut connected = None;
        if let Err(e) = session(
            &url,
            &args.token,
            &collector,
            args.interval,
            &mut connected,
            &manager,
            command_slots.clone(),
        )
        .await
        {
            eprintln!("session ended: {}", causes(&e));
        }
        wait = reconnect_wait(wait, connected.map_or(Duration::ZERO, |t: Instant| t.elapsed()));
        tokio::time::sleep(Duration::from_millis(rand::random_range(reconnect_jitter(wait)))).await;
    }
}

/// Backoff before the next connection attempt, derived from the previous wait
/// and the duration of the session that just ended -- measured from the
/// handshake, so a peer that swallows a connect for the whole
/// [`CONNECT_DEADLINE`] earns no credit.
///
/// A session that reported for a while proves the hub reachable and the token
/// valid, so the wait resets to one second. Only short-lived sessions keep
/// doubling, which keeps an agent off a hub in a crash loop.
fn reconnect_wait(previous: u64, lasted: Duration) -> u64 {
    if lasted >= Duration::from_secs(30) {
        1
    } else {
        (previous * 2).clamp(1, 60)
    }
}

/// Keep the exponential base separate from randomness. Applying jitter to
/// the previous random delay would distort the backoff on subsequent retries.
/// Millisecond precision spreads even the first reconnect after a hub restart;
/// the capped retry still varies from 45 to 60 seconds instead of synchronising
/// every node at exactly the ceiling.
fn reconnect_jitter(base: u64) -> std::ops::RangeInclusive<u64> {
    let millis = base.clamp(1, 60) * 1000;
    (millis * 3 / 4).max(1000)..=(millis * 5 / 4).min(60_000)
}

/// Deadline covering every stage of establishing a connection: resolution,
/// the TCP handshake, the TLS exchange and the HTTP upgrade.
///
/// Only the TCP handshake has a deadline of its own; the TLS exchange and the
/// HTTP upgrade have none, so a peer that accepts and then goes silent would
/// leave the handshake pending indefinitely, and the agent running without
/// reporting or logging.
///
/// Deliberately generous: a healthy connect takes a quarter of a second, the
/// slowest measured sixty. This is not a latency budget but the point past
/// which nothing is expected to arrive.
const CONNECT_DEADLINE: Duration = Duration::from_secs(120);

/// How long one of the hub's addresses may take to accept a TCP connection
/// while another remains to be tried: the first SYN and its retransmits at one
/// and three seconds. The last address has no limit of its own and is bounded
/// by [`CONNECT_DEADLINE`] alone, so a hub with a single slow address is
/// reached as before.
///
/// Without it a black-holed address -- typically an AAAA record over a v6
/// route that leads nowhere -- holds the connect for the kernel's 127 seconds
/// of SYN retries, beyond CONNECT_DEADLINE, and the address behind it is never
/// tried.
const DIAL_FALLBACK: Duration = Duration::from_secs(5);

/// The hub sends one kind of message, a probe list a few hundred bytes long.
/// Tungstenite's 64 MiB default would hand the peer this process's entire
/// memory budget.
// A 1 MiB UTF-8 config can expand to six JSON bytes per input byte.
const MAX_MESSAGE: usize = 6 * 1024 * 1024 + 16 * 1024;

/// How long the agent waits for any frame from the hub before giving up.
///
/// The hub pings every 30 seconds and drops an agent silent for 120. Without a
/// matching watchdog, a one-way path failure -- an expired NAT entry, a route
/// gone dark -- leaves the agent writing into a socket the kernel retransmits
/// on for fifteen minutes, long after the panel has marked the node offline.
///
/// Staying under the hub's own timeout makes the agent give up first, bounding
/// recovery at this constant rather than at tcp_retries2.
const HUB_SILENCE: Duration = Duration::from_secs(90);

/// How often a session collects its facts again; a change goes out as another
/// hello. An address added while connected, or one that arrived after the
/// boot-time hello, would otherwise wait for the next reconnect, weeks away on
/// a steady link. The hub writes at most one hello per half of this and holds
/// the latest of any that arrive sooner, so none needs sending twice.
const FACTS_RECHECK: Duration = Duration::from_secs(60);

/// One connection: handshake, then report until the socket closes.
async fn session(
    url: &str,
    token: &str,
    collector: &metric_worker::Worker,
    interval: u64,
    connected: &mut Option<Instant>,
    manager: &Manager,
    command_slots: Arc<Semaphore>,
) -> Result<()> {
    let request = authorized(url, token)?;
    let config =
        WebSocketConfig::default().max_message_size(Some(MAX_MESSAGE)).max_frame_size(Some(MAX_MESSAGE));
    let uri = request.uri();
    // Brackets off an IPv6 literal, which `lookup_host` parses bare.
    let host = uri
        .host()
        .context("server URL has no host")?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("wss") { 443 } else { 80 });
    // Collected before dialing, as the v4 it reports decides which family is
    // tried first.
    let mut facts = collector.facts().await?;
    let behind_nat = facts.ipv4.parse().is_ok_and(|ip| !collect::is_public(ip));
    let connect = async {
        let stream = dial(&host, port, behind_nat).await?;
        let peer = stream.peer_addr()?;
        let (ws, _) = tokio_tungstenite::client_async_tls_with_config(request, stream, Some(config), None)
            .await
            .context("handshake")?;
        anyhow::Ok((ws, peer))
    };
    let (mut ws, peer) = tokio::time::timeout(CONNECT_DEADLINE, connect)
        .await
        .map_err(|_| anyhow!("no connection after {}s", CONNECT_DEADLINE.as_secs()))??;
    eprintln!("connected to {peer}");
    *connected = Some(Instant::now());
    let glimpse_from = |facts: &Facts| {
        if let Some(v6) = glimpse_family(peer.is_ipv6(), &facts.ipv4, &facts.ipv6) {
            tokio::spawn(glimpse(url.to_owned(), token.to_owned(), host.clone(), port, v6));
        }
    };
    glimpse_from(&facts);
    // The clock starts at the handshake and the hello below draws from it like
    // every other write, so no two writes can each claim a full HUB_SILENCE.
    let mut last_frame = Instant::now();

    let mut hello = serde_json::to_value(&facts)?;
    hello["capabilities"] = serde_json::json!(["components.v1"]);
    for (key, value) in collector.capacities().await? {
        hello[key] = value.into();
    }
    let hello_capabilities = hello["capabilities"].clone();
    send(&mut ws, notify("hello", hello), remaining(last_frame)).await?;

    let (result_tx, mut result_rx) = mpsc::channel::<Message>(64);
    let mut commands = commands::CommandQueue::new(command_slots, manager.clone(), result_tx.clone());
    let mut ping_tasks: Vec<(PingTask, tokio::task::JoinHandle<()>)> = Vec::new();
    // The baseline is taken a second before the first report, which then
    // measures CPU and network over that second rather than over nothing --
    // all a freshly started process has -- or over the outage since the
    // previous session.
    let mut ticker =
        tokio::time::interval_at(Instant::now() + Duration::from_secs(1), Duration::from_secs(interval));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let sampling = collector.collect();
    tokio::pin!(sampling);
    let mut collecting = true;
    let mut baseline = true;
    let mut traffic = proxy_traffic::Worker::new(manager.api_port());
    let mut traffic_error_reported = false;
    let mut components = components::Worker::new();
    let mut recheck = tokio::time::interval_at(Instant::now() + FACTS_RECHECK, FACTS_RECHECK);
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut refreshing = false;
    let mut refresh = std::pin::pin!(collector.facts());

    let result = loop {
        tokio::select! {
            _ = commands.start_next(), if commands.has_pending() => {}
            Some(snapshot) = components.recv() => {
                if let Err(e)=send(&mut ws,notify("components.status",snapshot),remaining(last_frame)).await {break Err(e)}
            }
            _ = ticker.tick(), if !collecting => {
                collecting = true;
            }
            reading = &mut sampling, if collecting => {
                let reading = match reading {
                    Ok(reading) => reading,
                    Err(e) => break Err(e),
                };
                if baseline {
                    baseline = false;
                    collecting = false;
                    sampling.set(collector.collect());
                    ticker.reset_after(Duration::from_secs(1));
                    continue;
                }
                let m = serde_json::to_value(reading)?;
                collecting = false;
                sampling.set(collector.collect());
                if let Err(e) = send(&mut ws, notify("report", m), remaining(last_frame)).await { break Err(e); }
            }
            Some(reading) = traffic.recv() => {
                match reading {
                    Ok(snapshot) => {
                        traffic_error_reported = false;
                        if let Err(e) = send(&mut ws, notify("proxy.traffic", serde_json::to_value(snapshot)?), remaining(last_frame)).await { break Err(e); }
                    }
                    Err(e) if !traffic_error_reported => {
                        eprintln!("sing-box traffic stats unavailable: {e:#}");
                        traffic_error_reported = true;
                    }
                    _ => {}
                }
            }
            _ = recheck.tick(), if !refreshing => { refreshing = true; }
            fresh = &mut refresh, if refreshing => {
                let fresh = fresh?;
                refreshing = false;
                refresh.set(collector.facts());
                if fresh == facts {
                    continue;
                }
                if (&fresh.ipv4, &fresh.ipv6) != (&facts.ipv4, &facts.ipv6) {
                    glimpse_from(&fresh);
                }
                let mut updated = serde_json::to_value(&fresh)?;
                updated["capabilities"] = hello_capabilities.clone();
                let hello = notify("hello", updated);
                if let Err(e) = send(&mut ws, hello, remaining(last_frame)).await { break Err(e); }
                facts = fresh;
            }
            // Rebuilt each pass from the last frame, so silence costs exactly
            // HUB_SILENCE rather than a polling interval more. Kept separate
            // from the report tick, which --interval can stretch to an hour.
            _ = tokio::time::sleep(remaining(last_frame)) => {
                break Err(anyhow!("no frame from the hub in {}s", HUB_SILENCE.as_secs()));
            }
            Some(msg) = result_rx.recv() => {
                if let Err(e) = send(&mut ws, msg, remaining(last_frame)).await { break Err(e); }
            }
            incoming = ws.next() => {
                // Any frame proves the path alive, including the hub's
                // heartbeat ping -- the only one on an otherwise idle link.
                last_frame = Instant::now();
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                            if value.get("type").and_then(serde_json::Value::as_str) == Some("command") {
                                if let Err(response) = commands.submit(value) {
                                    if let Err(e) = send(&mut ws, Message::Text(response.to_string().into()), remaining(last_frame)).await {
                                        break Err(e);
                                    }
                                }
                                continue;
                            }
                        }
                        if let Ok(rpc) = serde_json::from_str::<Rpc>(&text) {
                            if rpc.method == "ping.tasks" {
                                if let Ok(tasks) = serde_json::from_value::<Vec<PingTask>>(rpc.params) {
                                    respawn_ping_tasks(&mut ping_tasks, tasks, &result_tx);
                                }
                            }
                        }
                    }
                    // Ping included: tungstenite queues the pong itself and
                    // sends it on the next read; a manual reply would duplicate it.
                    Some(Ok(Message::Close(_))) => break Ok(()),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => break Err(e.into()),
                    None => break Ok(()),
                }
            }
        }
    };

    for (_, handle) in ping_tasks {
        handle.abort();
    }
    result
}

async fn command_result(manager: &Manager, command: &serde_json::Value) -> serde_json::Value {
    let id = command.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let action = command.get("action").and_then(serde_json::Value::as_str).unwrap_or("");
    let params = command.get("params").unwrap_or(&serde_json::Value::Null);
    let outcome = manager.execute(action, params).await;
    match outcome {
        Ok(result) => serde_json::json!({"type":"command_result", "id":id, "ok":true, "result":result}),
        Err(message) => {
            let code = if matches!(
                action,
                "singbox.status"
                    | "singbox.config.get"
                    | "singbox.config.check"
                    | "singbox.config.apply"
                    | "singbox.restart"
            ) {
                "SINGBOX_ERROR"
            } else {
                "UNKNOWN_ACTION"
            };
            serde_json::json!({"type":"command_result", "id":id, "ok":false, "error":{"code":code, "message":message}})
        }
    }
}

/// The upgrade request for `url`, carrying the node's token.
fn authorized(url: &str, token: &str) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().context("token is not header-safe")?);
    Ok(request)
}

/// Which family a second connection is wanted in, as `v6`, given the one the
/// session runs over and the addresses the interfaces hold. The session shows
/// the hub one exit; for the other family it is wanted only where the interface
/// holds a private address of it -- NAT66, a ULA behind a masquerading host --
/// since a public one is reported as it is and none means nothing to connect
/// from.
fn glimpse_family(session_v6: bool, ipv4: &str, ipv6: &str) -> Option<bool> {
    let held = if session_v6 { ipv4 } else { ipv6 };
    held.parse::<std::net::IpAddr>().is_ok_and(|ip| !collect::is_public(ip)).then_some(!session_v6)
}

/// How long one attempt of [`glimpse`] may take. The session does not wait on
/// it, so a path that cannot complete is abandoned well before
/// [`CONNECT_DEADLINE`].
const GLIMPSE_DEADLINE: Duration = Duration::from_secs(15);

/// Attempts of [`glimpse`], and the wait between them. A session on a NAT66
/// node can run for weeks, so one lost SYN or failed lookup would leave the hub
/// without the exit until the next reconnect.
const GLIMPSE_TRIES: u32 = 3;
const GLIMPSE_RETRY: Duration = Duration::from_secs(30);

/// How an attempt of [`glimpse`] ended once a connection was possible.
enum Glimpse {
    /// The hub took the connection, or has no address in that family.
    Done,
    /// The hub answered the handshake with a refusal: an older hub without the
    /// route (404), or a proxy or firewall in front of it (403). Retrying
    /// changes nothing.
    Refused(u16),
}

/// The hub's echo route, from the session's URL.
fn echo_url(session: &str) -> Result<String> {
    let base = session.strip_suffix("/ws").context("server URL is not a session route")?;
    Ok(format!("{base}/echo"))
}

/// Connects to the hub's echo route once over one address family, `v6` or v4,
/// and drops the connection once it is upgraded. The hub learns the address this
/// host exits at in that family from where the connection came, which an
/// interface holding only a private address cannot tell it. Nothing is
/// exchanged. Failures are logged and otherwise ignored: the session does not
/// depend on this one.
async fn glimpse(url: String, token: String, host: String, port: u16, v6: bool) {
    let family = if v6 { "IPv6" } else { "IPv4" };
    for attempt in 1..=GLIMPSE_TRIES {
        match tokio::time::timeout(GLIMPSE_DEADLINE, echo_once(&url, &token, &host, port, v6)).await {
            Ok(Ok(Glimpse::Done)) => return,
            Ok(Ok(Glimpse::Refused(status))) => {
                eprintln!("the hub answered {status} on its echo route over {family}; its exit there is not recorded");
                return;
            }
            Ok(Err(e)) => eprintln!("the hub could not be reached over {family}: {}", causes(&e)),
            Err(_) => eprintln!("the hub could not be reached over {family}: no answer"),
        }
        if attempt < GLIMPSE_TRIES {
            tokio::time::sleep(GLIMPSE_RETRY).await;
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn command_keeps_id_and_reports_unknown_action() {
    let result = command_result(
        &Manager::default(),
        &serde_json::json!({
            "type": "command", "id": "request-123", "action": "singbox.unknown", "params": {}
        }),
    )
    .await;
    assert_eq!(result["id"], "request-123");
    assert_eq!(result["ok"], false);
    assert_eq!(result["error"]["code"], "UNKNOWN_ACTION");
}

async fn echo_once(url: &str, token: &str, host: &str, port: u16, v6: bool) -> Result<Glimpse> {
    let addrs: Vec<_> = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolve {host}"))?
        .filter(|a| a.is_ipv6() == v6)
        .collect();
    if addrs.is_empty() {
        return Ok(Glimpse::Done);
    }
    let stream = connect_first(&addrs).await?;
    let request = authorized(&echo_url(url)?, token)?;
    match tokio_tungstenite::client_async_tls_with_config(request, stream, None, None).await {
        Ok(_) => Ok(Glimpse::Done),
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            Ok(Glimpse::Refused(response.status().as_u16()))
        }
        Err(e) => Err(anyhow::Error::new(e).context("handshake")),
    }
}

/// Opens the TCP connection to the hub, trying its addresses in turn.
///
/// A host whose IPv4 is private tries the hub's IPv4 addresses first. Its
/// public IPv4 exists only on the NAT in front of it, and the hub, which knows
/// no more than the address a connection arrives from, learns it only from a
/// connection made over v4. A public v6 needs no such route: it sits on the
/// interface and travels in the hello. Every other host keeps the resolver's
/// order.
async fn dial(host: &str, port: u16, prefer_v4: bool) -> Result<TcpStream> {
    let mut addrs: Vec<std::net::SocketAddr> =
        tokio::net::lookup_host((host, port)).await.with_context(|| format!("resolve {host}"))?.collect();
    if prefer_v4 {
        addrs.sort_by_key(|a| !a.is_ipv4());
    }
    // Each failure already names its address, the host as resolved.
    connect_first(&addrs).await.context("connect")
}

/// The first of `addrs` to accept, each but the last given [`DIAL_FALLBACK`].
/// Every failure is kept, so the log names which family failed and how.
async fn connect_first(addrs: &[std::net::SocketAddr]) -> Result<TcpStream> {
    let mut failures = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let attempt = TcpStream::connect(addr);
        let result = if i + 1 < addrs.len() {
            tokio::time::timeout(DIAL_FALLBACK, attempt)
                .await
                .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()))
        } else {
            attempt.await
        };
        match result {
            Ok(stream) => return Ok(stream),
            Err(e) => failures.push(format!("{addr}: {e}")),
        }
    }
    if failures.is_empty() {
        bail!("no address");
    }
    bail!("{}", failures.join("; "))
}

/// Ceiling on concurrent probe loops.
///
/// A task serialises to about forty bytes, so one [`MAX_MESSAGE`] frame could
/// request some fifteen hundred; at the five-second interval floor that is
/// hundreds of outbound connects per second to hub-chosen addresses, which on
/// a shared VPS reads as a port scan and acts as an amplifier. A compromised
/// or merely buggy hub is within the threat model, so the list is bounded
/// rather than trusted.
const MAX_PING_TASKS: usize = 64;

/// Replaces the running probe loops with the hub's current task list, leaving
/// unchanged tasks in place so their timers survive a push.
fn respawn_ping_tasks(
    running: &mut Vec<(PingTask, tokio::task::JoinHandle<()>)>,
    mut wanted: Vec<PingTask>,
    tx: &mpsc::Sender<Message>,
) {
    if wanted.len() > MAX_PING_TASKS {
        // Silent truncation would leave no record of which probes run.
        eprintln!("hub asked for {} ping tasks, running {MAX_PING_TASKS}", wanted.len());
        wanted.truncate(MAX_PING_TASKS);
    }
    running.retain(|(task, handle)| {
        let keep =
            wanted.iter().any(|w| w.id == task.id && w.target == task.target && w.interval == task.interval);
        if !keep {
            handle.abort();
        }
        keep
    });
    for task in wanted {
        if running.iter().any(|(t, _)| t.id == task.id) {
            continue;
        }
        let (tx, spawned) = (tx.clone(), task.clone());
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(spawned.interval.clamp(5, 3600)));
            // As with the report ticker, missed ticks must not fire back to
            // back: the default burst behaviour would turn one stalled
            // resolution into a rapid series of connects.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Logged once per probe per session: a resolver that overruns does
            // so every round, and the resulting gap would otherwise be
            // unexplained.
            let mut said = false;
            loop {
                ticker.tick().await;
                let Some(latency) = tcp_ping(&spawned.target).await else {
                    if !std::mem::replace(&mut said, true) {
                        eprintln!(
                            "{}: name resolution runs past {}ms, so these rounds report no sample \
                             rather than a loss",
                            spawned.target,
                            HANDSHAKE_DEADLINE.as_millis()
                        );
                    }
                    continue;
                };
                let msg =
                    notify("ping.result", serde_json::json!({"task_id": spawned.id, "latency_ms": latency}));
                if tx.send(msg).await.is_err() {
                    return;
                }
            }
        });
        running.push((task, handle));
    }
}

/// Deadline for one handshake, deliberately under the kernel's first SYN
/// retransmit.
///
/// Linux arms its initial SYN timer at one second. A longer wait turns a
/// dropped SYN into a late success, reporting the retransmit timer plus the
/// round trip as latency.
///
/// Cutting it short guarantees that every reading belongs to a handshake
/// completed on the first SYN, and that a dropped one becomes -1. The cost is
/// that a link whose genuine round trip exceeds this reads as unreachable.
const HANDSHAKE_DEADLINE: Duration = Duration::from_millis(900);

/// How many of a name's addresses one probe attempts.
///
/// Each dead address costs a [`HANDSHAKE_DEADLINE`], and a probe must not
/// outlast the five-second floor on its own interval.
const MAX_PING_ADDRS: usize = 3;

/// Round-trip time of a TCP handshake in milliseconds; -1 when no address
/// answered within [`HANDSHAKE_DEADLINE`], `None` when the name could not be
/// resolved in that time.
///
/// The two failure modes must stay distinct. -1 is the protocol's word for a
/// target that did not answer, and the hub folds every negative reading into a
/// bucket's packet loss, so returning it for a slow resolver would draw loss on
/// a link that dropped nothing. An overrun resolution is a sample not taken,
/// which is not a reading of zero.
///
/// The name is resolved before the clock starts: `TcpStream::connect` on a
/// hostname resolves first and connects second, which would fold resolver
/// latency into every sample. musl, which the release binaries link, keeps no
/// cache, so this happens each round.
///
/// ponytail: the `None` arm has no runtime reproduction; forcing it would
/// require either a genuinely overrunning resolver or a test-only deadline
/// parameter. The assertions below spell `Some(-1)`, so collapsing the two
/// answers back into one `i32` fails to compile.
async fn tcp_ping(target: &str) -> Option<i32> {
    // Bounded by the handshake deadline: a resolution slower than a connect is
    // useless as a latency sample, and `lookup_host` has no deadline of its own
    // -- musl waits five seconds on a black-holed nameserver.
    //
    // This does not cancel the underlying `getaddrinfo`, which runs to
    // completion on a blocking thread; it only keeps this probe on cadence.
    let Ok(resolved) = tokio::time::timeout(HANDSHAKE_DEADLINE, tokio::net::lookup_host(target)).await else {
        return None;
    };
    // A resolution error is an unreachable target, which is what -1 reports.
    // Only the deadline above is ambiguous.
    let Ok(addresses) = resolved else { return Some(-1) };
    Some(handshake(addresses).await)
}

/// Round-trip time of the first address that completes a handshake.
///
/// The clock restarts on each address, so a dead one contributes nothing;
/// summing them would report the accumulated wait as latency.
///
/// Every failure advances to the next address, refusals included. The
/// resolver lists v6 first wherever the host has a v6 route, so stopping at
/// the first failure would report a target reachable over v4 as down for as
/// long as that route leads nowhere.
async fn handshake(addresses: impl Iterator<Item = std::net::SocketAddr>) -> i32 {
    for address in addresses.take(MAX_PING_ADDRS) {
        let started = std::time::Instant::now();
        if let Ok(Ok(_)) = tokio::time::timeout(HANDSHAKE_DEADLINE, TcpStream::connect(address)).await {
            return started.elapsed().as_millis().min(i32::MAX as u128) as i32;
        }
    }
    -1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn slow_metrics_leave_the_websocket_responsive_without_starting_more_samples() {
        struct SlowMetrics {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            calls: Arc<std::sync::atomic::AtomicU32>,
        }
        impl metric_worker::Source for SlowMetrics {
            fn facts(&self) -> collect::Facts {
                Collector::default().facts()
            }
            fn collect(&mut self) -> collect::Metrics {
                let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                    let _ = self.release.recv();
                }
                collect::Metrics { procs: n, ..collect::Metrics::default() }
            }
        }
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let worker = metric_worker::Worker::new(SlowMetrics {
            entered: Some(entered),
            release: wait,
            calls: calls.clone(),
        })
        .unwrap();
        let hub = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/api/agent/ws", hub.local_addr().unwrap());
        let clone = worker.clone();
        let client = tokio::spawn(async move {
            session(
                &url,
                "test-token",
                &clone,
                1,
                &mut None,
                &Manager::default(),
                Arc::new(Semaphore::new(commands::MAX_RUNNING)),
            )
            .await
        });
        let (stream, _) = hub.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), started).await.unwrap().unwrap();
        ws.send(Message::Ping(vec![9].into())).await.unwrap();
        ws.send(Message::Text(
            serde_json::json!({"type": "command", "id": "slow-metrics",
            "action": "unknown"})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_millis(500), async {
            let (mut pong, mut response) = (false, false);
            while !pong || !response {
                match ws.next().await.unwrap().unwrap() {
                    Message::Pong(bytes) => pong = bytes.as_ref() == [9],
                    Message::Text(text) => {
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        response |= value["id"] == "slow-metrics";
                        assert_ne!(value["method"], "report", "sample is still blocked");
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("blocking metric I/O must not block network messages");
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Message::Text(text) = ws.next().await.unwrap().unwrap() {
                    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if value["method"] == "report" {
                        assert_eq!(value["params"]["procs"], 2);
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
        ws.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), client).await.unwrap().unwrap().unwrap();
        assert!(worker.collect().await.unwrap().procs >= 2, "worker state survives the session");
    }

    #[tokio::test]
    async fn a_stalled_stats_api_does_not_delay_reports_pongs_or_commands() {
        let hub = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/api/agent/ws", hub.local_addr().unwrap());
        let manager = Manager::with_api_port(api.local_addr().unwrap().port());
        let client = tokio::spawn(async move {
            session(
                &url,
                "test-token",
                &metric_worker::Worker::new(Collector::default()).unwrap(),
                1,
                &mut None,
                &manager,
                Arc::new(Semaphore::new(commands::MAX_RUNNING)),
            )
            .await
        });
        let (stream, _) = hub.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        let (_stalled, _) = api.accept().await.unwrap();
        ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
        ws.send(Message::Text(
            serde_json::json!({"type": "command", "id": "still-responsive",
            "action": "unknown"})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut reports, mut pong, mut response) = (0, false, false);
            while reports < 2 || !pong || !response {
                match ws.next().await.unwrap().unwrap() {
                    Message::Pong(bytes) => pong = bytes.as_ref() == [1, 2, 3],
                    Message::Text(text) => {
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if value["method"] == "report" {
                            reports += 1;
                        }
                        if value["id"] == "still-responsive" {
                            assert_eq!(value["error"]["code"], "UNKNOWN_ACTION");
                            response = true;
                        }
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("stats query must not block the WebSocket loop for its 3s timeout");
        ws.close(None).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), client).await.unwrap().unwrap().unwrap();
    }

    #[test]
    fn a_hub_restart_costs_a_second_while_an_unreachable_one_still_backs_off() {
        // Nothing on the other end: double to the ceiling and hold. Zero is
        // what the caller passes for a connect that never completed, so a
        // stalled attempt cannot be credited as a session that ran.
        let mut wait = 0;
        let climb: Vec<u64> = (0..8)
            .map(|_| {
                wait = reconnect_wait(wait, Duration::ZERO);
                wait
            })
            .collect();
        assert_eq!(climb, [1, 2, 4, 8, 16, 32, 60, 60]);

        // A session that ran resets the wait however high it had climbed:
        // the hub-restart case.
        assert_eq!(reconnect_wait(60, Duration::from_secs(3600)), 1);
        // Connected but dropped too early to prove anything: still a retreat.
        assert_eq!(reconnect_wait(4, Duration::from_secs(29)), 8);
    }

    #[test]
    fn reconnect_jitter_spreads_retries_within_the_hard_limits() {
        assert_eq!(reconnect_jitter(1), 1000..=1250);
        assert_eq!(reconnect_jitter(2), 1500..=2500);
        assert_eq!(reconnect_jitter(4), 3000..=5000);
        assert_eq!(reconnect_jitter(60), 45_000..=60_000);
        assert_eq!(reconnect_jitter(0), reconnect_jitter(1));
        assert_eq!(reconnect_jitter(u64::MAX), reconnect_jitter(60));
        for base in 1..=60 {
            let range = reconnect_jitter(base);
            assert!(*range.start() >= 1000 && *range.end() <= 60_000);
            assert!(range.start() < range.end(), "jitter must remain effective at the ceiling");
        }
        let reset = reconnect_wait(60, Duration::from_secs(30));
        assert_eq!(reconnect_jitter(reset), 1000..=1250, "stable sessions reset to a short delay");
    }

    #[test]
    fn a_second_connection_is_wanted_only_for_a_private_address_in_the_other_family() {
        // A v4 session: NAT66 or a ULA is worth asking about, a public v6 is
        // reported as it is, and none leaves nothing to connect from.
        assert_eq!(glimpse_family(false, "10.0.0.5", "fd00::5"), Some(true));
        assert_eq!(glimpse_family(false, "203.0.113.5", "fd42::1"), Some(true));
        assert_eq!(glimpse_family(false, "10.0.0.5", "2001:db8::5"), None);
        assert_eq!(glimpse_family(false, "10.0.0.5", ""), None);
        // A v6 session reached over a hub that has no v4 record, or whose v4 failed.
        assert_eq!(glimpse_family(true, "10.0.0.5", "2001:db8::5"), Some(false));
        assert_eq!(glimpse_family(true, "203.0.113.5", "2001:db8::5"), None);
        assert_eq!(glimpse_family(true, "", "2001:db8::5"), None);
    }

    #[test]
    fn the_echo_route_sits_beside_the_session_route() {
        let session = ws_url("https://hub.example.com/", false).unwrap();
        assert_eq!(echo_url(&session).unwrap(), "wss://hub.example.com/api/agent/echo");
        assert!(echo_url("wss://hub.example.com/other").is_err());
    }

    #[test]
    fn ws_url_upgrades_scheme_and_refuses_plaintext_to_remote() {
        assert_eq!(ws_url("https://hub.example.com/", false).unwrap(), "wss://hub.example.com/api/agent/ws");
        assert_eq!(ws_url("http://127.0.0.1:28080", false).unwrap(), "ws://127.0.0.1:28080/api/agent/ws");
        // A bare host defaults to TLS rather than leaking the token.
        assert!(ws_url("hub.example.com", false).unwrap().starts_with("wss://"));
        assert!(ws_url("http://hub.example.com", false).is_err());
        // Bracketed IPv6 loopback is not remote.
        assert_eq!(ws_url("http://[::1]:28080", false).unwrap(), "ws://[::1]:28080/api/agent/ws");
        assert_eq!(ws_url("http://localhost:28080", false).unwrap(), "ws://localhost:28080/api/agent/ws");
        // A name that merely begins like the loopback net belongs to someone
        // else: the host is parsed, not prefix-matched.
        assert!(ws_url("http://127.attacker.example/", false).is_err());
        // Fail closed: a mapped literal is not read as loopback either.
        assert!(ws_url("http://[::ffff:127.0.0.1]:28080", false).is_err());
        // Userinfo places a loopback address where the host check looks and
        // another name where the socket goes; http::Uri resolves this authority's
        // host to evil.example.com. --insecure skips the plaintext refusal, so the
        // check cannot live inside it.
        assert!(ws_url("http://127.0.0.1:28080@evil.example.com/", false).is_err());
        assert!(ws_url("http://127.0.0.1:28080@evil.example.com/", true).is_err());
        assert!(ws_url("https://hub.example.com@evil.example.com/", false).is_err());
        // No token anywhere in the URL; it travels in a header.
        assert!(!ws_url("https://hub.example.com", false).unwrap().contains("token"));
        // Already a WebSocket address, and any scheme in either case.
        assert_eq!(ws_url("ws://127.0.0.1:1", false).unwrap(), "ws://127.0.0.1:1/api/agent/ws");
        assert_eq!(ws_url("wss://hub.example.com", false).unwrap(), "wss://hub.example.com/api/agent/ws");
        assert_eq!(ws_url("HTTPS://hub.example.com", false).unwrap(), "wss://hub.example.com/api/agent/ws");
        assert!(ws_url("HTTP://hub.example.com", false).is_err(), "plaintext whatever its case");
    }

    /// `--insecure` covers a hub reached at ip:port with no TLS: it permits the
    /// plaintext hop and suppresses the bare-host TLS upgrade, which would
    /// otherwise dial wss:// at a port that cannot answer.
    #[test]
    fn insecure_allows_plaintext_to_a_remote_hub_and_stops_upgrading_bare_hosts() {
        assert_eq!(
            ws_url("http://203.0.113.10:28080", true).unwrap(),
            "ws://203.0.113.10:28080/api/agent/ws"
        );
        assert_eq!(ws_url("203.0.113.10:28080", true).unwrap(), "ws://203.0.113.10:28080/api/agent/ws");
        // An explicit https:// hub stays on TLS: the flag permits plaintext
        // rather than forcing it.
        assert_eq!(ws_url("https://hub.example.com", true).unwrap(), "wss://hub.example.com/api/agent/ws");
    }

    #[tokio::test]
    async fn tcp_ping_measures_success_and_reports_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });
        // A loopback handshake completes within a millisecond, so this reads 0
        // either way; what it pins is the contract that reachable is
        // non-negative and unreachable is -1.
        assert!(tcp_ping(&addr.to_string()).await.unwrap() >= 0);
        assert_eq!(tcp_ping("127.0.0.1:1").await, Some(-1), "nothing is listening there");
        // A failed resolution is an unreachable target, not a missing sample.
        // std rejects this port before the resolver is reached, so the
        // assertion needs no network.
        assert_eq!(tcp_ping("127.0.0.1:99999").await, Some(-1), "an unresolvable target is unreachable");

        // First address dead: a dual-stack target on a host whose v6 goes
        // nowhere. The probe advances rather than reporting it unreachable.
        let dead: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(handshake([dead, addr].into_iter()).await >= 0, "a dead address must not end the probe");
        assert_eq!(handshake([dead, dead].into_iter()).await, -1, "every address failed");
        // Past the ceiling the remainder are skipped; a long address list
        // would otherwise hold a probe past its own interval.
        assert_eq!(
            handshake([dead, dead, dead, addr].into_iter()).await,
            -1,
            "a fourth address is not tried"
        );
    }

    /// A listener whose accept queue is full drops further SYNs rather than
    /// refusing them: a black hole on loopback. The clock is paused, so the
    /// fallback deadline elapses as soon as nothing else can progress, while
    /// without it the connect would wait out the kernel's SYN retries.
    #[tokio::test(start_paused = true)]
    async fn a_black_holed_address_gives_way_to_the_next_within_the_fallback() {
        let hole = tokio::net::TcpSocket::new_v4().unwrap();
        hole.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let hole = hole.listen(0).unwrap();
        let dead = hole.local_addr().unwrap();
        let _queued = std::net::TcpStream::connect(dead).unwrap();
        let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live = live.local_addr().unwrap();

        let started = Instant::now();
        let stream = connect_first(&[dead, live]).await.unwrap();
        assert_eq!(stream.peer_addr().unwrap(), live);
        assert_eq!(started.elapsed(), DIAL_FALLBACK, "the dead address costs the fallback and no more");
        let e = connect_first(&[dead, "127.0.0.1:1".parse().unwrap()]).await.unwrap_err().to_string();
        assert!(e.contains("timed out") && e.contains("refused"), "each failure is named: {e}");
        // The last address has no fallback of its own: a hub with a single
        // address keeps the whole CONNECT_DEADLINE for a slow handshake.
        let alone = tokio::time::timeout(DIAL_FALLBACK * 2, connect_first(&[dead])).await;
        assert!(alone.is_err(), "the last address must not be cut off at the fallback");
    }

    /// Where `localhost` resolves to ::1 before 127.0.0.1, only the preference
    /// can land this connect on v4. Elsewhere -- no v6 loopback, or a resolver
    /// ordering v4 first -- the preference is not observable and the test says
    /// so rather than failing.
    #[tokio::test]
    async fn a_host_behind_nat_dials_the_hubs_v4_first() {
        let v4 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = v4.local_addr().unwrap().port();
        let Ok(_v6) = std::net::TcpListener::bind(("::1", port)) else {
            return eprintln!("skipped: no IPv6 loopback");
        };
        if !dial("localhost", port, false).await.unwrap().peer_addr().unwrap().is_ipv6() {
            return eprintln!("skipped: the resolver lists 127.0.0.1 first");
        }
        assert!(dial("localhost", port, true).await.unwrap().peer_addr().unwrap().is_ipv4());
        assert!(
            dial("::1", port, true).await.unwrap().peer_addr().unwrap().is_ipv6(),
            "a literal is dialed as given"
        );
    }

    /// The deadline must stay under the kernel's first SYN retransmit, or a
    /// dropped SYN returns as roughly 1200ms of apparent latency -- the 1s timer
    /// plus the round trip. Such readings cluster at 1200ms and 3200ms, the
    /// signature of a retransmit rather than a slow link.
    #[test]
    fn the_handshake_deadline_stays_under_the_kernels_syn_timer() {
        assert!(
            HANDSHAKE_DEADLINE < Duration::from_secs(1),
            "a deadline at or past the 1s initial RTO lets retransmits be reported as latency"
        );
    }

    /// The hub pings every 30s and drops an agent silent for 120s. Both ends of
    /// this window are load-bearing and neither is visible from this file.
    ///
    /// The bound holds only because the watchdog sleeps to a deadline and each
    /// write draws from what remains of that same deadline; the two structures
    /// enforcing that are asserted separately below.
    #[test]
    fn the_agent_gives_up_on_a_silent_hub_before_the_hub_gives_up_on_it() {
        assert!(
            HUB_SILENCE < Duration::from_secs(120),
            "past the hub's own timeout the agent stops being what recovers the connection"
        );
        assert!(HUB_SILENCE > Duration::from_secs(60), "two lost heartbeats are a blip, not a dead link");
    }

    /// A sink that never accepts: the socket whose peer has stopped reading,
    /// which is the case the write deadline exists for.
    struct NeverDrains;

    impl Sink<Message> for NeverDrains {
        type Error = WsError;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), WsError>> {
            std::task::Poll::Pending
        }

        fn start_send(self: std::pin::Pin<&mut Self>, _: Message) -> Result<(), WsError> {
            unreachable!("never ready")
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), WsError>> {
            std::task::Poll::Pending
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), WsError>> {
            std::task::Poll::Pending
        }
    }

    /// Every write draws from one clock started at the handshake, so a session
    /// gives up exactly HUB_SILENCE after its last sign of life regardless of
    /// how many writes stalled in between. The hello is the first such write.
    #[tokio::test(start_paused = true)]
    async fn a_slow_hello_cannot_push_the_give_up_point_past_the_hubs_own_timeout() {
        let handshake = Instant::now();
        assert_eq!(remaining(handshake), HUB_SILENCE, "the first write gets the whole budget");

        // A stalled hello must fail at the budget it was handed, not one of
        // its own.
        assert!(send(&mut NeverDrains, notify("hello", serde_json::json!({})), remaining(handshake))
            .await
            .is_err());
        assert_eq!(handshake.elapsed(), HUB_SILENCE, "the stall costs the budget, no more");

        // Afterwards the give-up moment stays at last_frame + HUB_SILENCE:
        // spent plus remaining is always that one window.
        for spent in [0, 30, 89, 90, 200] {
            let last_frame = Instant::now();
            tokio::time::advance(Duration::from_secs(spent)).await;
            assert_eq!(
                last_frame.elapsed() + remaining(last_frame),
                HUB_SILENCE.max(last_frame.elapsed()),
                "{spent}s in, the budget must not push the deadline out"
            );
        }
    }

    #[test]
    fn a_log_line_names_each_cause_once() {
        use tokio_tungstenite::tungstenite::error::ProtocolError;
        for e in [
            WsError::Io(std::io::ErrorKind::ConnectionReset.into()),
            WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
        ] {
            let e = anyhow::Error::new(e).context("write");
            let (line, cause) = (causes(&e), e.root_cause().to_string());
            assert!(line.starts_with("write: "), "{line}");
            assert_eq!(line.matches(&cause).count(), 1, "{line}");
        }
        // A cause the message above it does not repeat stays in the line.
        assert_eq!(causes(&anyhow!("reset").context("write")), "write: reset");
    }

    #[tokio::test]
    async fn a_failed_dial_names_the_address_once() {
        let line = causes(&dial("127.0.0.1", 1, false).await.unwrap_err());
        assert!(line.starts_with("connect: 127.0.0.1:1: "), "{line}");
        assert_eq!(line.matches("127.0.0.1").count(), 1, "{line}");
    }

    #[test]
    fn the_token_travels_in_the_authorization_header_alone() {
        let request = authorized("wss://hub.example.com/api/agent/ws", "s3cret").unwrap();
        assert_eq!(request.headers()["authorization"], "Bearer s3cret");
        assert!(!request.uri().to_string().contains("s3cret"));
    }

    /// The hub sends every assigned task in one `ping.tasks` frame. A full list
    /// of the longest targets a DNS name allows must fit under MAX_MESSAGE, or
    /// the agent would end the session on that frame and receive it again on
    /// every reconnect.
    #[test]
    fn a_full_task_list_fits_in_one_frame() {
        let target = format!("{}:65535", "a".repeat(253));
        let tasks: Vec<_> = (0..MAX_PING_TASKS as i64)
            .map(|id| serde_json::json!({"id": i64::MAX - id, "target": target, "interval": 3600}))
            .collect();
        let frame = notify("ping.tasks", serde_json::json!(tasks));
        assert!(frame.len() <= MAX_MESSAGE, "{} bytes", frame.len());
    }

    #[test]
    fn ping_tasks_keep_their_timers_unless_the_task_changed() {
        // The runtime flavour the binary uses.
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _g = rt.enter();
        let (tx, _rx) = mpsc::channel(8);
        let mut running = Vec::new();
        let task = |id, target: &str, interval| PingTask { id, target: target.into(), interval };

        respawn_ping_tasks(&mut running, vec![task(1, "a:1", 60), task(2, "b:2", 60)], &tx);
        assert_eq!(running.len(), 2);
        let (first, second) = (running[0].1.id(), running[1].1.id());
        let replaced = running[1].1.abort_handle();

        // Task 1 unchanged, task 2 retargeted, task 3 added.
        respawn_ping_tasks(
            &mut running,
            vec![task(1, "a:1", 60), task(2, "c:3", 60), task(3, "d:4", 60)],
            &tx,
        );
        assert_eq!(running.len(), 3);
        assert_eq!(running[0].1.id(), first, "unchanged task must not be restarted");
        // A task whose target changed must be torn down, or it keeps probing
        // the old address.
        assert_ne!(running[1].1.id(), second, "a retargeted task must be restarted");
        rt.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
        assert!(replaced.is_finished(), "the old one is stopped, not left probing beside it");
        assert!(!running[0].1.is_finished(), "the unchanged one keeps running");

        // Interval 0 must not take the probe down: tokio's interval panics on
        // a zero period, and a panicked task stops reporting silently.
        respawn_ping_tasks(&mut running, vec![task(9, "e:5", 0)], &tx);
        rt.block_on(async { tokio::time::sleep(Duration::from_millis(50)).await });
        assert!(!running[0].1.is_finished(), "a zero interval must be clamped, not panic the probe");

        // One 64 KiB frame could carry some fifteen hundred of these; the
        // agent enforces its own ceiling rather than trusting the count.
        let flood = (0..500).map(|id| task(id, "f:6", 60)).collect();
        respawn_ping_tasks(&mut running, flood, &tx);
        assert_eq!(running.len(), MAX_PING_TASKS, "the hub does not choose how many probes run");
    }
}
