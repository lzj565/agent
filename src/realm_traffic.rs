//! Optional, fail-closed accounting. This module never controls Realm's service.
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{mpsc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const TABLE: &str = "monitor_realm_traffic";
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub key: String,
    pub listen: String,
    pub port: u16,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub digest: String,
    pub recheck: String,
    pub rules: Vec<Rule>,
}
fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}
fn token() -> String {
    format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>())
}
pub fn valid_key(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 1024 || self.digest.len() > 128 || self.recheck.len() > 64 {
            return Err("invalid accounting config size".into());
        }
        let mut ports = std::collections::HashSet::new();
        let mut keys = std::collections::HashSet::new();
        for r in &self.rules {
            if !valid_key(&r.key)
                || r.port == 0
                || !["0.0.0.0", "::"].contains(&r.listen.as_str())
                || !ports.insert(r.port)
                || !keys.insert(&r.key)
            {
                return Err("invalid accounting rule".into());
            }
        }
        Ok(())
    }
}
enum Request {
    Configure(Config, tokio::sync::oneshot::Sender<Result<Value, String>>),
    Ack(Value),
}
struct Handle {
    tx: mpsc::SyncSender<Request>,
    snapshot: std::sync::Arc<Mutex<Value>>,
}
static HANDLE: OnceLock<Handle> = OnceLock::new();
fn handle() -> &'static Handle {
    HANDLE.get_or_init(|| {
        let (tx,rx)=mpsc::sync_channel(32); let snapshot=std::sync::Arc::new(Mutex::new(json!({"status":{"state":"waiting","reason":"等待有效规则"},"records":[]})));
        let shared=snapshot.clone();
        std::thread::Builder::new().name("realm-traffic".into()).spawn(move || {
            let mut worker=match Ledger::open(Path::new("/opt/monitor/config")) { Ok(w)=>w, Err(e)=> { *shared.lock().unwrap()=json!({"collector":token(),"status":{"state":"unmonitored","reason":format!("ledger: {e}")},"records":[]}); return; } };
            let mut next=Instant::now();
            loop {
                match rx.recv_timeout(next.saturating_duration_since(Instant::now())) {
                    Ok(Request::Configure(c,reply))=>{ let result=worker.configure(c); let _=reply.send(result); },
                    Ok(Request::Ack(v))=>{ let _=worker.ack(&v); },
                    Err(mpsc::RecvTimeoutError::Disconnected)=>break,
                    Err(mpsc::RecvTimeoutError::Timeout)=>{}
                }
                if Instant::now()>=next { if let Err(e)=worker.sample() { let _=worker.disable(&e); } next=Instant::now()+Duration::from_secs(5); }
                if let Ok(v)=worker.snapshot() { *shared.lock().unwrap()=v; }
            }
        }).expect("realm traffic worker");
        Handle{tx,snapshot}
    })
}
pub fn snapshot() -> Value {
    handle().snapshot.lock().unwrap().clone()
}
pub fn ack(v: Value) {
    let _ = handle().tx.try_send(Request::Ack(v));
}
pub async fn configure(c: Config) -> Result<Value, String> {
    c.validate()?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle().tx.try_send(Request::Configure(c, tx)).map_err(|_| "traffic worker unavailable".to_string())?;
    rx.await.map_err(|_| "traffic worker stopped".to_string())?
}
struct Ledger {
    db: Connection,
    root: PathBuf,
    config: Option<Config>,
    owner: String,
    collector: String,
    state: String,
    reason: String,
    gaps: i64,
    last: i64,
    baseline: HashMap<String, (i64, i64)>,
    rates: Value,
    boot: String,
}
impl Ledger {
    fn open(root: &Path) -> Result<Self, String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let path = root.join("realm-traffic.db");
        if fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err("symlink ledger refused".into());
        }
        let db = Connection::open(&path).map_err(|e| e.to_string())?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-512; PRAGMA journal_size_limit=4194304; PRAGMA wal_autocheckpoint=256; PRAGMA max_page_count=14336; CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL); CREATE TABLE IF NOT EXISTS buckets(id INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL,minute INTEGER NOT NULL,up INTEGER NOT NULL,down INTEGER NOT NULL,revision INTEGER NOT NULL,acked INTEGER NOT NULL DEFAULT 0,sample INTEGER NOT NULL DEFAULT 0,up_rate INTEGER,down_rate INTEGER,UNIQUE(key,minute)); CREATE INDEX IF NOT EXISTS pending ON buckets(minute,id) WHERE acked<revision; CREATE INDEX IF NOT EXISTS age ON buckets(minute,id);").map_err(|e|e.to_string())?;
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
        let get = |k: &str| -> String {
            db.query_row("SELECT v FROM meta WHERE k=?1", [k], |r| r.get(0))
                .optional()
                .ok()
                .flatten()
                .unwrap_or_default()
        };
        let owner = match get("owner") {
            s if valid_key(&s) => s,
            _ => token(),
        };
        let collector = match get("collector") {
            s if valid_key(&s) => s,
            _ => token(),
        };
        let config = serde_json::from_str(&get("config")).ok();
        let state = match get("state") {
            s if s.is_empty() => "waiting".into(),
            s => s,
        };
        let reason = get("reason");
        let gaps = get("gaps").parse().unwrap_or(0);
        let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap_or_default();
        let old_boot = get("boot");
        let mut w = Self {
            db,
            root: root.into(),
            config,
            owner,
            collector,
            state,
            reason,
            gaps,
            last: 0,
            baseline: HashMap::new(),
            rates: json!({}),
            boot,
        };
        w.set("owner", &w.owner)?;
        w.set("collector", &w.collector)?;
        // Restart creates an explicit gap; unobserved bytes are never assigned to a later minute.
        if w.state == "monitoring" {
            w.gaps += 1;
            if old_boot != w.boot {
                w.state = "waiting".into();
            } else if let Err(e) = w.read_counters() {
                w.disable(&e)?;
            }
        }
        w.persist()?;
        Ok(w)
    }
    fn set(&self, k: &str, v: &str) -> Result<(), String> {
        self.db
            .execute(
                "INSERT INTO meta VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                params![k, v],
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn persist(&self) -> Result<(), String> {
        for (k, v) in [
            ("state", self.state.clone()),
            ("reason", self.reason.clone()),
            ("gaps", self.gaps.to_string()),
            ("boot", self.boot.clone()),
        ] {
            self.set(k, &v)?;
        }
        Ok(())
    }
    fn disable(&mut self, reason: &str) -> Result<(), String> {
        if self.state != "unmonitored" {
            self.gaps += 1;
        }
        self.state = "unmonitored".into();
        self.reason = reason.chars().take(256).collect();
        self.rates = json!({});
        self.persist()
    }
    fn owned(&self, v: &Value) -> bool {
        v["nftables"].as_array().is_some_and(|a| {
            a.iter().any(|o| {
                o["chain"]["table"] == TABLE && o["chain"]["name"] == format!("owner_{}", self.owner)
            })
        })
    }
    fn read_table(&self) -> Result<Value, String> {
        let bytes = nft(&self.root, &["-j", "list", "table", "inet", TABLE], None)?;
        let v: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if !self.owned(&v) {
            return Err("nft table ownership mismatch".into());
        }
        Ok(v)
    }
    fn read_counters(&self) -> Result<HashMap<String, (i64, i64)>, String> {
        let v = self.read_table()?;
        let mut out = HashMap::new();
        let mut names = std::collections::HashSet::new();
        for o in v["nftables"].as_array().ok_or("invalid nft JSON")? {
            let c = &o["counter"];
            if let (Some(n), Some(bytes)) = (c["name"].as_str(), c["bytes"].as_i64()) {
                if bytes < 0 {
                    return Err("negative counter".into());
                }
                names.insert(n.to_owned());
                if n.len() == 34 {
                    let key = &n[2..];
                    if valid_key(key) {
                        let p = out.entry(key.to_owned()).or_insert((0, 0));
                        if n.starts_with("u_") {
                            p.0 = bytes;
                        } else if n.starts_with("d_") {
                            p.1 = bytes;
                        }
                    }
                }
            }
        }
        for r in &self.config.as_ref().ok_or("missing config")?.rules {
            if !names.contains(&format!("u_{}", r.key)) || !names.contains(&format!("d_{}", r.key)) {
                return Err("nft counter missing".into());
            }
        }
        Ok(out)
    }
    fn configure(&mut self, c: Config) -> Result<Value, String> {
        c.validate()?;
        let manual =
            self.config.as_ref().is_some_and(|old| old.recheck != c.recheck) && !c.recheck.is_empty();
        let changed = self.config.as_ref().is_none_or(|old| old.rules != c.rules);
        if self.state == "monitoring" && changed {
            if let Err(e) = self.sample() {
                self.disable(&e)?;
            }
        }
        self.set("config", &serde_json::to_string(&c).map_err(|e| e.to_string())?)?;
        self.config = Some(c);
        if self.state == "unmonitored" && !manual {
            return Ok(self.status());
        }
        if self.config.as_ref().unwrap().rules.is_empty() && self.state != "monitoring" {
            return Ok(self.status());
        }
        if changed || manual || self.state != "monitoring" {
            let preserve = self.state == "monitoring" && !manual;
            let result = (|| {
                self.install()?;
                let values = self.read_counters()?;
                if preserve {
                    self.record(values, now())?;
                } else {
                    self.baseline = values;
                    self.last = now();
                }
                // Maps no longer reference retired counters. Drain them before deleting objects.
                let mut retire = String::new();
                for key in self.baseline.keys() {
                    if !self.config.as_ref().unwrap().rules.iter().any(|r| &r.key == key) {
                        retire += &format!(
                            "delete counter inet {TABLE} u_{key}\ndelete counter inet {TABLE} d_{key}\n"
                        );
                    }
                }
                if !retire.is_empty() {
                    nft(&self.root, &["-f"], Some(&retire))?;
                }
                self.baseline
                    .retain(|key, _| self.config.as_ref().unwrap().rules.iter().any(|r| &r.key == key));
                self.state =
                    if self.config.as_ref().unwrap().rules.is_empty() { "waiting" } else { "monitoring" }
                        .into();
                self.reason.clear();
                self.persist()
            })();
            if let Err(e) = result {
                self.disable(&e)?;
            }
        }
        Ok(self.status())
    }
    fn install(&mut self) -> Result<(), String> {
        let tables: Value = serde_json::from_slice(&nft(&self.root, &["-j", "list", "tables"], None)?)
            .map_err(|e| e.to_string())?;
        let exists = tables["nftables"]
            .as_array()
            .is_some_and(|a| a.iter().any(|o| o["table"]["family"] == "inet" && o["table"]["name"] == TABLE));
        let previous = if exists { Some(self.read_table()?) } else { None };
        let c = self.config.as_ref().ok_or("missing config")?;
        let mut script = if exists {
            format!("flush chain inet {TABLE} input\nflush chain inet {TABLE} output\nflush chain inet {TABLE} ingress\nflush chain inet {TABLE} egress\n")
        } else {
            format!("add table inet {TABLE} {{ comment \"{}\"; }}\nadd chain inet {TABLE} owner_{}\nadd chain inet {TABLE} ingress\nadd chain inet {TABLE} egress\nadd chain inet {TABLE} input {{ type filter hook input priority -10; policy accept; }}\nadd chain inet {TABLE} output {{ type filter hook output priority -10; policy accept; }}\n",self.owner,self.owner)
        };
        for m in ["up4", "down4", "up6", "down6"] {
            if exists {
                script += &format!("flush map inet {TABLE} {m}\n");
            } else {
                script += &format!("add map inet {TABLE} {m} {{ type inet_service : counter; }}\n");
            }
        }
        let old: std::collections::HashSet<String> = previous
            .as_ref()
            .and_then(|v| v["nftables"].as_array())
            .into_iter()
            .flatten()
            .filter_map(|o| o["counter"]["name"].as_str().map(str::to_owned))
            .collect();
        if old.iter().any(|n| {
            n.len() != 34 || !n.is_ascii() || !["u_", "d_"].contains(&&n[..2]) || !valid_key(&n[2..])
        }) {
            return Err("unexpected object in accounting table".into());
        }
        for r in &c.rules {
            for prefix in ["u", "d"] {
                let name = format!("{prefix}_{}", r.key);
                if !old.contains(&name) {
                    script += &format!("add counter inet {TABLE} {name}\n");
                }
            }
            for (map, prefix) in [("up4", "u"), ("down4", "d")] {
                script +=
                    &format!("add element inet {TABLE} {map} {{ {} : \"{prefix}_{}\" }}\n", r.port, r.key);
            }
            if r.listen == "::" {
                for (map, prefix) in [("up6", "u"), ("down6", "d")] {
                    script += &format!(
                        "add element inet {TABLE} {map} {{ {} : \"{prefix}_{}\" }}\n",
                        r.port, r.key
                    );
                }
            }
        }
        script += &format!("add rule inet {TABLE} input iifname != \"lo\" meta l4proto tcp jump ingress\nadd rule inet {TABLE} output oifname != \"lo\" meta l4proto tcp jump egress\n");
        for (chain, field, map, family) in [
            ("ingress", "dport", "up4", "ipv4"),
            ("egress", "sport", "down4", "ipv4"),
            ("ingress", "dport", "up6", "ipv6"),
            ("egress", "sport", "down6", "ipv6"),
        ] {
            script += &format!(
                "add rule inet {TABLE} {chain} meta nfproto {family} counter name tcp {field} map @{map}\n"
            );
        }
        nft(&self.root, &["-c", "-f"], Some(&script))?;
        nft(&self.root, &["-f"], Some(&script))?;
        Ok(())
    }
    fn prune(&mut self, ts: i64) -> Result<(), String> {
        let expired: i64 = self
            .db
            .query_row(
                "SELECT COUNT(*) FROM buckets WHERE minute<?1 AND acked<revision",
                [ts - 7 * 86400],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        self.db
            .execute(
                "DELETE FROM buckets WHERE minute<?1 OR (minute<?2 AND acked=revision)",
                params![ts - 7 * 86400, ts / 60 * 60],
            )
            .map_err(|e| e.to_string())?;
        let pages: i64 =
            self.db.query_row("PRAGMA page_count", [], |r| r.get(0)).map_err(|e| e.to_string())?;
        let free: i64 =
            self.db.query_row("PRAGMA freelist_count", [], |r| r.get(0)).map_err(|e| e.to_string())?;
        let mut dropped = 0;
        if pages - free > 11500 {
            dropped = self
                .db
                .execute(
                    "DELETE FROM buckets WHERE id IN (SELECT id FROM buckets ORDER BY minute,id LIMIT 4096)",
                    [],
                )
                .map_err(|e| e.to_string())?;
        }
        if expired > 0 || dropped > 0 {
            self.gaps += 1;
            self.persist()?;
        }
        Ok(())
    }
    fn sample(&mut self) -> Result<(), String> {
        if self.state == "waiting" && self.config.as_ref().is_some_and(|c| !c.rules.is_empty()) {
            if let Err(e) = self.install() {
                return self.disable(&e);
            }
            self.state = "monitoring".into();
            self.persist()?;
        }
        if self.state != "monitoring" {
            self.prune(now())?;
            return Ok(());
        }
        let values = self.read_counters()?;
        self.record(values, now())
    }
    fn record(&mut self, values: HashMap<String, (i64, i64)>, ts: i64) -> Result<(), String> {
        let elapsed = ts - self.last;
        self.prune(ts)?;
        let mut rates = serde_json::Map::new();
        let tx = self.db.transaction().map_err(|e| e.to_string())?;
        for (key, (up, down)) in &values {
            if let Some((u, d)) = self.baseline.get(key) {
                if up < u || down < d {
                    return Err("nft counter reset; manual recheck required".into());
                }
                if (0..=15).contains(&elapsed) {
                    let (du, dd) = (up - u, down - d);
                    tx.execute("INSERT INTO buckets(key,minute,up,down,revision,sample,up_rate,down_rate) VALUES(?1,?2,?3,?4,1,?5,?6,?7) ON CONFLICT(key,minute) DO UPDATE SET up=up+excluded.up,down=down+excluded.down,revision=revision+1,sample=excluded.sample,up_rate=excluded.up_rate,down_rate=excluded.down_rate",params![key,ts/60*60,du,dd,ts,du/elapsed.max(1),dd/elapsed.max(1)]).map_err(|e|e.to_string())?;
                    rates.insert(key.clone(), json!({"up":du/elapsed.max(1),"down":dd/elapsed.max(1)}));
                }
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        if self.last > 0 && !(0..=15).contains(&elapsed) {
            self.gaps += 1;
            self.persist()?;
        }
        self.baseline = values;
        self.last = ts;
        self.rates = Value::Object(rates);
        Ok(())
    }
    fn status(&self) -> Value {
        json!({"state":self.state,"reason":self.reason,"digest":self.config.as_ref().map(|c|&c.digest),"recheck":self.config.as_ref().map(|c|&c.recheck),"last_sample":self.last,"gaps":self.gaps,"up_rate":self.rates.as_object().map(|m|m.values().filter_map(|v|v["up"].as_i64()).sum::<i64>()).unwrap_or(0),"down_rate":self.rates.as_object().map(|m|m.values().filter_map(|v|v["down"].as_i64()).sum::<i64>()).unwrap_or(0)})
    }
    fn snapshot(&self) -> Result<Value, String> {
        let mut stmt=self.db.prepare("SELECT id,key,minute,up,down,revision,sample,up_rate,down_rate FROM buckets WHERE acked<revision ORDER BY minute,id LIMIT 128").map_err(|e|e.to_string())?;
        let records=stmt.query_map([],|r|Ok(json!({"id":r.get::<_,i64>(0)?,"key":r.get::<_,String>(1)?,"minute":r.get::<_,i64>(2)?,"up":r.get::<_,i64>(3)?,"down":r.get::<_,i64>(4)?,"revision":r.get::<_,i64>(5)?,"up_rate":r.get::<_,Option<i64>>(7)?,"down_rate":r.get::<_,Option<i64>>(8)?,"sample":r.get::<_,i64>(6)?}))).map_err(|e|e.to_string())?.collect::<rusqlite::Result<Vec<_>>>().map_err(|e|e.to_string())?;
        Ok(json!({"collector":self.collector,"status":self.status(),"records":records}))
    }
    fn ack(&mut self, v: &Value) -> Result<(), String> {
        if v["collector"] != self.collector {
            return Ok(());
        }
        if let Some(records) = v["records"].as_array() {
            let tx = self.db.transaction().map_err(|e| e.to_string())?;
            for r in records.iter().take(128) {
                tx.execute(
                    "UPDATE buckets SET acked=?2 WHERE id=?1 AND revision=?2",
                    params![r["id"].as_i64(), r["revision"].as_i64()],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
// No shell, bounded execution/output, and private files: never read or flush a foreign firewall.
fn nft(root: &Path, args: &[&str], script: Option<&str>) -> Result<Vec<u8>, String> {
    use std::os::unix::fs::OpenOptionsExt;
    let stem = root.join(format!(".nft-{}", token()));
    let input = stem.with_extension("in");
    let output = stem.with_extension("out");
    let result = (|| {
        let out = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&output)
            .map_err(|e| e.to_string())?;
        let mut cmd = Command::new("nft");
        cmd.args(args)
            .stdout(Stdio::from(out.try_clone().map_err(|e| e.to_string())?))
            .stderr(Stdio::from(out));
        if let Some(script) = script {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&input)
                .map_err(|e| e.to_string())?;
            use std::io::Write;
            file.write_all(script.as_bytes()).map_err(|e| e.to_string())?;
            cmd.arg(&input);
        }
        let mut child = cmd.spawn().map_err(|e| format!("nft unavailable: {e}"))?;
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                let mut bytes = Vec::new();
                fs::File::open(&output)
                    .map_err(|e| e.to_string())?
                    .take(2 * 1024 * 1024 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|e| e.to_string())?;
                if bytes.len() > 2 * 1024 * 1024 {
                    return Err("nft output too large".into());
                }
                if !status.success() {
                    return Err(format!(
                        "nft unavailable: {}",
                        String::from_utf8_lossy(&bytes[..bytes.len().min(256)])
                    ));
                }
                return Ok(bytes);
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("nft timeout".into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    })();
    let _ = fs::remove_file(input);
    let _ = fs::remove_file(output);
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    fn ledger() -> Ledger {
        let dir = std::env::temp_dir().join(format!("realm-ledger-{}", token()));
        Ledger::open(&dir).unwrap()
    }
    #[test]
    fn disabled_is_persistent_and_config_changes_do_not_probe() {
        let mut w = ledger();
        w.disable("missing nft").unwrap();
        let c = Config {
            digest: "d".into(),
            recheck: "".into(),
            rules: vec![Rule { key: token(), listen: "::".into(), port: 1234 }],
        };
        assert_eq!(w.configure(c.clone()).unwrap()["state"], "unmonitored");
        let root = w.root.clone();
        drop(w);
        let mut w = Ledger::open(&root).unwrap();
        assert_eq!(w.configure(c).unwrap()["reason"], "missing nft");
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn ack_matches_revision_and_collector() {
        let mut w = ledger();
        w.db.execute(
            "INSERT INTO buckets(id,key,minute,up,down,revision,acked) VALUES(1,'k',60,10,20,2,0)",
            [],
        )
        .unwrap();
        w.ack(&json!({"collector":w.collector,"records":[{"id":1,"revision":1}]})).unwrap();
        assert_eq!(w.snapshot().unwrap()["records"].as_array().unwrap().len(), 1);
        w.ack(&json!({"collector":"other","records":[{"id":1,"revision":2}]})).unwrap();
        assert_eq!(w.snapshot().unwrap()["records"].as_array().unwrap().len(), 1);
        w.ack(&json!({"collector":w.collector,"records":[{"id":1,"revision":2}]})).unwrap();
        assert!(w.snapshot().unwrap()["records"].as_array().unwrap().is_empty());
        let root = w.root.clone();
        drop(w);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn samples_survive_offline_replay_and_reset_never_wraps() {
        let mut w = ledger();
        let key = token();
        w.last = 100;
        w.baseline.insert(key.clone(), (1000, 2000));
        w.record(HashMap::from([(key.clone(), (1100, 2200))]), 105).unwrap();
        let first = w.snapshot().unwrap();
        assert_eq!(first["records"][0]["up"], 100);
        assert_eq!(first["records"][0]["sample"], 105);
        w.record(HashMap::from([(key.clone(), (1300, 2600))]), 110).unwrap();
        let second = w.snapshot().unwrap();
        assert_eq!(second["records"][0]["up"], 300);
        w.ack(&json!({"collector":w.collector,"records":first["records"]})).unwrap();
        assert_eq!(w.snapshot().unwrap()["records"][0]["up"], 300);
        assert!(w.record(HashMap::from([(key.clone(), (1, 2))]), 115).is_err());
        assert_eq!(w.snapshot().unwrap()["records"][0]["up"], 300);
        // Late collection has no defensible minute attribution: mark gap and rebaseline.
        w.record(HashMap::from([(key, (10000, 20000))]), 200).unwrap();
        assert_eq!(w.gaps, 1);
        assert_eq!(w.snapshot().unwrap()["records"][0]["sample"], 110);
        let root = w.root.clone();
        drop(w);
        let reopened = Ledger::open(&root).unwrap();
        assert_eq!(reopened.snapshot().unwrap()["records"][0]["up"], 300);
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn retention_marks_lost_unacknowledged_minutes_and_ids_do_not_recur() {
        let mut w = ledger();
        w.db.execute("INSERT INTO buckets(key,minute,up,down,revision) VALUES('k',60,10,20,1)", []).unwrap();
        let first = w.db.last_insert_rowid();
        w.prune(8 * 86400).unwrap();
        assert_eq!(w.gaps, 1);
        w.db.execute("INSERT INTO buckets(key,minute,up,down,revision) VALUES('k',120,10,20,1)", []).unwrap();
        assert!(w.db.last_insert_rowid() > first);
        let root = w.root.clone();
        drop(w);
        fs::remove_dir_all(root).unwrap();
    }
}
