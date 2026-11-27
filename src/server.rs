//! One process, one Raft node. The loop owns the Forge log and fsyncs there.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use forge_core::{Clock, Disk, SystemClock};

use crate::logstore::ForgeLog;
use crate::raft::{ClientResult, Effect, Node};
use crate::rpc::{decode_frame, encode_frame, Frame};

pub struct Config {
    pub id: u8,
    pub data: PathBuf,
    pub raft: String,
    pub http: String,
    pub advertise: String,
    pub peers: BTreeMap<u8, String>,
    pub peer_http: BTreeMap<u8, String>,
}

pub fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut id = None;
    let mut data = None;
    let mut raft = None;
    let mut http = None;
    let mut advertise = None;
    let mut peers = BTreeMap::new();
    let mut peer_http = BTreeMap::new();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let mut value = || {
            it.next()
                .ok_or_else(|| format!("missing value after {arg}"))
        };
        match arg.as_str() {
            "--id" => id = Some(parse_id(&value()?)?),
            "--data" => data = Some(PathBuf::from(value()?)),
            "--raft" => raft = Some(value()?),
            "--http" => http = Some(value()?),
            "--advertise" => advertise = Some(value()?),
            "--peers" => peers = parse_map(&value()?)?,
            "--peer-http" => peer_http = parse_map(&value()?)?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let id = id.ok_or("missing --id")?;
    Ok(Config {
        id,
        data: data.ok_or("missing --data")?,
        raft: raft.ok_or("missing --raft")?,
        http: http.ok_or("missing --http")?,
        advertise: advertise.ok_or("missing --advertise")?,
        peers,
        peer_http,
    })
}

pub fn serve(cfg: Config) -> Result<(), String> {
    let disk = Disk::real(&cfg.data).map_err(|e| e.to_string())?;
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let log = ForgeLog::open(disk, clock)?;
    let peers: Vec<u8> = cfg.peers.keys().copied().collect();
    let started = Instant::now();
    let now = || started.elapsed().as_millis() as u64;
    let mut node = Node::start(cfg.id, peers, log, now(), cfg.id as u64)?;
    let raft_l = TcpListener::bind(&cfg.raft).map_err(|e| format!("raft bind: {e}"))?;
    let http_l = TcpListener::bind(&cfg.http).map_err(|e| format!("http bind: {e}"))?;
    raft_l.set_nonblocking(true).map_err(|e| e.to_string())?;
    http_l.set_nonblocking(true).map_err(|e| e.to_string())?;
    log_line(cfg.id, "info", "listening");
    println!(
        "quorum ready id={} raft={} http={}",
        cfg.id, cfg.raft, cfg.http
    );
    let _ = std::io::stdout().flush();

    let mut peers_conn: BTreeMap<u8, Conn> = BTreeMap::new();
    let mut handshake: Vec<Conn> = Vec::new();
    let mut http: Vec<HttpConn> = Vec::new();
    let mut next_cookie = 1u64;
    let mut next_req = 1u64;
    let mut next_dial = 0u64;
    let mut http_ids = 1u64;

    loop {
        let t = now();
        if t >= next_dial {
            next_dial = t + 200;
            dial(&cfg, &mut peers_conn);
        }
        accept_raft(&raft_l, &mut handshake);
        accept_http(&http_l, &mut http, &mut http_ids);
        read_conns(&mut handshake);
        read_conns_map(&mut peers_conn);
        read_http(&mut http);
        take_hellos(&mut handshake, &mut peers_conn, cfg.id);
        let rpcs = drain_rpcs(&mut peers_conn);
        for rpc in rpcs {
            match node.on_rpc(rpc, t) {
                Ok(effects) => {
                    apply_effects_in_order(&cfg, effects, &mut peers_conn, &mut http, cfg.id)
                }
                Err(err) => log_line(cfg.id, "error", &format!("rpc: {err}")),
            }
        }
        pump_http(
            &cfg,
            &mut node,
            &mut http,
            &mut peers_conn,
            &mut next_cookie,
            &mut next_req,
            t,
        );
        match node.tick(t) {
            Ok(effects) => {
                apply_effects_in_order(&cfg, effects, &mut peers_conn, &mut http, cfg.id)
            }
            Err(err) => log_line(cfg.id, "error", &format!("tick: {err}")),
        }
        timeout_http(&mut http, t);
        write_conns(&mut handshake);
        write_conns_map(&mut peers_conn);
        write_http(&mut http);
        handshake.retain(|c| c.alive);
        http.retain(|c| c.alive);
        peers_conn.retain(|_, c| c.alive);
        thread::sleep(Duration::from_millis(1));
    }
}

fn dial(cfg: &Config, peers: &mut BTreeMap<u8, Conn>) {
    for (id, addr) in &cfg.peers {
        if *id <= cfg.id || peers.contains_key(id) {
            continue;
        }
        match TcpStream::connect(addr) {
            Ok(stream) => {
                let _ = stream.set_nodelay(true);
                let _ = stream.set_nonblocking(true);
                let mut conn = Conn::new(stream);
                conn.outbound
                    .extend_from_slice(&encode_frame(&Frame::Hello { id: cfg.id }));
                peers.insert(*id, conn);
                log_line(cfg.id, "info", &format!("connected to {id}"));
            }
            Err(_) => {}
        }
    }
}

fn accept_raft(listener: &TcpListener, handshake: &mut Vec<Conn>) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
                let _ = stream.set_nonblocking(true);
                handshake.push(Conn::new(stream));
            }
            Err(_) => break,
        }
    }
}

fn accept_http(listener: &TcpListener, http: &mut Vec<HttpConn>, ids: &mut u64) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nodelay(true);
                let _ = stream.set_nonblocking(true);
                *ids += 1;
                http.push(HttpConn {
                    id: *ids,
                    stream,
                    inbound: Vec::new(),
                    outbound: Vec::new(),
                    alive: true,
                    waiting: None,
                });
            }
            Err(_) => break,
        }
    }
}

struct Conn {
    stream: TcpStream,
    inbound: Vec<u8>,
    outbound: Vec<u8>,
    alive: bool,
}

impl Conn {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            inbound: Vec::new(),
            outbound: Vec::new(),
            alive: true,
        }
    }
}

struct HttpConn {
    id: u64,
    stream: TcpStream,
    inbound: Vec<u8>,
    outbound: Vec<u8>,
    alive: bool,
    waiting: Option<Waiting>,
}

struct Waiting {
    cookie: u64,
    since: u64,
    kind: ReqKind,
    path: String,
}

#[derive(Clone, Copy)]
enum ReqKind {
    Put,
    Get,
}

fn read_conns(conns: &mut [Conn]) {
    for conn in conns.iter_mut() {
        read_tcp(&mut conn.stream, &mut conn.inbound, &mut conn.alive);
    }
}

fn read_conns_map(conns: &mut BTreeMap<u8, Conn>) {
    for conn in conns.values_mut() {
        read_tcp(&mut conn.stream, &mut conn.inbound, &mut conn.alive);
    }
}

fn read_http(conns: &mut [HttpConn]) {
    for conn in conns {
        if conn.alive {
            read_tcp(&mut conn.stream, &mut conn.inbound, &mut conn.alive);
        }
    }
}

fn read_tcp(stream: &mut TcpStream, inbound: &mut Vec<u8>, alive: &mut bool) {
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                *alive = false;
                return;
            }
            Ok(n) => inbound.extend_from_slice(&buf[..n]),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                *alive = false;
                return;
            }
        }
        if inbound.len() > 8_000_000 {
            *alive = false;
            return;
        }
    }
}

fn write_conns(conns: &mut [Conn]) {
    for conn in conns.iter_mut() {
        write_tcp(&mut conn.stream, &mut conn.outbound, &mut conn.alive);
    }
}

fn write_conns_map(conns: &mut BTreeMap<u8, Conn>) {
    for conn in conns.values_mut() {
        write_tcp(&mut conn.stream, &mut conn.outbound, &mut conn.alive);
    }
}

fn write_http(conns: &mut [HttpConn]) {
    for conn in conns {
        write_tcp(&mut conn.stream, &mut conn.outbound, &mut conn.alive);
    }
}

fn write_tcp(stream: &mut TcpStream, outbound: &mut Vec<u8>, alive: &mut bool) {
    while !outbound.is_empty() && *alive {
        match stream.write(outbound) {
            Ok(0) => {
                *alive = false;
                return;
            }
            Ok(n) => {
                outbound.drain(..n);
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                *alive = false;
                return;
            }
        }
    }
}

fn take_hellos(handshake: &mut Vec<Conn>, peers: &mut BTreeMap<u8, Conn>, self_id: u8) {
    let mut i = 0;
    while i < handshake.len() {
        if !handshake[i].alive {
            i += 1;
            continue;
        }
        match decode_frame(&handshake[i].inbound) {
            Ok(None) => i += 1,
            Ok(Some((Frame::Hello { id }, n))) => {
                handshake[i].inbound.drain(..n);
                if id == self_id {
                    handshake[i].alive = false;
                    i += 1;
                    continue;
                }
                let conn = handshake.remove(i);
                peers.insert(id, conn);
                log_line(self_id, "info", &format!("accepted {id}"));
            }
            Ok(Some(_)) => {
                handshake[i].alive = false;
                i += 1;
            }
            Err(_) => {
                handshake[i].alive = false;
                i += 1;
            }
        }
    }
}

fn drain_rpcs(peers: &mut BTreeMap<u8, Conn>) -> Vec<crate::rpc::Rpc> {
    let mut out = Vec::new();
    for conn in peers.values_mut() {
        if !conn.alive {
            continue;
        }
        loop {
            match decode_frame(&conn.inbound) {
                Ok(None) => break,
                Ok(Some((Frame::Rpc(rpc), n))) => {
                    conn.inbound.drain(..n);
                    out.push(rpc);
                }
                Ok(Some((Frame::Hello { .. }, n))) => {
                    conn.inbound.drain(..n);
                }
                Err(_) => {
                    conn.alive = false;
                    break;
                }
            }
        }
    }
    out
}

fn apply_effects_in_order(
    cfg: &Config,
    effects: Vec<Effect>,
    peers: &mut BTreeMap<u8, Conn>,
    http: &mut [HttpConn],
    self_id: u8,
) {
    for effect in effects {
        match effect {
            Effect::Send { to, rpc } => {
                if let Some(conn) = peers.get_mut(&to) {
                    conn.outbound
                        .extend_from_slice(&encode_frame(&Frame::Rpc(rpc)));
                }
            }
            Effect::Respond { cookie, result } => answer(cfg, http, cookie, result),
            Effect::Log { level, msg } => log_line(self_id, level, &msg),
        }
    }
}

fn answer(cfg: &Config, http: &mut [HttpConn], cookie: u64, result: ClientResult) {
    let Some(conn) = http.iter_mut().find(|c| {
        c.waiting
            .as_ref()
            .map(|w| w.cookie == cookie)
            .unwrap_or(false)
    }) else {
        return;
    };
    let waiting = conn.waiting.take().unwrap();
    let bytes = match result {
        ClientResult::WriteOk { .. } => response(200, "OK", &[], b"ok\n"),
        ClientResult::ReadOk { value } => match value {
            Some(body) => response(200, "OK", &[], &body),
            None => response(404, "Not Found", &[], b""),
        },
        ClientResult::Redirect { leader } => redirect(cfg, leader, &waiting.path),
    };
    let _ = waiting.kind;
    conn.outbound.extend_from_slice(&bytes);
}

fn redirect(cfg: &Config, leader: Option<u8>, path: &str) -> Vec<u8> {
    let Some(id) = leader else {
        return response(503, "Service Unavailable", &[], b"no leader\n");
    };
    if id == cfg.id {
        return response(503, "Service Unavailable", &[], b"no leader\n");
    }
    let Some(base) = cfg.peer_http.get(&id) else {
        return response(503, "Service Unavailable", &[], b"no leader\n");
    };
    let loc = format!("{base}{path}");
    response(
        307,
        "Temporary Redirect",
        &[
            ("Location", loc.as_str()),
            ("X-Quorum-Leader", base.as_str()),
        ],
        b"",
    )
}

fn pump_http(
    cfg: &Config,
    node: &mut Node,
    http: &mut [HttpConn],
    peers: &mut BTreeMap<u8, Conn>,
    next_cookie: &mut u64,
    next_req: &mut u64,
    now: u64,
) {
    for i in 0..http.len() {
        if !http[i].alive || http[i].waiting.is_some() {
            continue;
        }
        let parsed = match parse_request(&http[i].inbound) {
            Ok(Some(req)) => req,
            Ok(None) => continue,
            Err(()) => {
                http[i].alive = false;
                continue;
            }
        };
        http[i].inbound.drain(..parsed.consumed);
        if parsed.method == "GET" && parsed.path == "/health" {
            let bytes = response(200, "OK", &[], b"ok\n");
            http[i].outbound.extend_from_slice(&bytes);
            continue;
        }
        if parsed.method == "GET" && parsed.path == "/metrics" {
            let body = metrics(node);
            let bytes = response(200, "OK", &[("Content-Type", "text/plain")], body.as_bytes());
            http[i].outbound.extend_from_slice(&bytes);
            continue;
        }
        let key = match parsed.path.strip_prefix("/kv/") {
            Some(rest) if !rest.is_empty() => percent_decode(rest),
            _ => None,
        };
        let Some(key) = key else {
            let bytes = response(404, "Not Found", &[], b"");
            http[i].outbound.extend_from_slice(&bytes);
            continue;
        };
        *next_cookie += 1;
        let cookie = *next_cookie;
        let kind = if parsed.method == "PUT" {
            ReqKind::Put
        } else if parsed.method == "GET" {
            ReqKind::Get
        } else {
            let bytes = response(405, "Method Not Allowed", &[], b"");
            http[i].outbound.extend_from_slice(&bytes);
            continue;
        };
        let effects = match kind {
            ReqKind::Put => {
                *next_req += 1;
                node.client_write(cookie, http[i].id, *next_req, key, parsed.body, now)
            }
            ReqKind::Get => node.client_read(cookie, key, now),
        };
        let effects = match effects {
            Ok(effects) => effects,
            Err(err) => {
                log_line(cfg.id, "error", &format!("client: {err}"));
                let bytes = response(500, "Error", &[], b"log error\n");
                http[i].outbound.extend_from_slice(&bytes);
                continue;
            }
        };
        http[i].waiting = Some(Waiting {
            cookie,
            since: now,
            kind,
            path: parsed.path,
        });
        apply_effects_in_order(cfg, effects, peers, http, cfg.id);
    }
}

fn timeout_http(http: &mut [HttpConn], now: u64) {
    for conn in http {
        let expired = conn
            .waiting
            .as_ref()
            .map(|w| now.saturating_sub(w.since) > 2_000)
            .unwrap_or(false);
        if expired {
            conn.waiting = None;
            let bytes = response(503, "Service Unavailable", &[], b"timeout\n");
            conn.outbound.extend_from_slice(&bytes);
        }
    }
}

struct Parsed {
    method: String,
    path: String,
    body: Vec<u8>,
    consumed: usize,
}

fn parse_request(buf: &[u8]) -> Result<Option<Parsed>, ()> {
    let Some(header_end) = find_header_end(buf) else {
        if buf.len() > 65_536 {
            return Err(());
        }
        return Ok(None);
    };
    if header_end > 65_536 {
        return Err(());
    }
    let head = std::str::from_utf8(&buf[..header_end]).map_err(|_| ())?;
    let mut lines = head.split("\r\n");
    let req = lines.next().ok_or(())?;
    let mut parts = req.split(' ');
    let method = parts.next().ok_or(())?.to_string();
    let path = parts.next().ok_or(())?.to_string();
    let mut length = 0usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            length = value.trim().parse().map_err(|_| ())?;
        }
    }
    if length > 1_048_576 {
        return Err(());
    }
    let total = header_end + 4 + length;
    if buf.len() < total {
        return Ok(None);
    }
    let body = buf[header_end + 4..total].to_vec();
    Ok(Some(Parsed {
        method,
        path,
        body,
        consumed: total,
    }))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn response(status: u16, reason: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut out = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n", body.len());
    for (name, value) in headers {
        out.push_str(name);
        out.push_str(": ");
        out.push_str(value);
        out.push_str("\r\n");
    }
    out.push_str("\r\n");
    let mut bytes = out.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

fn metrics(node: &Node) -> String {
    let status = node.status();
    let leader = status.leader.unwrap_or(0);
    format!(
        "quorum_node {}\nquorum_role {}\nquorum_term {}\nquorum_commit {}\nquorum_applied {}\nquorum_leader {}\nquorum_log_end {}\nquorum_fsync_total {}\n",
        status.id,
        status.role as u8,
        status.term,
        status.commit,
        status.applied,
        leader,
        status.last_index,
        status.fsync_total
    )
}

fn percent_decode(raw: &str) -> Option<Vec<u8>> {
    let bytes = raw.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_val(bytes[i + 1])?;
            let lo = hex_val(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Some(out)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn parse_id(raw: &str) -> Result<u8, String> {
    raw.parse::<u8>().map_err(|_| format!("bad id {raw}"))
}

fn parse_map(raw: &str) -> Result<BTreeMap<u8, String>, String> {
    let mut map = BTreeMap::new();
    if raw.is_empty() {
        return Ok(map);
    }
    for part in raw.split(',') {
        let (id, addr) = part
            .split_once('=')
            .ok_or_else(|| format!("bad peer {part}"))?;
        map.insert(parse_id(id)?, addr.to_string());
    }
    Ok(map)
}

pub fn log_line(id: u8, level: &str, msg: &str) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    eprintln!(
        "{{\"ts_ms\":{ts},\"node\":{id},\"level\":\"{level}\",\"msg\":\"{}\"}}",
        escape(msg)
    );
}

fn escape(msg: &str) -> String {
    let mut out = String::new();
    for ch in msg.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            ch if ch.is_control() => out.push(' '),
            ch => out.push(ch),
        }
    }
    out
}
