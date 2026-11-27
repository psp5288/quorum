//! Local process benchmark. Numbers printed here are the only ones the README may cite.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

struct Proc {
    id: u8,
    http: SocketAddr,
    child: Child,
    log_path: PathBuf,
}

struct Group {
    procs: Vec<Proc>,
}

impl Drop for Group {
    fn drop(&mut self) {
        for proc in &mut self.procs {
            let _ = proc.child.kill();
            let _ = proc.child.wait();
        }
    }
}

pub fn run() -> Result<(), String> {
    let bin = std::env::current_exe().map_err(|e| e.to_string())?;
    println!("binary={}", bin.display());
    println!("profile=release-if-built-with---release");
    println!("durability=fsync-before-ack");
    println!("cluster_nodes=3");
    println!("clients=4");
    println!("ops_per_client=25");
    println!("value_bytes=32");
    println!("connection=new TCP connection per request");
    println!("hardware_uname={}", cmd_out("uname", &["-msr"])?);
    println!("hardware_cpu={}", cmd_out("sysctl", &["-n", "machdep.cpu.brand_string"])?);
    println!("hardware_mem_bytes={}", cmd_out("sysctl", &["-n", "hw.memsize"])?);
    println!("rustc={}", cmd_out("rustc", &["--version"])?);
    let root = std::env::temp_dir().join(format!("quorum-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let mut group = spawn_group(&bin, &root, 18_000 + (std::process::id() as u16 % 500) * 10)?;
    let leader = wait_leader(&group, Duration::from_secs(8))?;
    println!("warmup_ops=16");
    for i in 0..16 {
        put_ok(&group, leader, &format!("warm-{i}"), &vec![b'w'; 32])?;
    }
    let fsync_before = fsync_sum(&group)?;
    println!("fsync_total_sum_before_measured_writes={fsync_before}");
    let write = par_ops(&group, leader, 4, 25, true)?;
    print_stat_measured("normal_write", &write);
    let read = par_ops(&group, leader, 4, 25, false)?;
    print_stat_measured("normal_read", &read);
    let fsync_after_rw = fsync_sum(&group)?;
    println!(
        "fsync_total_sum_after_normal={fsync_after_rw} delta={}",
        fsync_after_rw.saturating_sub(fsync_before)
    );

    let killed = leader;
    let t_kill = Instant::now();
    kill_id(&mut group, killed)?;
    let mut failover = None;
    let mut attempts = 0u64;
    while t_kill.elapsed() < Duration::from_secs(10) {
        attempts += 1;
        let target = group
            .procs
            .iter()
            .find(|p| p.id != killed)
            .map(|p| p.id);
        let Some(target) = target else {
            break;
        };
        let started = Instant::now();
        match put(&group, target, "fail-key", &vec![b'f'; 32]) {
            Ok(reply) if reply.status == 200 => {
                failover = Some((t_kill.elapsed(), started.elapsed(), attempts));
                break;
            }
            Ok(reply) if reply.status == 307 => {
                if let Some(id) = leader_from(&group, &reply) {
                    if id != killed {
                        let started = Instant::now();
                        if let Ok(again) = put(&group, id, "fail-key", &vec![b'f'; 32]) {
                            if again.status == 200 {
                                failover = Some((t_kill.elapsed(), started.elapsed(), attempts));
                                break;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        thread::sleep(Duration::from_millis(15));
    }
    let (fail_over, fail_lat, fail_attempts) =
        failover.ok_or_else(|| format!("no write succeeded after killing leader {killed}"))?;
    println!("failover_ms={:.3}", ms(fail_over));
    println!("failover_write_ms={:.3}", ms(fail_lat));
    println!("failover_attempts={fail_attempts}");
    println!("killed_leader={killed}");

    let leader = wait_leader(&group, Duration::from_secs(8))?;
    for i in 0..20 {
        put_ok(&group, leader, &format!("gap-{i}"), &vec![b'g'; 32])?;
    }
    let target = metric_of(&group, leader, "quorum_commit")?.unwrap_or(0);
    println!("recovery_target_commit={target}");
    let t_up = Instant::now();
    let restarted = respawn(&bin, &root, &group, killed)?;
    let ready = t_up.elapsed();
    println!("recovery_ready_ms={:.3}", ms(ready));
    group.procs.push(restarted);
    let follower = killed;
    let mut ops = 0u64;
    let mut lats = Vec::new();
    let mut caught = None;
    while t_up.elapsed() < Duration::from_secs(15) {
        let started = Instant::now();
        if put(&group, leader, &format!("rec-{ops}"), &vec![b'r'; 32])
            .map(|r| r.status == 200)
            .unwrap_or(false)
        {
            ops += 1;
            lats.push(started.elapsed());
        }
        if let Some(commit) = metric_of(&group, follower, "quorum_commit")? {
            if commit >= target && caught.is_none() {
                caught = Some(t_up.elapsed());
                break;
            }
        }
        if ops > 0 && ops % 5 == 0 {
            thread::sleep(Duration::from_millis(5));
        }
    }
    let caught = caught.ok_or("follower did not catch up")?;
    println!("recovery_ms={:.3}", ms(caught));
    println!("recovery_writes={ops}");
    println!(
        "recovery_window_sec={:.3}",
        caught.as_secs_f64()
    );
    if !lats.is_empty() && caught.as_secs_f64() > 0.0 {
        println!(
            "recovery_write_ops_per_sec={:.2}",
            ops as f64 / caught.as_secs_f64()
        );
        print_lat("recovery_write", &mut lats);
    }
    let fsync_after = fsync_sum(&group)?;
    println!("fsync_total_sum_end={fsync_after}");
    println!("data_dir={}", root.display());
    Ok(())
}

pub fn smoke(bin: &Path) -> Result<(), String> {
    let root = std::env::temp_dir().join(format!(
        "quorum-smoke-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let group = spawn_group(bin, &root, 19_000 + (std::process::id() as u16 % 400) * 10)?;
    let leader = wait_leader(&group, Duration::from_secs(8))?;
    put_ok(&group, leader, "smoke", b"value")?;
    let got = get(&group, leader, "smoke")?;
    if got.status != 200 || got.body != b"value" {
        return Err(format!("read status {} body {:?}", got.status, got.body));
    }
    let other = group.procs.iter().find(|p| p.id != leader).unwrap();
    let redirected = request(&other.http, "PUT", "/kv/smoke", b"x")?;
    if redirected.status != 307 {
        return Err(format!(
            "follower returned {} headers {:?}",
            redirected.status, redirected.headers
        ));
    }
    let _ = std::fs::remove_dir_all(&root);
    Ok(())
}

fn print_lat(name: &str, samples: &mut [Duration]) {
    samples.sort();
    let n = samples.len();
    println!("{name}_p50_ms={:.3}", ms(samples[n / 2]));
    println!(
        "{name}_p99_ms={:.3}",
        ms(samples[((n - 1) * 99) / 100])
    );
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

struct Measured {
    lats: Vec<Duration>,
    wall: Duration,
    ops: usize,
}

fn par_ops(group: &Group, leader: u8, threads: usize, each: usize, writes: bool) -> Result<Measured, String> {
    let addr = http_of(group, leader)?;
    let started = Instant::now();
    let mut joins = Vec::new();
    for worker in 0..threads {
        let addr = addr;
        joins.push(thread::spawn(move || {
            let mut lats = Vec::new();
            for i in 0..each {
                let key = if writes {
                    format!("w{worker}-{i}")
                } else {
                    format!("w{}-{}", worker % 4, i % each)
                };
                let t = Instant::now();
                let reply = if writes {
                    request(&addr, "PUT", &format!("/kv/{key}"), &vec![b'x'; 32])
                } else {
                    request(&addr, "GET", &format!("/kv/{key}"), b"")
                };
                let reply = reply.map_err(|e| e)?;
                if writes && reply.status != 200 {
                    return Err(format!("write status {}", reply.status));
                }
                if !writes && reply.status != 200 {
                    return Err(format!("read status {}", reply.status));
                }
                lats.push(t.elapsed());
            }
            Ok(lats)
        }));
    }
    let mut lats = Vec::new();
    for join in joins {
        lats.extend(join.join().map_err(|_| "bench thread panicked".to_string())??);
    }
    let wall = started.elapsed();
    let ops = lats.len();
    Ok(Measured { lats, wall, ops })
}

fn print_stat_measured(name: &str, measured: &Measured) {
    let mut lats = measured.lats.clone();
    println!("{name}_ops={}", measured.ops);
    println!("{name}_wall_sec={:.3}", measured.wall.as_secs_f64());
    if measured.wall.as_secs_f64() > 0.0 {
        println!(
            "{name}_ops_per_sec={:.2}",
            measured.ops as f64 / measured.wall.as_secs_f64()
        );
    }
    if !lats.is_empty() {
        print_lat(name, &mut lats);
    }
}

fn spawn_group(bin: &Path, root: &Path, base: u16) -> Result<Group, String> {
    let mut procs = Vec::new();
    for id in 1..=3u8 {
        procs.push(spawn_id(bin, root, base, id)?);
    }
    Ok(Group { procs })
}

fn spawn_id(bin: &Path, root: &Path, base: u16, id: u8) -> Result<Proc, String> {
    let raft_port = base + id as u16;
    let http_port = base + 100 + id as u16;
    let data = root.join(id.to_string());
    std::fs::create_dir_all(&data).map_err(|e| e.to_string())?;
    let mut peers = Vec::new();
    let mut https = Vec::new();
    for other in 1..=3u8 {
        if other == id {
            continue;
        }
        peers.push(format!(
            "{other}=127.0.0.1:{}",
            base + other as u16
        ));
        https.push(format!(
            "{other}=http://127.0.0.1:{}",
            base + 100 + other as u16
        ));
    }
    let log_path = root.join(format!("node-{id}.log"));
    let log = std::fs::File::create(&log_path).map_err(|e| e.to_string())?;
    let mut child = Command::new(bin)
        .arg("node")
        .arg("--id")
        .arg(id.to_string())
        .arg("--data")
        .arg(&data)
        .arg("--raft")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--http")
        .arg(format!("127.0.0.1:{http_port}"))
        .arg("--advertise")
        .arg(format!("http://127.0.0.1:{http_port}"))
        .arg("--peers")
        .arg(peers.join(","))
        .arg("--peer-http")
        .arg(https.join(","))
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log))
        .spawn()
        .map_err(|e| e.to_string())?;
    let mut stdout = child.stdout.take().ok_or("no stdout")?;
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut buf = String::new();
        let mut byte = [0u8; 1];
        loop {
            match stdout.read(&mut byte) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    buf.push(byte[0] as char);
                    if byte[0] == b'\n' {
                        break;
                    }
                }
            }
        }
        let _ = tx.send(buf);
    });
    let line = rx
        .recv_timeout(Duration::from_secs(8))
        .map_err(|_| format!("node {id} did not print ready\n{}", tail(&log_path)))?;
    if !line.contains("quorum ready") {
        return Err(format!("node {id} said {line}\n{}", tail(&log_path)));
    }
    Ok(Proc {
        id,
        http: SocketAddr::from(([127, 0, 0, 1], http_port)),
        child,
        log_path,
    })
}

fn respawn(bin: &Path, root: &Path, group: &Group, id: u8) -> Result<Proc, String> {
    let survivor = group
        .procs
        .iter()
        .find(|p| p.id != id)
        .ok_or("no survivor")?;
    let base = survivor.http.port() - 100 - survivor.id as u16;
    spawn_id(bin, root, base, id)
}

fn kill_id(group: &mut Group, id: u8) -> Result<(), String> {
    let pos = group
        .procs
        .iter()
        .position(|p| p.id == id)
        .ok_or("missing proc")?;
    let mut proc = group.procs.remove(pos);
    proc.child.kill().map_err(|e| e.to_string())?;
    let _ = proc.child.wait();
    Ok(())
}

fn wait_leader(group: &Group, limit: Duration) -> Result<u8, String> {
    let started = Instant::now();
    while started.elapsed() < limit {
        for proc in &group.procs {
            if let Ok(Some(role)) = metric_of(group, proc.id, "quorum_role") {
                if role == 3 {
                    return Ok(proc.id);
                }
            }
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(format!("no leader\n{}", logs(group)))
}

fn fsync_sum(group: &Group) -> Result<u64, String> {
    let mut sum = 0;
    for proc in &group.procs {
        sum += metric_of(group, proc.id, "quorum_fsync_total")?.unwrap_or(0);
    }
    Ok(sum)
}

fn metric_of(group: &Group, id: u8, name: &str) -> Result<Option<u64>, String> {
    let addr = http_of(group, id)?;
    let reply = request(&addr, "GET", "/metrics", b"")?;
    if reply.status != 200 {
        return Ok(None);
    }
    let body = String::from_utf8_lossy(&reply.body);
    Ok(metric(&body, name))
}

fn http_of(group: &Group, id: u8) -> Result<SocketAddr, String> {
    group
        .procs
        .iter()
        .find(|p| p.id == id)
        .map(|p| p.http)
        .ok_or_else(|| format!("node {id} is down"))
}

fn put_ok(group: &Group, id: u8, key: &str, body: &[u8]) -> Result<(), String> {
    let reply = put(group, id, key, body)?;
    if reply.status != 200 {
        return Err(format!("put {key} status {}", reply.status));
    }
    Ok(())
}

fn put(group: &Group, id: u8, key: &str, body: &[u8]) -> Result<Reply, String> {
    let addr = http_of(group, id)?;
    request(&addr, "PUT", &format!("/kv/{key}"), body)
}

fn get(group: &Group, id: u8, key: &str) -> Result<Reply, String> {
    let addr = http_of(group, id)?;
    let path = if key.is_empty() {
        "/metrics".to_string()
    } else {
        format!("/kv/{key}")
    };
    request(&addr, "GET", &path, b"")
}

fn leader_from(group: &Group, reply: &Reply) -> Option<u8> {
    let header = reply
        .headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("x-quorum-leader"))
        .map(|(_, v)| v.as_str())?;
    group.procs.iter().find_map(|p| {
        let adv = format!("http://{}", p.http);
        if header.trim_end_matches('/') == adv {
            Some(p.id)
        } else {
            None
        }
    })
}

struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn request(addr: &SocketAddr, method: &str, path: &str, body: &[u8]) -> Result<Reply, String> {
    let mut stream = TcpStream::connect_timeout(addr, Duration::from_millis(400))
        .map_err(|e| format!("connect {addr}: {e}"))?;
    stream.set_nodelay(true).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| e.to_string())?;
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(reply) = parse_reply(&buf) {
            return Ok(reply);
        }
        match stream.read(&mut tmp) {
            Ok(0) => {
                return parse_reply(&buf).ok_or_else(|| format!("short reply from {addr}"));
            }
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                return parse_reply(&buf).ok_or_else(|| format!("timeout reading {addr}"));
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("read {addr}: {err}")),
        }
    }
}

fn parse_reply(buf: &[u8]) -> Option<Reply> {
    let end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&buf[..end]).ok()?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next()?;
    let status = status_line.split_whitespace().nth(1)?.parse().ok()?;
    let mut headers = Vec::new();
    let mut length = 0usize;
    for line in lines {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            length = value.trim().parse().ok()?;
        }
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }
    let total = end + 4 + length;
    if buf.len() < total {
        return None;
    }
    Some(Reply {
        status,
        headers,
        body: buf[end + 4..total].to_vec(),
    })
}

fn metric(body: &str, name: &str) -> Option<u64> {
    for line in body.lines() {
        let mut parts = line.split_whitespace();
        if parts.next() == Some(name) {
            return parts.next()?.parse().ok();
        }
    }
    None
}

fn tail(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn logs(group: &Group) -> String {
    group
        .procs
        .iter()
        .map(|p| format!("--- node {} ---\n{}", p.id, tail(&p.log_path)))
        .collect::<Vec<_>>()
        .join("\n")
}

fn cmd_out(bin: &str, args: &[&str]) -> Result<String, String> {
    let out = Command::new(bin)
        .args(args)
        .output()
        .map_err(|e| format!("{bin}: {e}"))?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
