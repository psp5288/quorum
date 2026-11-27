//! Deterministic cluster simulation.
//!
//! Time, message delay, drops, partitions, crashes, and restarts come from a
//! fixed seed. On failure the seed is printed and written under `failing-seeds/`.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use forge_core::{Clock, Disk, ManualClock};

use crate::check::{check_linearizable, Call, Op};
use crate::logstore::ForgeLog;
use crate::raft::{ClientResult, Effect, Node};
use crate::rpc::Rpc;

#[derive(Clone, Debug)]
enum Kind {
    Write { key: Vec<u8>, value: Vec<u8> },
    Read { key: Vec<u8> },
}

struct Inflight {
    cookie: u64,
    start: u64,
    accepted: bool,
    kind: Kind,
}

struct Client {
    id: u64,
    next_req: u64,
    hold_req: u64,
    hint: Option<u8>,
    op_start: u64,
    next_at: u64,
    hold: Option<Kind>,
    inflight: Option<Inflight>,
}

struct Envelope {
    at: u64,
    seq: u64,
    from: u8,
    to: u8,
    rpc: Rpc,
}

struct World {
    seed: u64,
    rng: u64,
    faults: bool,
    now: u64,
    seq: u64,
    next_cookie: u64,
    max_records: usize,
    nodes: [Option<Node>; 3],
    disks: [Disk; 3],
    clocks: [Arc<ManualClock>; 3],
    restarts: [u64; 3],
    down_until: [u64; 3],
    partition_until: [u64; 3],
    net: Vec<Envelope>,
    clients: Vec<Client>,
    history: Vec<Call>,
    trace: Vec<String>,
}

pub fn run(seed: u64, faults: bool) -> Result<(), String> {
    let mut world = World::new(seed, faults)?;
    let steps = if faults { 320 } else { 60 };
    for _ in 0..steps {
        world.step()?;
    }
    let mark = world.history.len();
    world.heal()?;
    world.max_records = world.history.len() + 8;
    let mut agreed = false;
    for _ in 0..400 {
        world.step()?;
        if world.converged() && world.certain_since(mark) {
            agreed = true;
            break;
        }
    }
    for _ in 0..80 {
        if world.clients.iter().all(|c| c.inflight.is_none() && c.hold.is_none()) {
            break;
        }
        world.step()?;
    }
    if !world.converged() {
        return world.fail("nodes did not agree after the partition healed");
    }
    if !agreed && !world.certain_since(mark) {
        return world.fail("no acknowledged write after the partition healed");
    }
    if let Err(err) = check_linearizable(&world.history) {
        return world.fail(&err);
    }
    let _ = agreed;
    Ok(())
}

impl World {
    fn new(seed: u64, faults: bool) -> Result<Self, String> {
        let mut world = Self {
            seed,
            rng: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
            faults,
            now: 0,
            seq: 0,
            next_cookie: 0,
            max_records: if faults { 16 } else { 8 },
            nodes: [None, None, None],
            disks: [Disk::memory(), Disk::memory(), Disk::memory()],
            clocks: [
                Arc::new(ManualClock::new(0)),
                Arc::new(ManualClock::new(0)),
                Arc::new(ManualClock::new(0)),
            ],
            restarts: [0; 3],
            down_until: [0; 3],
            partition_until: [0; 3],
            net: Vec::new(),
            clients: vec![
                Client {
                    id: 1,
                    next_req: 1,
                    hold_req: 0,
                    hint: None,
                    op_start: 0,
                    next_at: 0,
                    hold: None,
                    inflight: None,
                },
                Client {
                    id: 2,
                    next_req: 1,
                    hold_req: 0,
                    hint: None,
                    op_start: 0,
                    next_at: 0,
                    hold: None,
                    inflight: None,
                },
            ],
            history: Vec::new(),
            trace: Vec::new(),
        };
        for id in 1..=3 {
            world.boot(id)?;
        }
        Ok(world)
    }

    fn boot(&mut self, id: u8) -> Result<(), String> {
        let idx = (id - 1) as usize;
        self.nodes[idx] = None;
        self.restarts[idx] = self.restarts[idx].wrapping_add(1);
        let clock: Arc<dyn Clock> = self.clocks[idx].clone();
        let log = ForgeLog::open(self.disks[idx].clone(), clock)?;
        let rng = self
            .seed
            .wrapping_mul(1000)
            .wrapping_add(id as u64 * 17)
            .wrapping_add(self.restarts[idx] << 8);
        let node = Node::start(id, vec![1, 2, 3], log, self.now, rng)?;
        self.nodes[idx] = Some(node);
        self.trace(format!("start node {id}"));
        Ok(())
    }

    fn step(&mut self) -> Result<(), String> {
        self.now += 5;
        self.expire_partitions();
        if self.faults {
            self.maybe_fault()?;
        }
        self.ensure_up()?;
        self.tick_all()?;
        self.drain()?;
        self.poll_clients()?;
        self.drain()?;
        Ok(())
    }

    fn heal(&mut self) -> Result<(), String> {
        self.faults = false;
        for i in 0..3 {
            self.partition_until[i] = 0;
            if self.nodes[i].is_none() {
                self.down_until[i] = 0;
            }
        }
        self.ensure_up()?;
        self.trace("heal".into());
        Ok(())
    }

    fn maybe_fault(&mut self) -> Result<(), String> {
        let roll = self.rng() % 100;
        if roll < 4 {
            let id = (self.rng() as u8 % 3) + 1;
            if self.nodes[(id - 1) as usize].is_some() {
                let dur = 120 + self.rng() % 280;
                self.nodes[(id - 1) as usize] = None;
                self.down_until[(id - 1) as usize] = self.now + dur;
                self.trace(format!("crash {id} for {dur}ms"));
            }
        } else if roll < 8 {
            let id = (self.rng() as u8 % 3) + 1;
            let dur = 150 + self.rng() % 350;
            self.partition_until[(id - 1) as usize] = self.now + dur;
            self.trace(format!("partition {id} for {dur}ms"));
        }
        Ok(())
    }

    fn expire_partitions(&mut self) {
        for i in 0..3 {
            if self.partition_until[i] != 0 && self.now >= self.partition_until[i] {
                self.partition_until[i] = 0;
            }
        }
    }

    fn ensure_up(&mut self) -> Result<(), String> {
        for id in 1..=3 {
            let idx = (id - 1) as usize;
            if self.nodes[idx].is_none() && self.now >= self.down_until[idx] {
                self.boot(id)?;
            }
        }
        Ok(())
    }

    fn tick_all(&mut self) -> Result<(), String> {
        for id in 1..=3 {
            let idx = (id - 1) as usize;
            if self.nodes[idx].is_none() {
                continue;
            }
            let effects = self.nodes[idx].as_mut().unwrap().tick(self.now)?;
            self.apply_effects(id, effects)?;
        }
        Ok(())
    }

    fn drain(&mut self) -> Result<(), String> {
        for _ in 0..20_000 {
            let Some(env) = self.pop_due() else {
                return Ok(());
            };
            self.deliver(env)?;
        }
        if self.net.iter().any(|env| env.at <= self.now) {
            return self.fail("message drain did not finish");
        }
        Ok(())
    }

    fn pop_due(&mut self) -> Option<Envelope> {
        let mut best: Option<usize> = None;
        for (i, env) in self.net.iter().enumerate() {
            if env.at > self.now {
                continue;
            }
            let take = match best {
                None => true,
                Some(b) => {
                    env.at < self.net[b].at
                        || (env.at == self.net[b].at && env.seq < self.net[b].seq)
                }
            };
            if take {
                best = Some(i);
            }
        }
        best.map(|i| self.net.remove(i))
    }

    fn deliver(&mut self, env: Envelope) -> Result<(), String> {
        let idx = (env.to - 1) as usize;
        if self.nodes[idx].is_none() || self.blocked(env.from, env.to) {
            return Ok(());
        }
        let effects = self.nodes[idx].as_mut().unwrap().on_rpc(env.rpc, self.now)?;
        self.apply_effects(env.to, effects)
    }

    fn enqueue(&mut self, from: u8, to: u8, rpc: Rpc) {
        if self.faults && self.rng() % 10 == 0 {
            return;
        }
        if self.blocked(from, to) {
            return;
        }
        let delay = if self.faults { self.rng() % 41 } else { 0 };
        self.seq += 1;
        self.net.push(Envelope {
            at: self.now + delay,
            seq: self.seq,
            from,
            to,
            rpc,
        });
    }

    fn blocked(&self, a: u8, b: u8) -> bool {
        let left = self.partition_until[(a - 1) as usize];
        let right = self.partition_until[(b - 1) as usize];
        (left != 0 && self.now < left) || (right != 0 && self.now < right)
    }

    fn apply_effects(&mut self, from: u8, effects: Vec<Effect>) -> Result<(), String> {
        for effect in effects {
            match effect {
                Effect::Send { to, rpc } => self.enqueue(from, to, rpc),
                Effect::Respond { cookie, result } => self.on_response(cookie, result),
                Effect::Log { msg, .. } => self.trace(format!("n{from} {msg}")),
            }
        }
        Ok(())
    }

    fn poll_clients(&mut self) -> Result<(), String> {
        for c in 0..self.clients.len() {
            self.timeout_client(c);
            if self.clients[c].inflight.is_some() {
                continue;
            }
            if self.clients[c].hold.is_none()
                && (self.now < self.clients[c].next_at || self.history.len() >= self.max_records)
            {
                continue;
            }
            if self.clients[c].hold.is_none() {
                self.arm_client(c);
            }
            for _ in 0..3 {
                if self.clients[c].inflight.is_some() || self.clients[c].hold.is_none() {
                    break;
                }
                let Some(node) = self.pick_node(c) else {
                    break;
                };
                self.invoke(c, node)?;
            }
        }
        Ok(())
    }

    fn arm_client(&mut self, c: usize) {
        let id = self.clients[c].id;
        let key_n = (self.rng() % 3) as u8;
        let key = vec![b'k', b'0' + key_n];
        let write = self.rng() % 10 < 7;
        let kind = if write {
            let req = self.clients[c].next_req;
            self.clients[c].next_req += 1;
            self.clients[c].hold_req = req;
            Kind::Write {
                key,
                value: format!("{id}-{req}").into_bytes(),
            }
        } else {
            Kind::Read { key }
        };
        self.clients[c].op_start = self.now;
        self.clients[c].hold = Some(kind);
    }

    fn pick_node(&mut self, c: usize) -> Option<u8> {
        if let Some(hint) = self.clients[c].hint {
            if self.nodes[(hint - 1) as usize].is_some() {
                return Some(hint);
            }
        }
        let shift = (self.rng() as usize) % 3;
        let mut ids = [1u8, 2, 3];
        ids.rotate_left(shift);
        ids.into_iter()
            .find(|id| self.nodes[(*id - 1) as usize].is_some())
    }

    fn invoke(&mut self, c: usize, node: u8) -> Result<(), String> {
        let cookie = {
            self.next_cookie += 1;
            self.next_cookie
        };
        let kind = self.clients[c].hold.clone().unwrap();
        let start = self.clients[c].op_start;
        let req = self.clients[c].hold_req;
        let client_id = self.clients[c].id;
        self.clients[c].inflight = Some(Inflight {
            cookie,
            start,
            accepted: false,
            kind: kind.clone(),
        });
        let effects = {
            let raft = self.nodes[(node - 1) as usize].as_mut().unwrap();
            match &kind {
                Kind::Write { key, value } => {
                    raft.client_write(cookie, client_id, req, key.clone(), value.clone(), self.now)?
                }
                Kind::Read { key } => raft.client_read(cookie, key.clone(), self.now)?,
            }
        };
        let immediate = effects.iter().any(|effect| {
            matches!(effect, Effect::Respond { cookie: found, .. } if *found == cookie)
        });
        self.apply_effects(node, effects)?;
        if let Some(inf) = self.clients[c].inflight.as_mut() {
            if inf.cookie == cookie && !immediate && matches!(kind, Kind::Write { .. }) {
                inf.accepted = true;
            }
        }
        Ok(())
    }

    fn timeout_client(&mut self, c: usize) {
        let timed_out = self
            .clients[c]
            .inflight
            .as_ref()
            .map(|inf| self.now >= inf.start.saturating_add(800))
            .unwrap_or(false);
        if !timed_out {
            return;
        }
        let inf = self.clients[c].inflight.take().unwrap();
        if inf.accepted {
            if let Kind::Write { key, value } = inf.kind {
                self.history.push(Call {
                    start: inf.start,
                    end: self.now,
                    op: Op::Write {
                        key,
                        value,
                        uncertain: true,
                    },
                });
                self.trace(format!("uncertain write client {}", self.clients[c].id));
            }
        }
        self.clients[c].hold = None;
        self.clients[c].next_at = self.now + 10;
    }

    fn on_response(&mut self, cookie: u64, result: ClientResult) {
        let Some(c) = self
            .clients
            .iter()
            .position(|client| client.inflight.as_ref().map(|inf| inf.cookie) == Some(cookie))
        else {
            return;
        };
        let inf = match self.clients[c].inflight.take() {
            Some(inf) if inf.cookie == cookie => inf,
            other => {
                self.clients[c].inflight = other;
                return;
            }
        };
        match result {
            ClientResult::Redirect { leader } => {
                if let Some(id) = leader {
                    if (1..=3).contains(&id) {
                        self.clients[c].hint = Some(id);
                    }
                }
                if inf.accepted {
                    if let Kind::Write { key, value } = inf.kind {
                        self.history.push(Call {
                            start: inf.start,
                            end: self.now,
                            op: Op::Write {
                                key,
                                value,
                                uncertain: true,
                            },
                        });
                    }
                    self.clients[c].hold = None;
                    self.clients[c].next_at = self.now + 10;
                }
            }
            ClientResult::WriteOk { .. } => {
                if let Kind::Write { key, value } = inf.kind {
                    self.history.push(Call {
                        start: inf.start,
                        end: self.now,
                        op: Op::Write {
                            key,
                            value,
                            uncertain: false,
                        },
                    });
                }
                self.clients[c].hold = None;
                self.clients[c].next_at = self.now + 5;
            }
            ClientResult::ReadOk { value } => {
                if let Kind::Read { key } = inf.kind {
                    self.history.push(Call {
                        start: inf.start,
                        end: self.now,
                        op: Op::Read { key, value },
                    });
                }
                self.clients[c].hold = None;
                self.clients[c].next_at = self.now + 5;
            }
        }
    }

    fn converged(&self) -> bool {
        if self.nodes.iter().any(|node| node.is_none()) {
            return false;
        }
        let first = self.nodes[0].as_ref().unwrap();
        let commit = first.commit();
        let state = first.state();
        for node in &self.nodes {
            let node = node.as_ref().unwrap();
            if node.commit() != commit || node.applied() != commit || node.state() != state {
                return false;
            }
            for index in 1..=commit {
                if node.entry_term(index) != first.entry_term(index)
                    || node.entry_cmd(index) != first.entry_cmd(index)
                {
                    return false;
                }
            }
        }
        true
    }

    fn certain_since(&self, mark: usize) -> bool {
        self.history[mark..].iter().any(|call| {
            matches!(
                call.op,
                Op::Write {
                    uncertain: false,
                    ..
                }
            )
        })
    }

    fn rng(&mut self) -> u64 {
        self.rng = self
            .rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        self.rng
    }

    fn trace(&mut self, line: String) {
        if self.trace.len() == 400 {
            self.trace.remove(0);
        }
        self.trace.push(format!("t={} {line}", self.now));
    }

    fn fail(&self, err: &str) -> Result<(), String> {
        let dir: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("failing-seeds");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(format!("{}.txt", self.seed));
        let mut body = format!(
            "seed={}\nfaults were enabled at the start: {}\nerror: {err}\n\n",
            self.seed, self.faults
        );
        body.push_str(&self.trace.join("\n"));
        body.push_str("\n\nhistory:\n");
        for call in &self.history {
            body.push_str(&format!("{call:?}\n"));
        }
        let _ = fs::write(&path, body);
        eprintln!(
            "quorum sim failed seed={} file={}",
            self.seed,
            path.display()
        );
        Err(format!("seed {}: {err}", self.seed))
    }
}

#[cfg(test)]
mod tests {
    use super::run;

    #[test]
    fn clean_seed_is_linearizable() {
        run(1, false).unwrap();
    }

    #[test]
    fn fault_seeds_are_linearizable() {
        for seed in 1..=24 {
            run(seed, true).unwrap();
        }
    }
}
