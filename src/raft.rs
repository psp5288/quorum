//! Single-threaded Raft node.
//!
//! The log lives in `ForgeLog`. A client write is acknowledged only after the
//! entry is committed, which requires a majority to have fsynced it.

use std::collections::{BTreeMap, BTreeSet};

use crate::logstore::{Command, ForgeLog};
use crate::rpc::Rpc;

const HEARTBEAT_MS: u64 = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower = 1,
    Candidate = 2,
    Leader = 3,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientResult {
    WriteOk { index: u64 },
    ReadOk { value: Option<Vec<u8>> },
    Redirect { leader: Option<u8> },
}

#[derive(Clone, Debug)]
pub enum Effect {
    Send { to: u8, rpc: Rpc },
    Respond { cookie: u64, result: ClientResult },
    Log { level: &'static str, msg: String },
}

#[derive(Clone, Debug)]
pub struct Status {
    pub id: u8,
    pub role: Role,
    pub term: u64,
    pub commit: u64,
    pub applied: u64,
    pub leader: Option<u8>,
    pub last_index: u64,
    pub fsync_total: u64,
}

pub struct Node {
    id: u8,
    peers: Vec<u8>,
    log: ForgeLog,
    role: Role,
    leader: Option<u8>,
    commit: u64,
    applied: u64,
    sm: BTreeMap<Vec<u8>, Vec<u8>>,
    session: BTreeMap<u64, u64>,
    next_index: BTreeMap<u8, u64>,
    match_index: BTreeMap<u8, u64>,
    votes: BTreeSet<u8>,
    election_deadline: u64,
    heartbeat_due: u64,
    rng: u64,
    commit_this_term: bool,
    pending_writes: Vec<(u64, u64)>,
    pending_reads: Vec<(u64, Vec<u8>)>,
    queued_reads: Vec<(u64, Vec<u8>)>,
    read_round: u64,
    read_acks: BTreeSet<u8>,
}

impl Node {
    pub fn start(
        id: u8,
        mut peers: Vec<u8>,
        log: ForgeLog,
        now: u64,
        rng_seed: u64,
    ) -> Result<Self, String> {
        peers.sort_unstable();
        peers.dedup();
        peers.retain(|p| *p != id);
        let mut node = Self {
            id,
            peers,
            log,
            role: Role::Follower,
            leader: None,
            commit: 0,
            applied: 0,
            sm: BTreeMap::new(),
            session: BTreeMap::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            votes: BTreeSet::new(),
            election_deadline: 0,
            heartbeat_due: 0,
            rng: if rng_seed == 0 { 1 } else { rng_seed },
            commit_this_term: false,
            pending_writes: Vec::new(),
            pending_reads: Vec::new(),
            queued_reads: Vec::new(),
            read_round: 0,
            read_acks: BTreeSet::new(),
        };
        node.election_deadline = now + node.timeout();
        Ok(node)
    }

    pub fn id(&self) -> u8 {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn leader(&self) -> Option<u8> {
        self.leader
    }

    pub fn term(&self) -> u64 {
        self.log.term()
    }

    pub fn commit(&self) -> u64 {
        self.commit
    }

    pub fn applied(&self) -> u64 {
        self.applied
    }

    pub fn last_index(&self) -> u64 {
        self.log.last_index()
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.sm.get(key).map(|v| v.as_slice())
    }

    pub fn state(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        &self.sm
    }

    pub fn entry_term(&self, index: u64) -> Option<u64> {
        self.log.term_at(index)
    }

    pub fn entry_cmd(&self, index: u64) -> Option<&Command> {
        self.log.entry(index).map(|e| &e.cmd)
    }

    pub fn status(&self) -> Status {
        Status {
            id: self.id,
            role: self.role,
            term: self.term(),
            commit: self.commit,
            applied: self.applied,
            leader: self.leader,
            last_index: self.log.last_index(),
            fsync_total: self.log.fsync_total(),
        }
    }

    pub fn tick(&mut self, now: u64) -> Result<Vec<Effect>, String> {
        if self.role != Role::Leader && now >= self.election_deadline {
            return self.start_election(now);
        }
        if self.role == Role::Leader && now >= self.heartbeat_due {
            self.heartbeat_due = now + HEARTBEAT_MS;
            return Ok(self.replicate_all());
        }
        Ok(Vec::new())
    }

    pub fn client_write(
        &mut self,
        cookie: u64,
        client: u64,
        req: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let _ = now;
        if self.role != Role::Leader {
            return Ok(vec![self.redirect(cookie)]);
        }
        if let Some(index) = self.log.find_put(client, req) {
            if index <= self.commit {
                return Ok(vec![Effect::Respond {
                    cookie,
                    result: ClientResult::WriteOk { index },
                }]);
            }
            self.pending_writes.push((index, cookie));
            return Ok(self.replicate_all());
        }
        let index = self.log.append(
            self.term(),
            Command::Put {
                client,
                req,
                key,
                value,
            },
        )?;
        self.pending_writes.push((index, cookie));
        let mut effects = self.replicate_all();
        effects.extend(self.try_commit()?);
        Ok(effects)
    }

    pub fn client_read(
        &mut self,
        cookie: u64,
        key: Vec<u8>,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let _ = now;
        if self.role != Role::Leader {
            return Ok(vec![self.redirect(cookie)]);
        }
        if !self.commit_this_term {
            self.queued_reads.push((cookie, key));
            return Ok(Vec::new());
        }
        let mut effects = Vec::new();
        self.enqueue_read(cookie, key, &mut effects);
        Ok(effects)
    }

    pub fn on_rpc(&mut self, rpc: Rpc, now: u64) -> Result<Vec<Effect>, String> {
        match rpc {
            Rpc::RequestVote {
                term,
                candidate,
                last_index,
                last_term,
            } => self.on_request_vote(term, candidate, last_index, last_term, now),
            Rpc::Vote { term, from, granted } => self.on_vote(term, from, granted, now),
            Rpc::Append {
                term,
                leader,
                prev_index,
                prev_term,
                leader_commit,
                rid,
                entries,
            } => self.on_append(
                term,
                leader,
                prev_index,
                prev_term,
                leader_commit,
                rid,
                entries,
                now,
            ),
            Rpc::AppendResp {
                term,
                from,
                success,
                index,
                rid,
            } => self.on_append_resp(term, from, success, index, rid, now),
        }
    }

    fn start_election(&mut self, now: u64) -> Result<Vec<Effect>, String> {
        let term = self.term() + 1;
        self.log.set_term(term, Some(self.id))?;
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.commit_this_term = false;
        self.election_deadline = now + self.timeout();
        let mut effects = vec![Effect::Log {
            level: "info",
            msg: format!("election term {term}"),
        }];
        let last_index = self.log.last_index();
        let last_term = self.log.last_term();
        for peer in &self.peers {
            effects.push(Effect::Send {
                to: *peer,
                rpc: Rpc::RequestVote {
                    term,
                    candidate: self.id,
                    last_index,
                    last_term,
                },
            });
        }
        if self.votes.len() >= self.majority() {
            effects.extend(self.become_leader(now)?);
        }
        Ok(effects)
    }

    fn become_leader(&mut self, now: u64) -> Result<Vec<Effect>, String> {
        if self.role != Role::Candidate {
            return Ok(Vec::new());
        }
        self.role = Role::Leader;
        self.leader = Some(self.id);
        self.commit_this_term = false;
        let last = self.log.last_index();
        for peer in &self.peers {
            self.next_index.insert(*peer, last + 1);
            self.match_index.insert(*peer, 0);
        }
        let term = self.term();
        self.log.append(term, Command::Noop)?;
        self.heartbeat_due = now + HEARTBEAT_MS;
        let mut effects = vec![Effect::Log {
            level: "info",
            msg: format!("became leader term {term}"),
        }];
        effects.extend(self.replicate_all());
        effects.extend(self.try_commit()?);
        Ok(effects)
    }

    fn on_request_vote(
        &mut self,
        term: u64,
        candidate: u8,
        last_index: u64,
        last_term: u64,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let mut effects = self.observe_term(term, now)?;
        let mut granted = false;
        if term == self.term() {
            let voted = self.log.voted_for();
            let free = voted.is_none() || voted == Some(candidate);
            if free && self.log_ok(last_index, last_term) {
                self.log.set_term(self.term(), Some(candidate))?;
                self.election_deadline = now + self.timeout();
                granted = true;
            }
        }
        effects.push(Effect::Send {
            to: candidate,
            rpc: Rpc::Vote {
                term: self.term(),
                from: self.id,
                granted,
            },
        });
        Ok(effects)
    }

    fn on_vote(
        &mut self,
        term: u64,
        from: u8,
        granted: bool,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let mut effects = self.observe_term(term, now)?;
        if self.role == Role::Candidate && term == self.term() && granted {
            self.votes.insert(from);
            if self.votes.len() >= self.majority() {
                effects.extend(self.become_leader(now)?);
            }
        }
        Ok(effects)
    }

    fn on_append(
        &mut self,
        term: u64,
        leader: u8,
        prev_index: u64,
        prev_term: u64,
        leader_commit: u64,
        rid: u64,
        entries: Vec<crate::logstore::Entry>,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let mut effects = self.observe_term(term, now)?;
        if term < self.term() {
            effects.push(self.append_resp(leader, false, self.log.last_index(), rid));
            return Ok(effects);
        }
        if self.role != Role::Follower {
            effects.extend(self.fail_pending());
            self.role = Role::Follower;
            self.votes.clear();
        }
        self.leader = Some(leader);
        self.election_deadline = now + self.timeout();
        if !self.log.matches(prev_index, prev_term) {
            effects.push(self.append_resp(leader, false, self.log.last_index(), rid));
            return Ok(effects);
        }
        let mut matched = prev_index;
        for (offset, entry) in entries.iter().enumerate() {
            let expect = prev_index + offset as u64 + 1;
            if entry.index != expect {
                effects.push(self.append_resp(leader, false, self.log.last_index(), rid));
                return Ok(effects);
            }
            match self.log.term_at(entry.index) {
                Some(have) if have == entry.term => {
                    matched = entry.index;
                }
                Some(_) => {
                    if entry.index <= self.commit {
                        effects.push(self.append_resp(leader, false, self.log.last_index(), rid));
                        return Ok(effects);
                    }
                    self.log.truncate_from(entry.index)?;
                    self.log.append(entry.term, entry.cmd.clone())?;
                    matched = entry.index;
                }
                None => {
                    if entry.index != self.log.last_index() + 1 {
                        effects.push(self.append_resp(leader, false, self.log.last_index(), rid));
                        return Ok(effects);
                    }
                    self.log.append(entry.term, entry.cmd.clone())?;
                    matched = entry.index;
                }
            }
        }
        if leader_commit > self.commit {
            self.commit = leader_commit.min(self.log.last_index());
            effects.extend(self.apply());
        }
        effects.push(self.append_resp(leader, true, matched, rid));
        Ok(effects)
    }

    fn on_append_resp(
        &mut self,
        term: u64,
        from: u8,
        success: bool,
        index: u64,
        rid: u64,
        now: u64,
    ) -> Result<Vec<Effect>, String> {
        let mut effects = self.observe_term(term, now)?;
        if self.role != Role::Leader || term != self.term() {
            return Ok(effects);
        }
        if success {
            let before = self.next_of(from);
            let matched = index.min(self.log.last_index());
            let prev = self.match_index.get(&from).copied().unwrap_or(0);
            if matched >= prev {
                self.match_index.insert(from, matched);
                self.next_index.insert(from, matched + 1);
            }
            effects.extend(self.try_commit()?);
            self.note_read_ack(from, rid, &mut effects);
            let next = self.next_of(from);
            if next > before && next <= self.log.last_index() {
                effects.push(Effect::Send {
                    to: from,
                    rpc: self.replicate(from),
                });
            }
        } else {
            let cur = self.next_of(from);
            let next = cur.saturating_sub(1).max(1);
            self.next_index.insert(from, next);
            if next < cur {
                effects.push(Effect::Send {
                    to: from,
                    rpc: self.replicate(from),
                });
            }
        }
        Ok(effects)
    }

    fn try_commit(&mut self) -> Result<Vec<Effect>, String> {
        if self.role != Role::Leader {
            return Ok(Vec::new());
        }
        let term = self.term();
        let mut index = self.log.last_index();
        let mut advanced = false;
        while index > self.commit {
            if self.log.term_at(index) == Some(term) && self.replicated(index) >= self.majority()
            {
                self.commit = index;
                self.commit_this_term = true;
                advanced = true;
                break;
            }
            index -= 1;
        }
        let mut effects = self.apply();
        if advanced {
            let queued = std::mem::take(&mut self.queued_reads);
            for (cookie, key) in queued {
                self.enqueue_read(cookie, key, &mut effects);
            }
        }
        Ok(effects)
    }

    fn apply(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        while self.applied < self.commit {
            self.applied += 1;
            let entry = match self.log.entry(self.applied) {
                Some(entry) => entry.clone(),
                None => break,
            };
            if let Command::Put {
                client,
                req,
                key,
                value,
            } = entry.cmd
            {
                let dup = self
                    .session
                    .get(&client)
                    .copied()
                    .map(|last| req <= last)
                    .unwrap_or(false);
                if !dup {
                    self.sm.insert(key, value);
                    self.session.insert(client, req);
                }
            }
            let index = self.applied;
            let mut left = Vec::new();
            for (idx, cookie) in self.pending_writes.drain(..) {
                if idx == index {
                    effects.push(Effect::Respond {
                        cookie,
                        result: ClientResult::WriteOk { index },
                    });
                } else {
                    left.push((idx, cookie));
                }
            }
            self.pending_writes = left;
        }
        effects
    }

    fn enqueue_read(&mut self, cookie: u64, key: Vec<u8>, effects: &mut Vec<Effect>) {
        let fresh = self.pending_reads.is_empty();
        if fresh {
            self.read_round = self.read_round.wrapping_add(1);
            self.read_acks.clear();
        }
        self.pending_reads.push((cookie, key));
        if fresh {
            effects.extend(self.replicate_all());
        }
        if self.majority() == 1 || self.read_acks.len() + 1 >= self.majority() {
            effects.extend(self.finish_reads());
        }
    }

    fn note_read_ack(&mut self, from: u8, rid: u64, effects: &mut Vec<Effect>) {
        if self.role != Role::Leader || rid == 0 || rid != self.read_round {
            return;
        }
        self.read_acks.insert(from);
        if self.read_acks.len() + 1 >= self.majority() {
            effects.extend(self.finish_reads());
        }
    }

    fn finish_reads(&mut self) -> Vec<Effect> {
        let pending = std::mem::take(&mut self.pending_reads);
        self.read_acks.clear();
        let mut effects = Vec::new();
        for (cookie, key) in pending {
            effects.push(Effect::Respond {
                cookie,
                result: ClientResult::ReadOk {
                    value: self.sm.get(&key).cloned(),
                },
            });
        }
        effects
    }

    fn observe_term(&mut self, term: u64, now: u64) -> Result<Vec<Effect>, String> {
        if term > self.term() {
            self.log.set_term(term, None)?;
            self.role = Role::Follower;
            self.leader = None;
            self.votes.clear();
            self.commit_this_term = false;
            self.election_deadline = now + self.timeout();
            return Ok(self.fail_pending());
        }
        Ok(Vec::new())
    }

    fn fail_pending(&mut self) -> Vec<Effect> {
        let leader = self.leader;
        let mut effects = Vec::new();
        for (_, cookie) in self.pending_writes.drain(..) {
            effects.push(Effect::Respond {
                cookie,
                result: ClientResult::Redirect { leader },
            });
        }
        for (cookie, _) in self.pending_reads.drain(..) {
            effects.push(Effect::Respond {
                cookie,
                result: ClientResult::Redirect { leader },
            });
        }
        for (cookie, _) in self.queued_reads.drain(..) {
            effects.push(Effect::Respond {
                cookie,
                result: ClientResult::Redirect { leader },
            });
        }
        self.read_acks.clear();
        effects
    }

    fn replicate_all(&self) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.role != Role::Leader {
            return effects;
        }
        for peer in &self.peers {
            effects.push(Effect::Send {
                to: *peer,
                rpc: self.replicate(*peer),
            });
        }
        effects
    }

    fn replicate(&self, to: u8) -> Rpc {
        let next = self.next_of(to);
        let prev = next.saturating_sub(1);
        let prev_term = self.log.term_at(prev).unwrap_or(0);
        Rpc::Append {
            term: self.term(),
            leader: self.id,
            prev_index: prev,
            prev_term,
            leader_commit: self.commit,
            rid: if self.pending_reads.is_empty() {
                0
            } else {
                self.read_round
            },
            entries: self.log.slice_from(next, 32),
        }
    }

    fn append_resp(&self, to: u8, success: bool, index: u64, rid: u64) -> Effect {
        Effect::Send {
            to,
            rpc: Rpc::AppendResp {
                term: self.term(),
                from: self.id,
                success,
                index,
                rid,
            },
        }
    }

    fn redirect(&self, cookie: u64) -> Effect {
        Effect::Respond {
            cookie,
            result: ClientResult::Redirect {
                leader: self.leader,
            },
        }
    }

    fn replicated(&self, index: u64) -> usize {
        let mut n = 1;
        for peer in &self.peers {
            if self.match_index.get(peer).copied().unwrap_or(0) >= index {
                n += 1;
            }
        }
        n
    }

    fn next_of(&self, peer: u8) -> u64 {
        self.next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.log.last_index() + 1)
    }

    fn log_ok(&self, last_index: u64, last_term: u64) -> bool {
        let mine = self.log.last_term();
        last_term > mine || (last_term == mine && last_index >= self.log.last_index())
    }

    fn majority(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn timeout(&mut self) -> u64 {
        self.rng = self
            .rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1);
        150 + (self.rng % 150)
    }
}
