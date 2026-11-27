//! Raft log and hard state stored in Forge.
//!
//! `commit` prepares the records and calls `durable_commit`, which fsyncs the
//! WAL once before it returns. Callers send an RPC response or a client
//! acknowledgement only after that returns.

use std::sync::Arc;

use forge_core::{Clock, Disk, Prepared, Store, StoreOpts, UserOp};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Noop,
    Put {
        client: u64,
        req: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub cmd: Command,
}

pub struct ForgeLog {
    store: Store,
    disk: Disk,
    entries: Vec<Entry>,
    term: u64,
    voted_for: Option<u8>,
    end: u64,
}

impl ForgeLog {
    pub fn open(disk: Disk, clock: Arc<dyn Clock>) -> Result<Self, String> {
        let store = Store::open(disk.clone(), clock, opts()).map_err(|e| e.to_string())?;
        let mut log = Self {
            store,
            disk,
            entries: vec![Entry {
                index: 0,
                term: 0,
                cmd: Command::Noop,
            }],
            term: 0,
            voted_for: None,
            end: 0,
        };
        if let Some(raw) = log.store.get(hs_key()).map_err(|e| e.to_string())? {
            let (term, voted, end) = decode_hs(&raw)?;
            log.term = term;
            log.voted_for = voted;
            log.end = end;
            for index in 1..=end {
                let raw = log
                    .store
                    .get(&entry_key(index))
                    .map_err(|e| e.to_string())?
                    .ok_or_else(|| format!("raft log missing index {index}"))?;
                let entry = decode_entry(index, &raw)?;
                log.entries.push(entry);
            }
        }
        Ok(log)
    }

    pub fn disk(&self) -> Disk {
        self.disk.clone()
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn voted_for(&self) -> Option<u8> {
        self.voted_for
    }

    pub fn last_index(&self) -> u64 {
        self.end
    }

    pub fn last_term(&self) -> u64 {
        self.term_at(self.end).unwrap_or(0)
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.entries.get(index as usize).map(|e| e.term)
    }

    pub fn matches(&self, index: u64, term: u64) -> bool {
        self.term_at(index) == Some(term)
    }

    pub fn entry(&self, index: u64) -> Option<&Entry> {
        if index == 0 {
            return None;
        }
        self.entries.get(index as usize)
    }

    pub fn slice_from(&self, index: u64, max: usize) -> Vec<Entry> {
        if index == 0 || index > self.end || max == 0 {
            return Vec::new();
        }
        let start = index as usize;
        let stop = ((self.end as usize) + 1).min(start + max);
        self.entries[start..stop].to_vec()
    }

    pub fn find_put(&self, client: u64, req: u64) -> Option<u64> {
        for entry in self.entries.iter().skip(1) {
            if let Command::Put {
                client: c, req: r, ..
            } = &entry.cmd
            {
                if *c == client && *r == req {
                    return Some(entry.index);
                }
            }
        }
        None
    }

    pub fn fsync_total(&self) -> u64 {
        self.store.stats().fsync_total
    }

    pub fn set_term(&mut self, term: u64, voted_for: Option<u8>) -> Result<(), String> {
        self.write_hs(term, voted_for, self.end)?;
        self.term = term;
        self.voted_for = voted_for;
        Ok(())
    }

    pub fn append(&mut self, term: u64, cmd: Command) -> Result<u64, String> {
        let index = self.end + 1;
        let entry = Entry { index, term, cmd };
        self.commit(vec![
            UserOp::Set {
                key: entry_key(index),
                value: encode_entry(&entry),
                expiry: 0,
            },
            UserOp::Set {
                key: hs_key().to_vec(),
                value: encode_hs(self.term, self.voted_for, index),
                expiry: 0,
            },
        ])?;
        self.end = index;
        self.entries.push(entry);
        Ok(index)
    }

    /// Drop `index` and every entry after it. The hard-state end moves first,
    /// in the same fsync as the deletes, so a crash cannot resurrect the suffix.
    pub fn truncate_from(&mut self, index: u64) -> Result<(), String> {
        if index == 0 || index > self.end {
            return Ok(());
        }
        let old = self.end;
        let new_end = index - 1;
        let mut ops = vec![UserOp::Set {
            key: hs_key().to_vec(),
            value: encode_hs(self.term, self.voted_for, new_end),
            expiry: 0,
        }];
        for i in index..=old {
            ops.push(UserOp::Del {
                key: entry_key(i),
            });
        }
        self.commit(ops)?;
        self.end = new_end;
        self.entries.truncate(index as usize);
        Ok(())
    }

    fn write_hs(&self, term: u64, voted: Option<u8>, end: u64) -> Result<(), String> {
        self.commit(vec![UserOp::Set {
            key: hs_key().to_vec(),
            value: encode_hs(term, voted, end),
            expiry: 0,
        }])
    }

    fn commit(&self, ops: Vec<UserOp>) -> Result<(), String> {
        let mut recs = Vec::new();
        for op in ops {
            match self.store.prepare(op).map_err(|e| e.to_string())? {
                Prepared::Immediate(_) => {}
                Prepared::Durable { record, .. } => recs.push(record),
            }
        }
        if recs.is_empty() {
            return Ok(());
        }
        self.store
            .durable_commit(&recs)
            .map_err(|e| e.to_string())?;
        for rec in &recs {
            self.store.make_visible(rec).map_err(|e| e.to_string())?;
        }
        if let Some(frozen) = self.store.freeze_if_needed() {
            self.store.flush_frozen(frozen).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

fn opts() -> StoreOpts {
    StoreOpts {
        memtable_bytes: 32 * 1024 * 1024,
        compact_threshold: 32,
        wal_rotate_bytes: 32 * 1024 * 1024,
        exit_on_failpoint: false,
        sync_gate: None,
    }
}

fn hs_key() -> &'static [u8] {
    b"h"
}

fn entry_key(index: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(b'e');
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn encode_hs(term: u64, voted: Option<u8>, end: u64) -> Vec<u8> {
    let mut raw = Vec::with_capacity(17);
    raw.extend_from_slice(&term.to_be_bytes());
    raw.push(voted.unwrap_or(0));
    raw.extend_from_slice(&end.to_be_bytes());
    raw
}

fn decode_hs(raw: &[u8]) -> Result<(u64, Option<u8>, u64), String> {
    if raw.len() != 17 {
        return Err(format!("hard state is {} bytes", raw.len()));
    }
    let term = u64::from_be_bytes(raw[0..8].try_into().unwrap());
    let voted = match raw[8] {
        0 => None,
        id => Some(id),
    };
    let end = u64::from_be_bytes(raw[9..17].try_into().unwrap());
    Ok((term, voted, end))
}

pub fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&entry.term.to_be_bytes());
    match &entry.cmd {
        Command::Noop => raw.push(0),
        Command::Put {
            client,
            req,
            key,
            value,
        } => {
            raw.push(1);
            raw.extend_from_slice(&client.to_be_bytes());
            raw.extend_from_slice(&req.to_be_bytes());
            raw.extend_from_slice(&(key.len() as u32).to_be_bytes());
            raw.extend_from_slice(key);
            raw.extend_from_slice(&(value.len() as u32).to_be_bytes());
            raw.extend_from_slice(value);
        }
    }
    raw
}

pub fn decode_entry(index: u64, raw: &[u8]) -> Result<Entry, String> {
    if raw.len() < 9 {
        return Err(format!("short entry {index}"));
    }
    let term = u64::from_be_bytes(raw[0..8].try_into().unwrap());
    let cmd = match raw[8] {
        0 => Command::Noop,
        1 => {
            let mut at = 9;
            let client = take_u64(raw, &mut at)?;
            let req = take_u64(raw, &mut at)?;
            let key = take_bytes(raw, &mut at)?;
            let value = take_bytes(raw, &mut at)?;
            Command::Put {
                client,
                req,
                key,
                value,
            }
        }
        other => return Err(format!("unknown entry tag {other}")),
    };
    Ok(Entry { index, term, cmd })
}

fn take_u64(raw: &[u8], at: &mut usize) -> Result<u64, String> {
    if *at + 8 > raw.len() {
        return Err("truncated u64".into());
    }
    let n = u64::from_be_bytes(raw[*at..*at + 8].try_into().unwrap());
    *at += 8;
    Ok(n)
}

fn take_bytes(raw: &[u8], at: &mut usize) -> Result<Vec<u8>, String> {
    if *at + 4 > raw.len() {
        return Err("truncated length".into());
    }
    let n = u32::from_be_bytes(raw[*at..*at + 4].try_into().unwrap()) as usize;
    *at += 4;
    if *at + n > raw.len() {
        return Err("truncated bytes".into());
    }
    let out = raw[*at..*at + n].to_vec();
    *at += n;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use forge_core::{fullfsync_calls, ManualClock};

    fn clock() -> Arc<dyn Clock> {
        Arc::new(ManualClock::new(0))
    }

    #[test]
    fn append_fsyncs_before_it_returns() {
        let disk = Disk::memory();
        let mut log = ForgeLog::open(disk.clone(), clock()).unwrap();
        let before = disk.sync_ok();
        log.append(
            1,
            Command::Put {
                client: 1,
                req: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
        )
        .unwrap();
        assert!(disk.sync_ok() > before);
        assert!(log.fsync_total() >= 1);
    }

    #[test]
    fn reopen_reads_fsynced_entries() {
        let disk = Disk::memory();
        let mut log = ForgeLog::open(disk.clone(), clock()).unwrap();
        log.set_term(4, Some(2)).unwrap();
        log.append(
            4,
            Command::Put {
                client: 7,
                req: 3,
                key: b"a".to_vec(),
                value: b"b".to_vec(),
            },
        )
        .unwrap();
        log.append(4, Command::Noop).unwrap();
        let end = log.last_index();
        drop(log);
        let log = ForgeLog::open(disk, clock()).unwrap();
        assert_eq!(log.term(), 4);
        assert_eq!(log.voted_for(), Some(2));
        assert_eq!(log.last_index(), end);
        match &log.entry(1).unwrap().cmd {
            Command::Put { key, value, .. } => {
                assert_eq!(key, b"a");
                assert_eq!(value, b"b");
            }
            Command::Noop => panic!("index 1 was a put"),
        }
        assert_eq!(log.entry(2).unwrap().cmd, Command::Noop);
    }

    #[test]
    fn truncate_does_not_come_back_after_reopen() {
        let disk = Disk::memory();
        let mut log = ForgeLog::open(disk.clone(), clock()).unwrap();
        log.append(1, Command::Noop).unwrap();
        log.append(1, Command::Noop).unwrap();
        log.truncate_from(2).unwrap();
        assert_eq!(log.last_index(), 1);
        drop(log);
        let log = ForgeLog::open(disk, clock()).unwrap();
        assert_eq!(log.last_index(), 1);
        assert!(log.entry(2).is_none());
    }

    #[test]
    fn real_disk_append_uses_fullfsync() {
        let dir = std::env::temp_dir().join(format!(
            "quorum-log-{}-{}",
            std::process::id(),
            fullfsync_calls()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let disk = Disk::real(&dir).unwrap();
        let before = fullfsync_calls();
        let mut log = ForgeLog::open(disk, Arc::new(ManualClock::new(0))).unwrap();
        log.append(1, Command::Noop).unwrap();
        assert!(
            fullfsync_calls() > before,
            "F_FULLFSYNC did not increase across open+append"
        );
        drop(log);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
