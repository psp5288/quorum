//! Per-key register linearizability.
//!
//! Writes that timed out are uncertain: the checker may treat each one as
//! having happened or not. A call that returned is fixed.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Write {
        key: Vec<u8>,
        value: Vec<u8>,
        uncertain: bool,
    },
    Read {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Call {
    pub start: u64,
    pub end: u64,
    pub op: Op,
}

pub fn check_linearizable(calls: &[Call]) -> Result<(), String> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for call in calls {
        let key = op_key(&call.op);
        if !keys.iter().any(|k| k == key) {
            keys.push(key.to_vec());
        }
    }
    for key in keys {
        let events: Vec<Call> = calls
            .iter()
            .filter(|c| op_key(&c.op) == key.as_slice())
            .cloned()
            .collect();
        check_key(&key, &events)?;
    }
    Ok(())
}

fn op_key(op: &Op) -> &[u8] {
    match op {
        Op::Write { key, .. } | Op::Read { key, .. } => key,
    }
}

fn check_key(key: &[u8], events: &[Call]) -> Result<(), String> {
    let uncertain: Vec<usize> = events
        .iter()
        .enumerate()
        .filter_map(|(i, call)| match &call.op {
            Op::Write { uncertain: true, .. } => Some(i),
            _ => None,
        })
        .collect();
    if uncertain.len() > 16 {
        return Err(format!(
            "key {} has {} uncertain writes; checker refused the search",
            String::from_utf8_lossy(key),
            uncertain.len()
        ));
    }
    let subsets = 1u32 << uncertain.len();
    for mask in 0..subsets {
        let mut kept = Vec::new();
        for (i, call) in events.iter().enumerate() {
            if let Some(bit) = uncertain.iter().position(|u| *u == i) {
                if mask & (1 << bit) == 0 {
                    continue;
                }
            }
            kept.push(call.clone());
        }
        if linearize(&kept) {
            return Ok(());
        }
    }
    Err(format!(
        "history for key {} is not linearizable ({} calls)",
        String::from_utf8_lossy(key),
        events.len()
    ))
}

fn linearize(events: &[Call]) -> bool {
    let n = events.len();
    let mut used = vec![false; n];
    search(events, &mut used, &None, 0)
}

fn search(events: &[Call], used: &mut [bool], state: &Option<Vec<u8>>, placed: usize) -> bool {
    if placed == events.len() {
        return true;
    }
    for i in 0..events.len() {
        if used[i] {
            continue;
        }
        let ready = (0..events.len()).all(|j| {
            used[j] || events[j].end >= events[i].start
        });
        if !ready {
            continue;
        }
        match &events[i].op {
            Op::Read { value, .. } => {
                if value != state {
                    continue;
                }
                used[i] = true;
                if search(events, used, state, placed + 1) {
                    return true;
                }
                used[i] = false;
            }
            Op::Write { value, .. } => {
                used[i] = true;
                let next = Some(value.clone());
                if search(events, used, &next, placed + 1) {
                    return true;
                }
                used[i] = false;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(start: u64, end: u64, value: &str, uncertain: bool) -> Call {
        Call {
            start,
            end,
            op: Op::Write {
                key: b"a".to_vec(),
                value: value.as_bytes().to_vec(),
                uncertain,
            },
        }
    }

    fn read(start: u64, end: u64, value: Option<&str>) -> Call {
        Call {
            start,
            end,
            op: Op::Read {
                key: b"a".to_vec(),
                value: value.map(|v| v.as_bytes().to_vec()),
            },
        }
    }

    #[test]
    fn sequential_history_passes() {
        let calls = vec![write(0, 1, "1", false), read(2, 3, Some("1"))];
        check_linearizable(&calls).unwrap();
    }

    #[test]
    fn stale_read_fails() {
        let calls = vec![
            write(0, 1, "1", false),
            write(2, 3, "2", false),
            read(4, 5, Some("1")),
        ];
        assert!(check_linearizable(&calls).is_err());
    }

    #[test]
    fn overlapping_read_may_miss_the_write() {
        let calls = vec![write(0, 5, "1", false), read(3, 6, None)];
        check_linearizable(&calls).unwrap();
    }

    #[test]
    fn read_after_write_must_see_it() {
        let calls = vec![write(0, 5, "1", false), read(6, 7, None)];
        assert!(check_linearizable(&calls).is_err());
    }

    #[test]
    fn uncertain_write_can_explain_a_value() {
        let calls = vec![
            write(0, 5, "1", true),
            read(6, 7, Some("1")),
        ];
        check_linearizable(&calls).unwrap();
        let missing = vec![write(0, 5, "1", true), read(6, 7, None)];
        check_linearizable(&missing).unwrap();
    }
}
