//! Length-prefixed frames for Raft RPCs between processes.

use crate::logstore::{decode_entry, encode_entry, Command, Entry};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rpc {
    RequestVote {
        term: u64,
        candidate: u8,
        last_index: u64,
        last_term: u64,
    },
    Vote {
        term: u64,
        from: u8,
        granted: bool,
    },
    Append {
        term: u64,
        leader: u8,
        prev_index: u64,
        prev_term: u64,
        leader_commit: u64,
        rid: u64,
        entries: Vec<Entry>,
    },
    AppendResp {
        term: u64,
        from: u8,
        success: bool,
        index: u64,
        rid: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Hello { id: u8 },
    Rpc(Rpc),
}

pub fn encode_frame(frame: &Frame) -> Vec<u8> {
    let mut body = Vec::new();
    match frame {
        Frame::Hello { id } => {
            body.push(1);
            body.push(*id);
        }
        Frame::Rpc(Rpc::RequestVote {
            term,
            candidate,
            last_index,
            last_term,
        }) => {
            body.push(2);
            put_u64(&mut body, *term);
            body.push(*candidate);
            put_u64(&mut body, *last_index);
            put_u64(&mut body, *last_term);
        }
        Frame::Rpc(Rpc::Vote { term, from, granted }) => {
            body.push(3);
            put_u64(&mut body, *term);
            body.push(*from);
            body.push(u8::from(*granted));
        }
        Frame::Rpc(Rpc::Append {
            term,
            leader,
            prev_index,
            prev_term,
            leader_commit,
            rid,
            entries,
        }) => {
            body.push(4);
            put_u64(&mut body, *term);
            body.push(*leader);
            put_u64(&mut body, *prev_index);
            put_u64(&mut body, *prev_term);
            put_u64(&mut body, *leader_commit);
            put_u64(&mut body, *rid);
            put_u32(&mut body, entries.len() as u32);
            for entry in entries {
                let raw = encode_entry(entry);
                put_u64(&mut body, entry.index);
                put_u32(&mut body, raw.len() as u32);
                body.extend_from_slice(&raw);
            }
        }
        Frame::Rpc(Rpc::AppendResp {
            term,
            from,
            success,
            index,
            rid,
        }) => {
            body.push(5);
            put_u64(&mut body, *term);
            body.push(*from);
            body.push(u8::from(*success));
            put_u64(&mut body, *index);
            put_u64(&mut body, *rid);
        }
    }
    let mut out = Vec::with_capacity(4 + body.len());
    put_u32(&mut out, body.len() as u32);
    out.extend_from_slice(&body);
    out
}

pub fn decode_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let n = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    if n > 8_000_000 {
        return Err(format!("frame is {n} bytes"));
    }
    if buf.len() < 4 + n {
        return Ok(None);
    }
    let body = &buf[4..4 + n];
    if body.is_empty() {
        return Err("empty frame".into());
    }
    let frame = match body[0] {
        1 => {
            if body.len() != 2 {
                return Err("bad hello".into());
            }
            Frame::Hello { id: body[1] }
        }
        2 => {
            let mut at = 1;
            let term = take_u64(body, &mut at)?;
            let candidate = take_u8(body, &mut at)?;
            let last_index = take_u64(body, &mut at)?;
            let last_term = take_u64(body, &mut at)?;
            Frame::Rpc(Rpc::RequestVote {
                term,
                candidate,
                last_index,
                last_term,
            })
        }
        3 => {
            let mut at = 1;
            let term = take_u64(body, &mut at)?;
            let from = take_u8(body, &mut at)?;
            let granted = take_u8(body, &mut at)? == 1;
            Frame::Rpc(Rpc::Vote { term, from, granted })
        }
        4 => {
            let mut at = 1;
            let term = take_u64(body, &mut at)?;
            let leader = take_u8(body, &mut at)?;
            let prev_index = take_u64(body, &mut at)?;
            let prev_term = take_u64(body, &mut at)?;
            let leader_commit = take_u64(body, &mut at)?;
            let rid = take_u64(body, &mut at)?;
            let count = take_u32(body, &mut at)? as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let index = take_u64(body, &mut at)?;
                let len = take_u32(body, &mut at)? as usize;
                if at + len > body.len() {
                    return Err("truncated entry".into());
                }
                let entry = decode_entry(index, &body[at..at + len])?;
                at += len;
                entries.push(entry);
            }
            Frame::Rpc(Rpc::Append {
                term,
                leader,
                prev_index,
                prev_term,
                leader_commit,
                rid,
                entries,
            })
        }
        5 => {
            let mut at = 1;
            let term = take_u64(body, &mut at)?;
            let from = take_u8(body, &mut at)?;
            let success = take_u8(body, &mut at)? == 1;
            let index = take_u64(body, &mut at)?;
            let rid = take_u64(body, &mut at)?;
            Frame::Rpc(Rpc::AppendResp {
                term,
                from,
                success,
                index,
                rid,
            })
        }
        other => return Err(format!("unknown frame {other}")),
    };
    Ok(Some((frame, 4 + n)))
}

pub fn command_put(client: u64, req: u64, key: Vec<u8>, value: Vec<u8>) -> Command {
    Command::Put {
        client,
        req,
        key,
        value,
    }
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn take_u8(raw: &[u8], at: &mut usize) -> Result<u8, String> {
    if *at >= raw.len() {
        return Err("truncated u8".into());
    }
    let v = raw[*at];
    *at += 1;
    Ok(v)
}

fn take_u32(raw: &[u8], at: &mut usize) -> Result<u32, String> {
    if *at + 4 > raw.len() {
        return Err("truncated u32".into());
    }
    let v = u32::from_be_bytes(raw[*at..*at + 4].try_into().unwrap());
    *at += 4;
    Ok(v)
}

fn take_u64(raw: &[u8], at: &mut usize) -> Result<u64, String> {
    if *at + 8 > raw.len() {
        return Err("truncated u64".into());
    }
    let v = u64::from_be_bytes(raw[*at..*at + 8].try_into().unwrap());
    *at += 8;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let frame = Frame::Rpc(Rpc::Append {
            term: 3,
            leader: 1,
            prev_index: 1,
            prev_term: 2,
            leader_commit: 1,
            rid: 9,
            entries: vec![Entry {
                index: 2,
                term: 3,
                cmd: Command::Put {
                    client: 4,
                    req: 5,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            }],
        });
        let bytes = encode_frame(&frame);
        let (decoded, n) = decode_frame(&bytes).unwrap().unwrap();
        assert_eq!(n, bytes.len());
        assert_eq!(decoded, frame);
        assert!(decode_frame(&bytes[..2]).unwrap().is_none());
    }
}
