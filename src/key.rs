use std::cmp::Ordering;
use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::utils::*;
#[derive(Clone, Debug, Default)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    tail: u64, //sequence number (7 bytes) + type (1 byte)   
}

impl InternalKey {
    pub fn new(user_key: &[u8], seq_num: u64, op_type: u8) -> Self {
        InternalKey {
            user_key: user_key.to_vec(),
            tail: seq_num << 8 | (op_type as u64),
        }
    }

    pub fn get_type(&self) -> u8 {
        (self.tail & 0xff) as u8
    }

    pub fn encode_to(&self) -> Vec<u8> {
        let mut res = self.user_key.clone();
        res.extend_from_slice(&self.tail.to_le_bytes());
        res
    }

    pub fn decode_from(bytes: &[u8]) -> Self {
        let len = bytes.len();
        let user_key = bytes[0..len-8].to_vec();
        let tail = to_u64(&bytes[len-8..]);
        InternalKey {
            user_key,
            tail,
        }
    }

}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && (self.tail >> 8 == other.tail >> 8)
    }
}

impl Eq for InternalKey {}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(match self.user_key.cmp(&other.user_key) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let sa = self.tail >> 8;
                let sb = other.tail >> 8;
                if sa > sb {
                    Ordering::Less
                } else if sa == sb {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
        })
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let sa = self.tail >> 8;
                let sb = other.tail >> 8;
                if sa > sb {
                    Ordering::Less
                } else if sa == sb {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LookUpKey {
    pub key_len: u64,
    pub internal_key: InternalKey,
}

impl LookUpKey {
    pub fn new(internal_key: InternalKey) -> Self {
        LookUpKey {
            key_len: (internal_key.user_key.len() + 8) as u64,
            internal_key,
        }
    }

    pub fn encode_to(&self) -> Vec<u8> {
        let mut res = Vec::new();
        res.extend_from_slice(&self.key_len.to_le_bytes());
        res.append(&mut self.internal_key.encode_to());
        res
    }

    pub fn decode_from_bytes(bytes: &[u8], offset: &mut u64) -> Self {
        let mut cur = *offset as usize;
        let mut next = (*offset + 8) as usize;
        let key_len = to_u64(&bytes[cur..next]);
        *offset += 8;
        cur = *offset as usize;
        next = (*offset + key_len) as usize;
        let internal_key = InternalKey::decode_from(&bytes[cur..next]);
        *offset += key_len as u64;
        LookUpKey {
            key_len,
            internal_key,
        }
    }

    pub fn decode_from_file(file: &File, offset: &mut u64) -> Self {
        let mut key_len = vec![0; 8];
        file.read_exact_at(
            key_len.as_mut_slice(),
            *offset,
        ).unwrap();
        *offset += 8;
        let key_len = to_u64(&key_len);
        let mut internal_key = vec![0; key_len as usize];
        file.read_exact_at(
            internal_key.as_mut_slice(),
            *offset,
        ).unwrap();
        *offset += key_len;
        let internal_key = InternalKey::decode_from(&internal_key);
        LookUpKey {
            key_len,
            internal_key,
        }
    }

    pub fn get_user_key(&self) -> &[u8] {
        &self.internal_key.user_key
    }

    pub fn get_seq_num(&self) -> u64 {
        self.internal_key.tail >> 8
    }

    pub fn get_type(&self) -> u8 {
        self.internal_key.get_type()
    }

}

impl PartialEq for LookUpKey {
    fn eq(&self, other: &Self) -> bool {
        self.internal_key == other.internal_key
    }
}

impl Eq for LookUpKey {}

impl PartialOrd for LookUpKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.internal_key.partial_cmp(&other.internal_key)
    }
}

impl Ord for LookUpKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.internal_key.cmp(&other.internal_key)
    }
}

