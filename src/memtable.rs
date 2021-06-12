
use std::collections::HashMap;
use std::fs::remove_file;
use std::path::PathBuf;

use crate::key::InternalKey;
use crate::wal::{Log, LogEntry};

use skiplist::skipmap::SkipMap;

pub struct MemTable {
    pub inner: SkipMap<InternalKey, Vec<u8>>,
    writer: Option<Log>,
    pub size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            inner: SkipMap::new(),
            writer: None,
            size: 0,
        }
    }

    pub fn take(&mut self) -> SkipMap<InternalKey, Vec<u8>> {
        std::mem::take(&mut self.inner)
    }

    pub fn set_writer(&mut self, dir_path: &PathBuf, log_num: u64) {
        if self.writer.is_none() {
            let log = Log::open(dir_path, log_num);
            self.writer = Some(log);
        }
    }

    pub fn remove_writer(&mut self) {
        let log = self.writer.take().unwrap();
        let path = log.get_path();
        println!("remove writer {:?}", path);
        drop(log);
        remove_file(path).unwrap();
    }

    pub fn recover(&mut self, dir_path: &PathBuf, log_num: u64, trans: &mut HashMap<u64, Vec<LogEntry>>) -> u64 {
        println!("begin to recover mem_table");
        let mut max_seq_num = 0;
        let mut log = Log::open(dir_path, log_num);
        let log_entries = log.read();
        println!("log entries = {:?}", log_entries);
        //apply these entries to mem_table
        for entry in log_entries {
            match entry.entry_type {
                0 => {
                    self.insert_inner(&entry.key, &entry.value, entry.seq_num, false);
                    max_seq_num = std::cmp::max(max_seq_num, entry.seq_num);
                },
                1 => {
                    self.delete_inner(&entry.key, entry.seq_num, false);
                    max_seq_num = std::cmp::max(max_seq_num, entry.seq_num);
                },
                2 | 3 => {
                    trans.get_mut(&entry.seq_num).unwrap().push(entry);
                },
                4 => {
                    trans.insert(entry.seq_num, Vec::new());
                }
                5 => {
                    for entry in trans.remove(&entry.seq_num).unwrap() {
                        if entry.entry_type == 2 {
                            self.insert_inner(&entry.key, &entry.value, entry.seq_num, true);
                        } else {
                            self.delete_inner(&entry.key, entry.seq_num, true);
                        }
                    }
                }, 
                6 => {
                    trans.remove(&entry.seq_num);
                },
                _ => panic!("invalid entry type"),
            };
        }
        self.writer = Some(log);
        max_seq_num
    }

    pub fn begin_tx(&mut self, seq_num: u64) {
        let log_entry = LogEntry {
            entry_type: 4, 
            key: Vec::new(),
            value: Vec::new(),
            seq_num,
        };
        self.writer.as_mut().unwrap().write(log_entry).unwrap();
    }

    pub fn commit_tx(&mut self, seq_num: u64) {
        let log_entry = LogEntry {
            entry_type: 5, 
            key: Vec::new(),
            value: Vec::new(),
            seq_num,
        };
        self.writer.as_mut().unwrap().write(log_entry).unwrap();
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8], seq_num: u64, is_tx: bool) {
        let log_entry = LogEntry {
            entry_type: match is_tx {
                true => 2,
                false => 0,
            }, 
            key: key.to_vec(),
            value: value.to_vec(),
            seq_num,
        };
        self.writer.as_mut()
            .unwrap()
            .write(log_entry)
            .unwrap();
        self.insert_inner(key, value, seq_num, is_tx);
    }

    pub fn insert_inner(&mut self, key: &[u8], value: &[u8], seq_num: u64, is_tx: bool) {
        let internal_key = if is_tx {
            InternalKey::new(key, seq_num,2)
        } else {
            InternalKey::new(key, seq_num,0)
        };
        self.inner.insert(internal_key, value.to_vec());
        self.size += 8 + key.len() + value.len();   //size of internal key + size of value
    }

    pub fn delete(&mut self, key: &[u8], seq_num: u64, is_tx: bool) {
        let log_entry = LogEntry {
            entry_type: match is_tx {
                true => 3,
                false => 1,
            }, 
            key: key.to_vec(),
            value: Vec::new(),
            seq_num,
        };
        self.writer.as_mut().unwrap().write(log_entry).unwrap();
        self.delete_inner(key, seq_num, is_tx);
    }

    pub fn delete_inner(&mut self, key: &[u8], seq_num: u64, is_tx: bool) {
        let internal_key = if is_tx {
            InternalKey::new(key, seq_num,3)
        } else {
            InternalKey::new(key, seq_num,1)
        };
        self.inner.insert(internal_key, Vec::new());
        self.size += 8 + key.len();
    }

    pub fn search(&self, key: &[u8], seq_num: u64) -> Option<Option<Vec<u8>>> {
        let internal_key = InternalKey::new(key, seq_num, 1);
        self.inner.iter()
            .find(|kv| kv.0 >= &internal_key && &kv.0.user_key[..] == key)
            .map(|kv| {
                match kv.0.get_type() == 0 || kv.0.get_type() == 2 {   
                    true => Some(kv.1.clone()),  //insert
                    false => None                //delete
                }
            })
    }

}