use std::fs::{remove_file, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::utils::*;

#[derive(Debug)]
pub struct Log {
    path: PathBuf,
    file: File,
}

impl Log {
    pub fn open(dir_path: &PathBuf, log_num: u64) -> Self {
        let mut path = dir_path.clone();
        path.push(log_num.to_string());
        path.set_extension("LOG");
        let file = OpenOptions::new().create(true).append(true).read(true).open(&path).unwrap(); 
        Log {
            path,
            file,
        }
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn read(&mut self) -> Vec<LogEntry> {
        let mut buf = Vec::new();
        // read the whole file
        self.file.read_to_end(&mut buf).unwrap();
        let len = buf.len();
        let mut pos = 0;
        let mut entries = Vec::new();
        while pos < len {
            entries.push(LogEntry::decode(&buf, &mut pos));
        }
        entries
    }

    pub fn write(&mut self, log_entry: LogEntry) -> io::Result<()> {
        let bytes = log_entry.encode();
        self.file.write_all(&bytes)?;
        self.file.flush()
    }

}

#[derive(Clone, Debug)]
pub struct LogEntry {
    //0 insert, 1 delete, 2/3 tx-insert/tx-delete, 4 begin, 5 commit, 6 abort; entries in one transaction have the same number
    pub entry_type: u8, 
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub seq_num: u64,
}

impl LogEntry {
    pub fn new(entry_type: u8, key: &[u8], value: &[u8], seq_num: u64) -> Self {
        let key = key.to_vec();
        let value = value.to_vec();
        LogEntry {
            entry_type,
            key,
            value,
            seq_num,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![self.entry_type];
        if self.entry_type < 4 {
            bytes.extend_from_slice(&self.key.len().to_le_bytes());
            bytes.extend_from_slice(&self.key);
            bytes.extend_from_slice(&self.value.len().to_le_bytes());
            bytes.extend_from_slice(&self.value);
            bytes.extend_from_slice(&self.seq_num.to_le_bytes());
        } else {
            bytes.extend_from_slice(&self.seq_num.to_le_bytes());
        }
        bytes
    }

    pub fn decode(bytes: &[u8], pos: &mut usize) -> Self {
        //read entry_type
        let entry_type = bytes[*pos];
        *pos += 1;
        assert!(entry_type <= 6);
        if entry_type < 4 {
            //read key_len
            let key_len = to_usize(&bytes[*pos..*pos+8]);
            *pos += 8;
            //read key
            let key = bytes[*pos..*pos+key_len].to_vec();
            *pos += key_len;
            //read value_len
            let value_len = to_usize(&bytes[*pos..*pos+8]);
            *pos += 8;
            //read value
            let value = bytes[*pos..*pos+value_len].to_vec();
            *pos += value_len;
            //read sequence num, not suitable for 32-bit machine
            let seq_num = to_u64(&bytes[*pos..*pos+8]);
            *pos += 8;
            LogEntry {
                entry_type,
                key,
                value,
                seq_num,
            }
        } else {
            let seq_num = to_u64(&bytes[*pos..*pos+8]);
            *pos += 8;
            LogEntry {
                entry_type,
                key: Vec::new(),
                value: Vec::new(),
                seq_num,
            }
        }
    }
}

