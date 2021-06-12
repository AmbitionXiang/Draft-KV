use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::{remove_file, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{self, AtomicU64};
use std::path::{Path, PathBuf};

use crate::key::{InternalKey, LookUpKey};
use crate::lsm::Config;
use crate::memtable::MemTable;
use crate::utils::*;

use itertools::Itertools;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Footer {
    level: usize,
    min_key_addr: u64,  //For look up key
    max_key_addr: u64,  //For look up key
    last_seq_num: u64,  //used for sort of level 0
    meta_index_block_addr: u64,
    index_block_addr: u64,
    foot_addr: u64,   // it is not encoded
}

impl Footer {
    pub fn decode_from(sst_file: &File) -> Self {
        let file_len = sst_file.metadata().unwrap().len();
        let mut footer = vec![0; 48];
        sst_file.read_exact_at(
            footer.as_mut_slice(),
            file_len - 48,
        ).unwrap();

        let level = to_usize(&footer[0..8]);
        let min_key_addr = to_u64(&footer[8..16]);
        let max_key_addr = to_u64(&footer[16..24]);
        let last_seq_num = to_u64(&footer[24..32]);
        let meta_index_block_addr = to_u64(&footer[32..40]);
        let index_block_addr = to_u64(&footer[40..48]);
        Footer {
            level,
            min_key_addr,
            max_key_addr,
            last_seq_num,
            meta_index_block_addr,
            index_block_addr,
            foot_addr: file_len - 48,
        }
    }

    pub fn encode_to(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(48);
        buf.extend_from_slice(&self.level.to_le_bytes());
        buf.extend_from_slice(&self.min_key_addr.to_le_bytes());
        buf.extend_from_slice(&self.max_key_addr.to_le_bytes());
        buf.extend_from_slice(&self.last_seq_num.to_le_bytes());
        buf.extend_from_slice(&self.meta_index_block_addr.to_le_bytes());
        buf.extend_from_slice(&self.index_block_addr.to_le_bytes());
        buf
    }

}

#[derive(Clone, Debug, Default)]
pub struct DataBlockEntry {
    look_up_key: LookUpKey,
    value: Vec<u8>,
}

impl DataBlockEntry {
    pub fn new(look_up_key: LookUpKey, value: Vec<u8>) -> Self {
        DataBlockEntry {
            look_up_key,
            value,
        }
    }

    pub fn decode_from(bytes: &[u8], offset: &mut u64) -> Self {
        let look_up_key = LookUpKey::decode_from_bytes(bytes, offset);
        let mut cur = *offset as usize;
        let mut next = (*offset + 8) as usize;
        let value_len = to_u64(&bytes[cur..next]);
        *offset += 8;
        cur = *offset as usize;
        next = (*offset + value_len) as usize;
        DataBlockEntry {
            look_up_key,
            value: bytes[cur..next].to_vec(),
        }
    }

    pub fn encode_to(&self) -> Vec<u8> {
        let mut buf = self.look_up_key.encode_to();
        for b in &u64::to_le_bytes(self.value.len() as u64) {
            buf.push(*b);
        }
        buf.extend_from_slice(&self.value);
        buf
    }
    
}

#[derive(Clone, Debug, Default)]
pub struct IndexBlockEntry {
    max_key: LookUpKey,
    offset: u64,
    length: u64
}

impl IndexBlockEntry {
    pub fn new(max_key: LookUpKey, offset: u64, length: u64) -> Self {
        IndexBlockEntry {
            max_key,
            offset,
            length,
        }
    }

    pub fn decode_from(sst_file: &File, addr: &mut u64) -> Self {
        let max_key = LookUpKey::decode_from_file(sst_file, addr);
        //read offset
        let mut offset = vec![0; 8];
        sst_file.read_exact_at(
            offset.as_mut_slice(),
            *addr,
        ).unwrap();
        *addr += 8;
        let offset = to_u64(&offset);
        //read length
        let mut length = vec![0; 8];
        sst_file.read_exact_at(
            length.as_mut_slice(),
            *addr,
        ).unwrap();
        *addr += 8;
        let length = to_u64(&length);
        IndexBlockEntry {
            max_key,
            offset,
            length,
        }
    }

    pub fn encode_to(&self) -> Vec<u8> {
        let mut buf = self.max_key.encode_to();
        for b in &u64::to_le_bytes(self.offset) {
            buf.push(*b);
        }
        for b in &u64::to_le_bytes(self.length) {
            buf.push(*b);
        }
        buf
    }
}

pub struct Levels {
    db_path: PathBuf,
    inner: Vec<BTreeSet<Table>>,
    next_file_num: AtomicU64,
    block_size: usize,
    l0_compaction_threshold: usize,
    l1_max_bytes: u64,
}

impl Levels {
    pub fn new(db_path: PathBuf, sst_list: Vec<PathBuf>, config: &Config) -> Self {
        let mut levels = Vec::with_capacity(config.max_levels);
        for _ in 0..config.max_levels {
            levels.push(BTreeSet::new());
        }
        let mut max_file_num = 0;
        
        for sst_file in sst_list {
            let num = sst_file.file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            max_file_num = std::cmp::max(num, max_file_num);
            let table = Table::open(sst_file);
            levels[table.get_level()].insert(table);
        }

        Self {
            db_path,
            inner: levels,
            next_file_num: AtomicU64::new(max_file_num + 1),
            block_size: config.block_size,
            l0_compaction_threshold: config.l0_compaction_threshold,
            l1_max_bytes: config.l1_max_bytes,
        }
    }

    pub fn background_compaction(&self, im_mem_table: Option<MemTable>, input_start: &Vec<Option<(LookUpKey, LookUpKey)>>) -> (Vec<(usize, PathBuf)>, Vec<Table>) {
        match im_mem_table {
            Some(im_mem_table) => {
                (Vec::new(), vec![self.write_level0_files(im_mem_table)])
            },
            None => {
                let max_levels = self.inner.len();
                let mut deleted_tables = Vec::new();
                let mut new_tables = Vec::new();
                let mut src_table_idx = 0;
                for (level_idx, (level, input_start)) in self.inner.iter().zip(input_start.iter()).enumerate() {
                    let table_refs = level.iter().collect::<Vec<_>>();
                    let table_sizes = level.iter()
                        .map(|t| t.get_size())
                        .collect::<Vec<_>>();
                    let size_sum = table_sizes.iter().sum::<u64>();
                    if (level_idx == 0 && level.len() > self.l0_compaction_threshold) || 
                        (level_idx > 0 && size_sum > self.l1_max_bytes << (4*(level_idx-1)))
                    {
                        for (table_idx, &table) in table_refs.iter().enumerate() {
                            if input_start.as_ref().filter(|(min_key, max_key)| 
                                *min_key == table.min_key && *max_key == table.max_key
                            ).is_some() {
                                deleted_tables.push(table);
                                src_table_idx = table_idx;
                                break;
                            }
                        }
                    }
                    if !deleted_tables.is_empty() && level_idx < max_levels - 1 {
                        let dst_level_idx = level_idx + 1;
                        let mut dst_table_idx = usize::MAX;
                        let dst_table_refs = self.inner[dst_level_idx].iter().collect::<Vec<_>>();
                        let mut key_range = (&deleted_tables[0].min_key, &deleted_tables[0].max_key);
                        for (table_idx, &table) in dst_table_refs.iter().enumerate() {
                            //overlap
                            if table.min_key <= *key_range.1 || *key_range.0 <= table.max_key {
                                key_range.0 = std::cmp::min(&table.min_key, key_range.0);
                                key_range.1 = std::cmp::max(&table.max_key, key_range.1);
                                deleted_tables.push(table);
                                dst_table_idx = table_idx;
                            }
                        }
                        //sink directly without compaction
                        if dst_table_idx == usize::MAX {
                            assert!(deleted_tables.len() == 1);
                            let iter = Box::new(deleted_tables[0].content().into_iter());
                            let table = self.write_file(iter, dst_level_idx);
                            new_tables.push(table);
                        } else {
                            //src and dst take turn
                            let mut last_len = 0;
                            while deleted_tables.len() != last_len {
                                last_len = deleted_tables.len();
                                
                                while src_table_idx + 1 < table_refs.len() {
                                    src_table_idx += 1;
                                    let min_key = &table_refs[src_table_idx].min_key;
                                    let max_key = &table_refs[src_table_idx].max_key;
                                    if min_key <= key_range.1 || key_range.0 <= max_key {
                                        key_range.0 = std::cmp::min(min_key, key_range.0);
                                        key_range.1 = std::cmp::max(max_key, key_range.1);
                                        deleted_tables.push(table_refs[src_table_idx]);
                                    } else {
                                        src_table_idx -= 1;
                                        break;
                                    }
                                }
                                while dst_table_idx + 1 < dst_table_refs.len() {
                                    dst_table_idx += 1;
                                    let min_key = &dst_table_refs[dst_table_idx].min_key;
                                    let max_key = &dst_table_refs[dst_table_idx].max_key;
                                    if min_key <= key_range.1 || key_range.0 <= max_key {
                                        key_range.0 = std::cmp::min(min_key, key_range.0);
                                        key_range.1 = std::cmp::max(max_key, key_range.1);
                                        deleted_tables.push(dst_table_refs[dst_table_idx]);
                                    } else {
                                        dst_table_idx -= 1;
                                        break;
                                    }
                                }
    
                            }
                        }
                        //begin to compact
                        let mut merged = deleted_tables.iter()
                            .map(|x| x.content().into_iter())
                            .kmerge()
                            .collect::<Vec<_>>();
                        //only keep the newest version for the same key
                        merged.dedup_by_key(|(k, _)| k.get_user_key().to_vec());
                        self.write_file(Box::new(merged.into_iter()), dst_level_idx);
                        break;
                    }
                }
                (   
                    deleted_tables.into_iter()
                        .map(|x| (x.get_level(), x.file_name.clone()))
                        .collect::<Vec<_>>(), 
                    new_tables
                )
            },
        }
    }

    pub fn get_input_start(&self, mut last_input_start: Vec<Option<(LookUpKey, LookUpKey)>>) -> Vec<Option<(LookUpKey, LookUpKey)>> {
        if last_input_start.is_empty() {
            last_input_start = vec![None; self.inner.len()];
        }
        self.inner.iter()
            .enumerate()
            .map(|(level_idx, level)| if level_idx == 0 {
                level.last().map(|t| (t.min_key.clone(), t.max_key.clone()))
            } else {
                let mut input_start = None;
                for table in level.iter() {
                    if last_input_start[level_idx].as_ref().filter(|x| table.min_key >= x.1).is_some() {
                        input_start = Some((table.min_key.clone(), table.max_key.clone()));
                        break;
                    }
                }
                if input_start.is_none() {
                    input_start = level.first().map(|t| (t.min_key.clone(), t.max_key.clone()));
                }
                input_start
            }).collect::<Vec<_>>()
    }

    pub fn search(&self, key: &[u8], seq_num: u64) -> Option<Vec<u8>> {
        let internal_key = InternalKey::new(key, seq_num, 1);
        let look_up_key = LookUpKey::new(internal_key.clone());
        for (level, tables) in self.inner.iter().enumerate() {
            if tables.is_empty() {
                continue; 
            }
            if level == 0 {
                for table in tables {
                    if table.min_key <= look_up_key && table.max_key >= look_up_key {
                        let res = table.search(key, seq_num);
                        if res.is_some() {
                            return res.unwrap();
                        }
                    }
                }
            } else {
                let table = tables.iter()
                    .find(|table| table.min_key <= look_up_key && table.max_key >= look_up_key);
                let res = table.map(|t| t.search(key, seq_num)).flatten();
                if res.is_some() {
                    return res.unwrap();
                }
            }
        }
        None
    }

    pub fn update(&mut self, deleted_tables: Vec<(usize, PathBuf)>, new_tables: Vec<Table>) {
        let mut deleted_table_map = HashMap::new();            
        for (level, file_name) in deleted_tables {
            let files = deleted_table_map.entry(level).or_insert(Vec::new());
            files.push(file_name);
        }
        for (level, files) in deleted_table_map {
            //remove table from levels
            let deleted_tables = self.inner[level]
                .drain_filter(|t| files.contains(&t.file_name))
                .collect::<Vec<_>>();
            drop(deleted_tables);
            //detele corresponding sst files
            for file_name in files {
                remove_file(file_name).unwrap();
            }
        }

        for table in new_tables {
            self.inner[table.get_level()].insert(table);
        }
    }

    pub fn write_level0_files(&self, mut im_mem_table: MemTable) -> Table {
        let iter = Box::new(im_mem_table.take()
            .into_iter()
            .map(|(k, v)| (LookUpKey::new(k), v)));
        let table = self.write_file(iter, 0);
        im_mem_table.remove_writer();
        table
    }

    pub fn write_file(&self, iter: Box<dyn Iterator<Item = (LookUpKey, Vec<u8>)>>, level: usize) -> Table {
        let mut sst_file = self.db_path.clone();
        let next_file_num = self.next_file_num.fetch_add(1, atomic::Ordering::SeqCst);
        sst_file.push(next_file_num.to_string());
        sst_file.set_extension("sst");
        let table = Table::new(sst_file, iter, level, self.block_size);
        table
    }

}

#[derive(Debug)]
pub struct Table {
    file_name: PathBuf,
    file: File,
    footer: Footer,
    index_block: Vec<IndexBlockEntry>,
    min_key: LookUpKey,
    max_key: LookUpKey,
}

impl Table {
    pub fn new(sst_file: PathBuf, iter: Box<dyn Iterator<Item = (LookUpKey, Vec<u8>)>>, level: usize, block_size: usize) -> Self {
        let mut file = OpenOptions::new().create(true).append(true).read(true).open(&sst_file).unwrap();
        let mut buf = Vec::new();
        let mut index_block = Vec::new();
        let mut data_block = Vec::new();
        let data = iter.collect::<Vec<_>>();
        let min_key = data.first().unwrap().0.clone();
        let max_key = data.last().unwrap().0.clone();
        let mut last_seq_num = 0;

        for (key, value) in data {
            last_seq_num = std::cmp::max(key.get_seq_num(), last_seq_num);
            let data_block_entry = DataBlockEntry::new(key.clone(), value);
            data_block.append(&mut data_block_entry.encode_to());
            if data_block.len() > block_size {
                let offset = buf.len() as u64;
                let length = data_block.len() as u64; 
                let index_block_entry = IndexBlockEntry::new(key, offset, length);
                buf.append(&mut data_block);
                index_block.push(index_block_entry);
            }
        }
        let index_block_addr = buf.len() as u64;
        //Currently, there is no meta index block, so the addr is equal to index_block_addr
        let meta_index_block_addr = index_block_addr;
        buf.append(&mut index_block.iter().map(|e| e.encode_to()).flatten().collect::<Vec<_>>());
        let min_key_addr = buf.len() as u64;
        buf.append(&mut min_key.encode_to());
        let max_key_addr = buf.len() as u64;
        buf.append(&mut max_key.encode_to());
        let foot_addr = buf.len() as u64;

        let footer = Footer {
            level,
            min_key_addr,
            max_key_addr,
            last_seq_num,
            meta_index_block_addr,
            index_block_addr,
            foot_addr,
        };
        buf.append(&mut footer.encode_to());
        //Write to file
        file.write_all(&buf).unwrap();
        file.flush().unwrap();

        Table {
            file_name: sst_file,
            file,
            footer,
            index_block,
            min_key,
            max_key,
        }
    }

    pub fn open(sst_file: PathBuf) -> Self {
        let file = OpenOptions::new().read(true).open(&sst_file).unwrap();
        let footer = Footer::decode_from(&file);
        let mut index_block = Vec::new();
        let mut addr = footer.index_block_addr;
        while addr < footer.foot_addr {
            index_block.push(IndexBlockEntry::decode_from(&file, &mut addr));
        }
        let mut key_addr = footer.min_key_addr;
        let min_key = LookUpKey::decode_from_file(&file, &mut key_addr);
        assert!(key_addr == footer.max_key_addr);
        let max_key = LookUpKey::decode_from_file(&file, &mut key_addr);
        Table {
            file_name: sst_file,
            file,
            footer,
            index_block,
            min_key,
            max_key,
        }
    }

    pub fn get_level(&self) -> usize {
        self.footer.level
    }

    pub fn get_size(&self) -> u64 {
        self.file.metadata().unwrap().len()
    }

    pub fn search(&self, key: &[u8], seq_num: u64) -> Option<Option<Vec<u8>>> {
        let internal_key = InternalKey::new(key, seq_num, 1);
        let look_up_key = LookUpKey::new(internal_key.clone());
        let idx = match self.index_block.binary_search_by_key(&&look_up_key, |e| &e.max_key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        if idx < self.index_block.len() {
            let index_entry = self.index_block[idx].clone();
            let mut block = vec![0 as u8; index_entry.length as usize];
            self.file.read_exact_at(
                block.as_mut_slice(),
                index_entry.offset,
            ).unwrap();
            
            let mut offset = 0;
            while offset < index_entry.length {
                let block_entry = DataBlockEntry::decode_from(&block, &mut offset);
                if block_entry.look_up_key >= look_up_key && block_entry.look_up_key.get_user_key() == key {
                    match block_entry.look_up_key.get_type() {
                        0 => return Some(Some(block_entry.value.to_vec())), 
                        1 => return Some(None),
                        _ => panic!("invalid look_up_key"),
                    };
                }
            }
            return None;
        } else {
            return None;
        }
    }

    pub fn content(&self) -> Vec<(LookUpKey, Vec<u8>)> {
        let mut res = Vec::new();
        for index_entry in self.index_block.iter() {
            let mut block = vec![0 as u8; index_entry.length as usize];
            self.file.read_exact_at(
                block.as_mut_slice(),
                index_entry.offset,
            ).unwrap();
            let mut offset = 0;
            while offset < index_entry.length {
                let block_entry = DataBlockEntry::decode_from(&block, &mut offset);
                let DataBlockEntry {
                    look_up_key,
                    value,
                } = block_entry;
                res.push((look_up_key, value));
            }
        }
        res
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        if self.footer.level == 0 {
            self.footer.last_seq_num == self.footer.last_seq_num
        } else {
            self.min_key == other.min_key
        }
    }
}

impl Eq for Table {}

impl PartialOrd for Table {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.footer.level == 0 {
            other.footer.last_seq_num.partial_cmp(&self.footer.last_seq_num)
        } else {
            self.min_key.partial_cmp(&other.min_key)
        }
    }
}

impl Ord for Table {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.footer.level == 0 {
            other.footer.last_seq_num.cmp(&self.footer.last_seq_num)
        } else {
            self.min_key.cmp(&other.min_key)
        }
    }
}


