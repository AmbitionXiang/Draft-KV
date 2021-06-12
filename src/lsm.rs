use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, RwLock, Mutex};
use std::ffi::OsStr;
use std::fs::{create_dir_all, read_dir};
use std::thread;

use crate::memtable::MemTable;
use crate::sst::{Levels, Table};
use crate::wal::{Log, LogEntry};

use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::sync::ShardedLock;

pub struct Config {
    pub block_size: usize,
    pub l0_compaction_threshold: usize,
    pub l1_max_bytes: u64,
    pub max_levels: usize,
    write_buffer_size: usize,
}

impl Config {
    pub fn new() -> Self {
        Config {
            block_size: 4 * 1024, // 4KB
            l0_compaction_threshold: 4,
            l1_max_bytes: 64 * 1024 * 1024, // 64MB 
            max_levels: 7,
            write_buffer_size: 4 * 1024 * 1024, // 4MB,
        }
    }
}


pub struct LsmDb {
    config: Config,
    db_path: PathBuf,
    next_seq_num: AtomicU64,
    next_log_num: AtomicU64,
    mem_table: ShardedLock<MemTable>,
    im_mem_table: ShardedLock<Option<MemTable>>,
    levels: Arc<RwLock<Levels>>,
    do_compaction: Sender<Option<MemTable>>,
    running_compaction: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    shutdown_compaction_thread: Receiver<()>,
    update_lock: Arc<Mutex<()>>,
    tx_num: AtomicU64,
    tx_cache_table: Arc<RwLock<HashMap<u64, HashMap<(Vec<u8>, u64), Vec<u8>> >>>, //tx_id, cache_table
    tx_write_lock: AtomicU64,
}

impl LsmDb {
    pub fn new(dir_path: PathBuf) -> Self {
        //set configuration
        let config = Config::new();

        //open db
        create_dir_all(dir_path.clone()).unwrap();
        let all_file_list = read_dir(dir_path.clone()).unwrap()
            .map(|x| {
                x.unwrap().path()
            }).collect::<Vec<_>>();
        //read write-ahead-log
        let mut log_list = all_file_list.clone().into_iter().filter(|x| x.extension() == Some(OsStr::new("LOG")))
            .collect::<Vec<_>>();
        log_list.sort_by(|a, b| b.cmp(a)); //at most 2 logs, one for mutable mem table, and the other for immutable mem table
        log_list.truncate(2);

        let log_nums = log_list.into_iter().map(|x| x.file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
        ).collect::<Vec<_>>();
        println!("log_nums = {:?}", log_nums);
        let max_log_num = match log_nums.first() {
            Some(log_num) => *log_num, 
            None => 0,
        };
        let mut max_seq_num = 0;
        let mut trans = HashMap::<u64, Vec<LogEntry>>::new();
        let mut mem_table = MemTable::new();
        let mut im_mem_table = None;
        for (i, log_num) in log_nums.into_iter().enumerate() {
            let mut mem_table_temp = MemTable::new();
            max_seq_num = std::cmp::max(max_seq_num, mem_table_temp.recover(&dir_path, log_num, &mut trans));
            if i == 0 {
                mem_table = mem_table_temp;
            } else {
                im_mem_table = Some(mem_table_temp);
            }
        }
        mem_table.set_writer(&dir_path, max_log_num);

        //contruct sstable meta data
        let sst_list = all_file_list.clone().into_iter().filter(|x| x.extension() == Some(OsStr::new("sst")))
            .collect::<Vec<_>>();
        let levels = Arc::new(RwLock::new(Levels::new(dir_path.clone(), sst_list, &config)));

        let (do_compaction_sender, do_compaction_receiver) = crossbeam_channel::bounded(1);
        let (shutdown_compaction_sender, shutdown_compaction_receiver) = crossbeam_channel::bounded(1);

        let lsm_db = LsmDb {
            config,
            db_path: dir_path,
            next_seq_num: AtomicU64::new(max_seq_num+1),
            next_log_num: AtomicU64::new(max_log_num+1),
            mem_table: ShardedLock::new(mem_table),
            im_mem_table: ShardedLock::new(im_mem_table),
            levels,
            do_compaction: do_compaction_sender.clone(),
            running_compaction: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_compaction_thread: shutdown_compaction_receiver,
            update_lock: Arc::new(Mutex::new(())),
            tx_num: AtomicU64::new(1),
            tx_cache_table: Arc::new(RwLock::new(HashMap::new())),
            tx_write_lock: AtomicU64::new(0),  //0 is an invalid tx_id
        };

        lsm_db.process_compaction(shutdown_compaction_sender, (do_compaction_sender, do_compaction_receiver));

        lsm_db
    }

    pub fn may_compact_mem_table(&self) {
        if self.im_mem_table.read().unwrap().is_some() {
            if let Ok(_) = self.running_compaction.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst) {
                self.do_compaction.send(self.im_mem_table.write().unwrap().take()).unwrap();
            }
        }
        if self.mem_table.read().unwrap().size >= self.config.write_buffer_size 
        && self.im_mem_table.read().unwrap().is_none() {
            let mut mem_table = MemTable::new();
            mem_table.set_writer(&self.db_path, self.next_log_num.fetch_add(1, Ordering::SeqCst));
            let im_mem_table = std::mem::replace(&mut *self.mem_table.write().unwrap(), mem_table);  
            *self.im_mem_table.write().unwrap() = Some(im_mem_table);
        }
    }

    pub fn get_tx_write_lock(&self, tx_id: u64) {
        if tx_id != self.tx_write_lock.load(Ordering::Relaxed) {
            let mut res = Err(0);
            while res.is_err() {
                res = self.tx_write_lock.compare_exchange(0, tx_id,
                    Ordering::Acquire,
                    Ordering::Relaxed);
            }
        }
    }

    pub fn free_tx_write_lock(&self, tx_id: u64) {
        let _ = self.tx_write_lock.compare_exchange(tx_id, 0,
            Ordering::Acquire,
            Ordering::Relaxed);
    }

    pub fn tx_begin(&self) -> (u64, u64) {
        let tx_id = self.tx_num.fetch_add(1, Ordering::SeqCst);
        let seq_num = self.next_seq_num.fetch_add(1, Ordering::SeqCst);
        self.tx_cache_table.write().unwrap().insert(tx_id, HashMap::new());
        (tx_id, seq_num)
    }

    pub fn tx_insert(&self, tx_id: u64, seq_num: u64, key: &[u8], value: &[u8]) {
        self.get_tx_write_lock(tx_id);
        self.tx_cache_table.write()
            .unwrap()
            .get_mut(&tx_id)
            .unwrap()
            .insert((key.to_vec(), seq_num), value.to_vec());
    }

    pub fn tx_delete(&self, tx_id: u64, seq_num: u64, key: &[u8]) {
        self.get_tx_write_lock(tx_id);
        self.tx_cache_table.write()
            .unwrap()
            .get_mut(&tx_id)
            .unwrap()
            .insert((key.to_vec(), seq_num), Vec::new());
    }

    pub fn tx_update<F>(&self, tx_id: u64, seq_num: u64, key: &[u8], f: F)
    where
        F: Fn(Vec<u8>) -> Vec<u8>, 
    {
        self.get_tx_write_lock(tx_id);
        let old_value = self.tx_search(tx_id, seq_num, key);
        if let Some(v) = old_value {
            self.tx_insert(tx_id, seq_num, key, &f(v));
        }
    }

    pub fn tx_search(&self, tx_id: u64, seq_num: u64, key: &[u8]) -> Option<Vec<u8>> {
        match self.tx_cache_table.read()
            .unwrap()
            .get(&tx_id)
            .unwrap()
            .get(&(key.to_vec(), seq_num)) 
        {
            Some(v) => Some(v.clone()),
            None => {
                self.search(key, Some(seq_num))
            },
        }
    }

    pub fn tx_commit(&self, tx_id: u64) {
        let txs = self.tx_cache_table.write()
            .unwrap()
            .remove(&tx_id)
            .unwrap();
        let seq_num = txs.keys().collect::<Vec<_>>()[0].1; 
        self.mem_table.write().unwrap().begin_tx(seq_num);
        for ((key, seq_num), value) in txs {
            if value.is_empty() {
                self.mem_table.write().unwrap().delete(&key, seq_num, true);
            } else {
                self.mem_table.write().unwrap().insert(&key, &value, seq_num, true);
            }
        }
        self.mem_table.write().unwrap().commit_tx(seq_num);
        self.free_tx_write_lock(tx_id);
    }

    pub fn tx_abort(&self, tx_id: u64) {
        self.tx_cache_table.write().unwrap().remove(&tx_id);
        self.free_tx_write_lock(tx_id);
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) {
        let _lock = self.update_lock.lock().unwrap();
        self.mem_table.write().unwrap().insert(key, value, self.next_seq_num.fetch_add(1, Ordering::SeqCst), false);
        self.may_compact_mem_table();
    }

    pub fn delete(&self, key: &[u8]) {
        let _lock = self.update_lock.lock().unwrap();
        self.mem_table.write().unwrap().delete(key,  self.next_seq_num.fetch_add(1, Ordering::SeqCst), false);
        self.may_compact_mem_table();
    }

    pub fn update<F>(&self, key: &[u8], f: F)
    where
        F: Fn(Vec<u8>) -> Vec<u8>, 
    {
        let _lock = self.update_lock.lock().unwrap();
        let old_value = self.search(key, None);
        if let Some(v) = old_value {
            self.mem_table.write().unwrap().insert(key, &f(v), self.next_seq_num.fetch_add(1, Ordering::SeqCst), false);
            self.may_compact_mem_table();
        }
    }

    pub fn search(&self, key: &[u8], version: Option<u64>) -> Option<Vec<u8>> {
        let seq_num = match version {
            Some(seq_num) => seq_num,
            None => self.next_seq_num.load(Ordering::SeqCst) - 1,
        };
        //search in mutable table
        let mem_res = self.mem_table.read().unwrap().search(key, seq_num);
        if mem_res.is_some() {
            return mem_res.unwrap();
        }
        //search in immutable mem table
        let im_mem_res = self.im_mem_table.read().unwrap().as_ref().map(|t| t.search(key, seq_num)).flatten();
        if im_mem_res.is_some() {
            return im_mem_res.unwrap();
        }
        //search in sst, both None and deleted item will return None 
        self.levels.read().unwrap().search(key, seq_num)
    }

    fn process_compaction(&self, shutdown_compaction_sender: Sender<()>, do_compaction: (Sender<Option<MemTable>>, Receiver<Option<MemTable>>)) {
        let levels = self.levels.clone();
        let running_compaction = self.running_compaction.clone();
        let shutdown = self.shutdown.clone();
        thread::Builder::new()
            .name("compaction".to_owned())
            .spawn(move || {
                let (do_compaction_sender, do_compaction_receiver) = do_compaction;
                let mut done_compaction = false;
                let mut input_start = Vec::new();
                //For im_mem_table, Some: minor compaction; None: major compaction
                while let Ok(im_mem_table) = do_compaction_receiver.recv() {
                    if shutdown.load(Ordering::Acquire) {
                        break;
                    } else {
                        input_start = levels.read()
                            .unwrap()
                            .get_input_start(input_start);
                        //read lock to prevent blocking other services
                        let (deleted_tables, new_tables) = levels.read().unwrap().background_compaction(im_mem_table, &input_start);
                        done_compaction = !(deleted_tables.is_empty() && new_tables.is_empty());
                        levels.write().unwrap().update(deleted_tables, new_tables); 
                    }
                    running_compaction.store(false, Ordering::Release);

                    if done_compaction && !running_compaction.load(Ordering::Acquire) && !shutdown.load(Ordering::Acquire) {
                        // Previous compaction may have produced too many files in a level,
                        // so reschedule another compaction if needed
                        let _ = do_compaction_sender.try_send(None);
                        done_compaction = false;
                    }
                }
                shutdown_compaction_sender.send(()).unwrap();
            })
            .unwrap();
    }

}