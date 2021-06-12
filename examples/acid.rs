use draft_kv::lsm::LsmDb;

use std::env;
use std::time::{Instant, Duration};
use std::thread;
use std::sync::Arc;

fn u64_to_bytes(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

fn bytes_to_u64(bytes: Vec<u8>) -> u64 {
    let mut buf = [0; 8];
    for i in 0..8 {
        buf[i] = bytes[i];
    }
    u64::from_le_bytes(buf)
}

fn add_one(v: Vec<u8>) -> Vec<u8> {
    let i = bytes_to_u64(v);
    u64_to_bytes(i+1)
}

fn sub_one(v: Vec<u8>) -> Vec<u8> {
    let i = bytes_to_u64(v);
    u64_to_bytes(i-1)
}

fn main() {
    let cur_dir = env::current_dir().unwrap();
    println!("db_path = {:?}", cur_dir);
    let lsm = Arc::new(LsmDb::new(cur_dir));
 
    lsm.insert("A".as_bytes(), &u64_to_bytes(1));
    lsm.insert("B".as_bytes(), &u64_to_bytes(1));

    let threads = 3;
    let mut handles = Vec::new();
    for i in 0..threads {
        let lsm = lsm.clone();

        let h = thread::spawn(move || {
            let now = Instant::now();
            let mut iter_num = 0;
            while now.elapsed() <= Duration::from_secs(60) {
                println!("thread {:?}, iter {:?}", i, iter_num);
                iter_num += 1;
                let (tx_id, seq_num) = lsm.tx_begin();
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_commit(tx_id);

                let (tx_id, seq_num) = lsm.tx_begin();
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), add_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), add_one);
                lsm.tx_commit(tx_id);

                let (tx_id, seq_num) = lsm.tx_begin();
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "A".as_bytes(), sub_one);
                lsm.tx_update(tx_id, seq_num, "B".as_bytes(), sub_one);
                lsm.tx_abort(tx_id);
            }
        });
    
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }

    println!("GET A = {:?}", bytes_to_u64(lsm.search("A".as_bytes(), None).unwrap()));
    println!("GET B = {:?}", bytes_to_u64(lsm.search("B".as_bytes(), None).unwrap()));
}