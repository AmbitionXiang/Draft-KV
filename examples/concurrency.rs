use draft_kv::lsm::LsmDb;

use std::env;
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

fn main() {
    let cur_dir = env::current_dir().unwrap();
    println!("db_path = {:?}", cur_dir);
    let lsm = Arc::new(LsmDb::new(cur_dir));
 
    let lsm_c = lsm.clone();
    let h0 = thread::spawn(move || {
        for _ in 0..10 {
            lsm_c.insert("A".as_bytes(), &u64_to_bytes(1));
            lsm_c.insert("B".as_bytes(), &u64_to_bytes(1));
            lsm_c.update("A".as_bytes(), add_one);
            lsm_c.update("B".as_bytes(), add_one);
            println!("GET A = {:?}", lsm_c.search("A".as_bytes(), None));
            lsm_c.delete("A".as_bytes());
            println!("GET B = {:?}", lsm_c.search("B".as_bytes(), None));
            lsm_c.delete("B".as_bytes());
        }
    });

    let lsm_c = lsm.clone();
    let h1 = thread::spawn(move || {
        for _ in 0..10 {
            lsm_c.insert("C".as_bytes(), &u64_to_bytes(1));
            lsm_c.insert("D".as_bytes(), &u64_to_bytes(1));
            lsm_c.update("C".as_bytes(), add_one);
            lsm_c.update("D".as_bytes(), add_one);
            println!("GET C = {:?}", lsm_c.search("C".as_bytes(), None));
            lsm_c.delete("C".as_bytes());
            println!("GET D = {:?}", lsm_c.search("D".as_bytes(), None));
            lsm_c.delete("D".as_bytes());
        }
    });

    let lsm_c = lsm.clone();
    let h2 = thread::spawn(move || {
        for _ in 0..10 {
            lsm_c.insert("E".as_bytes(), &u64_to_bytes(1));
            lsm_c.insert("F".as_bytes(), &u64_to_bytes(1));
            lsm_c.update("E".as_bytes(), add_one);
            lsm_c.update("F".as_bytes(), add_one);
            println!("GET E = {:?}", lsm_c.search("E".as_bytes(), None));
            lsm_c.delete("E".as_bytes());
            println!("GET F = {:?}", lsm_c.search("F".as_bytes(), None));
            lsm_c.delete("F".as_bytes());
        }
    });

    h0.join().unwrap();
    h1.join().unwrap();
    h2.join().unwrap();

}