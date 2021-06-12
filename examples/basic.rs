use draft_kv::lsm::LsmDb;

use std::env;

fn main() {
    let cur_dir = env::current_dir().unwrap();
    println!("db_path = {:?}", cur_dir);
    let lsm = LsmDb::new(cur_dir);
    lsm.insert("A".as_bytes(), "3".as_bytes());
    lsm.insert("B".as_bytes(), "4".as_bytes());
    println!("GET A = {:?}", lsm.search("A".as_bytes(), None));
    println!("GET B = {:?}", lsm.search("B".as_bytes(), None));
    lsm.delete("A".as_bytes());
    lsm.delete("B".as_bytes());
    lsm.insert("A".as_bytes(), "5".as_bytes());
    println!("GET A = {:?}", lsm.search("A".as_bytes(), None));
    println!("GET B = {:?}", lsm.search("B".as_bytes(), None));
    lsm.insert("B".as_bytes(), "5".as_bytes());
    println!("GET B = {:?}", lsm.search("B".as_bytes(), None));
}