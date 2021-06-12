use draft_kv::lsm::LsmDb;

use std::env;

fn main() {
    let cur_dir = env::current_dir().unwrap();
    println!("db_path = {:?}", cur_dir);
    let lsm = LsmDb::new(cur_dir);
    println!("GET A = {:?}", lsm.search("A".as_bytes(), None));
    println!("GET B = {:?}", lsm.search("B".as_bytes(), None));
}