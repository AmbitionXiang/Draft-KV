#![feature(btree_drain_filter)]
#![feature(map_first_last)]

mod key;
pub mod lsm;
mod memtable;
mod sst;
mod utils;
mod wal;

#[cfg(test)]
mod tests {
    use crate::lsm::LsmDb;
    use std::env;

    #[test]
    fn open_lsmdb() {
        let cur_dir = env::current_dir().unwrap();
        let _lsm = LsmDb::new(cur_dir);
    }
}
