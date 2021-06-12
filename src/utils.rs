

pub fn to_usize(bytes: &[u8]) -> usize {
    let mut buf = [0 as u8; 8];
    for (p, i) in bytes.iter().enumerate() {
        buf[p] = *i;
    }
    usize::from_le_bytes(buf)
}

pub fn to_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0 as u8; 8];
    for (p, i) in bytes.iter().enumerate() {
        buf[p] = *i;
    }
    u64::from_le_bytes(buf)
}