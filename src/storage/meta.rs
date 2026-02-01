use std::io;

use crate::storage::page::{PageId, PAGE_SIZE};

pub struct Meta {
    pub freelist_head: Option<PageId>,
    pub root: Option<PageId>,
}

// disk layout
mod disk {
    pub const MAGIC: &[u8; 4] = b"DB!!";
    pub const VERSION: u32 = 1;

    pub const OFFSET_FREELIST: usize = 20;
    pub const OFFSET_ROOT: usize = 28;
}

// encode/decode page metadata
pub fn init_page(buf: &mut [u8; PAGE_SIZE]) { 
    buf.fill(0); // old garbage could exist before here
    buf[0..4].copy_from_slice(disk::MAGIC);
    buf[4..8].copy_from_slice(&disk::VERSION.to_le_bytes());
    buf[disk::OFFSET_FREELIST..disk::OFFSET_FREELIST + 8].copy_from_slice(&0u64.to_le_bytes());
    buf[disk::OFFSET_ROOT..disk::OFFSET_ROOT + 8].copy_from_slice(&0u64.to_le_bytes());
}
pub fn read_metapage(buf: &[u8; PAGE_SIZE]) -> io::Result<Meta> {
    if &buf[0..4] != disk::MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid magic number in meta page",
        ));
    }
    let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    if version != disk::VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported meta page version",
        ));
    }

    let freelist_head = u64::from_le_bytes(buf[disk::OFFSET_FREELIST..disk::OFFSET_FREELIST + 8].try_into().unwrap());
    let root = u64::from_le_bytes(buf[disk::OFFSET_ROOT..disk::OFFSET_ROOT + 8].try_into().unwrap());

    Ok(Meta {
        freelist_head: if freelist_head == 0 { None } else { Some(freelist_head) },
        root: if root == 0 { None } else { Some(root) },
    })
}
pub fn write_metapage(meta: &Meta, buf: &mut [u8; PAGE_SIZE]) {
    if let Some(freelist_head) = meta.freelist_head {
        buf[disk::OFFSET_FREELIST..disk::OFFSET_FREELIST + 8].copy_from_slice(&freelist_head.to_le_bytes());
    } else {
        buf[disk::OFFSET_FREELIST..disk::OFFSET_FREELIST + 8].copy_from_slice(&0u64.to_le_bytes());
    }

    if let Some(root) = meta.root {
        buf[disk::OFFSET_ROOT..disk::OFFSET_ROOT + 8].copy_from_slice(&root.to_le_bytes());
    } else {
        buf[disk::OFFSET_ROOT..disk::OFFSET_ROOT + 8].copy_from_slice(&0u64.to_le_bytes());
    }
}
