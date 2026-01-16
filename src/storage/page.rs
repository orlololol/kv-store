pub const PAGE_SIZE: usize = 4096;
pub type PageId = u64;

pub struct Page {
    data: [u8; PAGE_SIZE],
}

pub enum PageType {
    Meta,
    FreeList,
    BTreeLeaf,
    BTreeInternal,
}