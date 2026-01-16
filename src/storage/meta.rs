use crate::storage::page::PageId;

struct Meta {
    pub freelist_head: Option<PageId>,
    btree_root: Option<PageId>,
}