use std::path;

use kvstore::storage::pagemanager::PageManager;
use kvstore::storage::page::PAGE_SIZE;

fn main() -> std::io::Result<()> {
    let test_path = path::Path::new("test.db");
    let mut pager = PageManager::open(test_path)?;
    let mut page = [0u8; PAGE_SIZE];
    page[0..4].copy_from_slice(b"DB!!");

    pager.write_page(0, &page)?;
    pager.sync()?;

    let mut read_back = [0u8; PAGE_SIZE];
    pager.read_page(0, &mut read_back)?;
    assert_eq!(&read_back[0..4], b"DB!!");

    Ok(())
}
