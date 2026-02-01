use std::fs::{ File, OpenOptions };
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::storage::page::{ PAGE_SIZE, PageId };
use crate::storage::meta;

#[derive(Debug)]
pub struct PageManager {
    file: File,
    num_pages: PageId,
}

impl PageManager {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        
        let file_len = file.metadata()?.len();

        // initialize new database file if non existent
        if file_len == 0 {
            let mut page0 = [0u8; PAGE_SIZE];
            meta::init_page(&mut page0);
            file.set_len(PAGE_SIZE as u64)?;
            file.write_all(&page0)?;
            file.sync_all()?;
            return Ok(PageManager { file, num_pages: 1 });
        }

        if file_len % PAGE_SIZE as u64 != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "database file is not page-aligned",
            ));
        }
    
        let num_pages = (file_len / PAGE_SIZE as u64) as PageId;

        let mut page0 = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut page0)?;

        // validate meta page
        meta::read_metapage(&page0)?;

        Ok(PageManager { file, num_pages })
    }

    pub fn allocate_page(&mut self) -> std::io::Result<PageId> {
        let page_id = self.num_pages;
        self.num_pages += 1;
        Ok(page_id)
    }

    pub fn read_page(&mut self, page_id: PageId, buffer: &mut [u8; PAGE_SIZE]) -> std::io::Result<()> {
        if page_id >= self.num_pages {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "page_id out of bounds",
            ));
        }
        
        self.file.seek(SeekFrom::Start(page_id * PAGE_SIZE as u64))?;
        self.file.read_exact(buffer)?;
        Ok(())
    }

    pub fn write_page(&mut self, page_id: PageId, buffer: &[u8; PAGE_SIZE]) -> std::io::Result<()> {
        let offset = page_id * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buffer)?;

        if page_id >= self.num_pages {
            self.num_pages = page_id + 1;
        }

        Ok(())
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub fn num_pages(&self) -> PageId {
        self.num_pages
    }
}