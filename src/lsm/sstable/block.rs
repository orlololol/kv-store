use crate::constants::BLOCK_SIZE;
use std::io::{self, Write};

/// Block - Immutable 4KB data unit
///    - Binary layout: [Entries...] [Restart Points...] [Num Restarts]
///    - Each entry: [key_len(4B)][val_len(4B)][key][value]
///    - Restart points stored as u32 offsets
///    - from_bytes() - Deserialize from disk
///    - write_to() - Serialize to disk
///    - get(key) - Binary search with restart points
///    - iter() - Sequential iteration
#[derive(Debug, Clone)]
pub struct Block {
    data: Vec<u8>,
    restart_points: Vec<u32>,
}

///  BlockBuilder: Constructs blocks incrementally
///    - Adds key-value pairs until block reaches ~4KB
///    - Automatically creates restart points every 16 entries
///    - Returns false when block is full (won't fit more data)
///    - finish() method packages everything into a Block
pub struct BlockBuilder {
    data: Vec<u8>,
    restart_points: Vec<u32>,
    counter: usize,          // Entries since last restart
    restart_interval: usize, // Entries between restarts (default: 16)
}

/// Iterator over block entries
/// - Returns (key, value) pairs sequentially
/// - Automatically stops at the end of entries
pub struct BlockIterator {
    data: Vec<u8>,
    restart_points: Vec<u32>,
    current_offset: usize,
}

#[derive(Debug)]
pub enum BlockError {
    Io(io::Error),
    Corrupted(String),
    Full,
}

impl From<io::Error> for BlockError {
    fn from(err: io::Error) -> Self {
        BlockError::Io(err)
    }
}

impl std::fmt::Display for BlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockError::Io(e) => write!(f, "Block I/O error: {}", e),
            BlockError::Corrupted(msg) => write!(f, "Block corrupted: {}", msg),
            BlockError::Full => write!(f, "Block is full"),
        }
    }
}

impl std::error::Error for BlockError {}

pub type Result<T> = std::result::Result<T, BlockError>;

impl BlockBuilder {
    pub fn new() -> Self {
        let mut builder = Self {
            data: Vec::new(),
            restart_points: Vec::new(),
            counter: 0,
            restart_interval: 16,
        };
        // first entry is always a restart point
        builder.restart_points.push(0);
        builder
    }

    /// returns false if block is full and entry cannot be added
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let entry_size = 4 + 4 + key.len() + value.len(); // key_len(4) + val_len(4) + key + value

        let restart_size = (self.restart_points.len() + 1) * 4 + 4; // offsets + count

        if self.data.len() + entry_size + restart_size > BLOCK_SIZE {
            return Ok(false);
        }

        if self.counter >= self.restart_interval {
            self.restart_points.push(self.data.len() as u32);
            self.counter = 0;
        }

        self.data.extend_from_slice(&(key.len() as u32).to_le_bytes());
        self.data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(value);

        self.counter += 1;

        Ok(true)
    }

    pub fn finish(mut self) -> Block {
        // append restart points
        for &offset in &self.restart_points {
            self.data.extend_from_slice(&offset.to_le_bytes());
        }

        // append number of restart points
        self.data
            .extend_from_slice(&(self.restart_points.len() as u32).to_le_bytes());

        Block {
            data: self.data,
            restart_points: self.restart_points,
        }
    }

    pub fn current_size(&self) -> usize {
        self.data.len() + (self.restart_points.len() * 4) + 4
    }

    pub fn is_empty(&self) -> bool {
        self.counter == 0 && self.restart_points.len() == 1
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Block {
    pub fn from_bytes(data: Vec<u8>) -> Result<Self> {
        if data.len() < 4 {
            return Err(BlockError::Corrupted(
                "Block too small for restart count".to_string(),
            ));
        }

        let num_restarts_offset = data.len() - 4;
        let num_restarts = u32::from_le_bytes([
            data[num_restarts_offset],
            data[num_restarts_offset + 1],
            data[num_restarts_offset + 2],
            data[num_restarts_offset + 3],
        ]) as usize;

        if num_restarts == 0 {
            return Err(BlockError::Corrupted(
                "Block has no restart points".to_string(),
            ));
        }

        let restart_offset = num_restarts_offset - (num_restarts * 4);

        if restart_offset > data.len() {
            return Err(BlockError::Corrupted("Invalid restart offset".to_string()));
        }

        let mut restart_points = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let offset = restart_offset + i * 4;
            let restart_point = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            restart_points.push(restart_point);
        }

        Ok(Self {
            data,
            restart_points,
        })
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.data)?;
        Ok(())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn iter(&self) -> BlockIterator {
        BlockIterator {
            data: self.data.clone(),
            restart_points: self.restart_points.clone(),
            current_offset: 0,
        }
    }

    /// binary search for a key in the block
    pub fn get(&self, target_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let restart_idx = self.find_restart_point(target_key)?;

        let start_offset = self.restart_points[restart_idx] as usize;
        let end_offset = if restart_idx + 1 < self.restart_points.len() {
            self.restart_points[restart_idx + 1] as usize
        } else {
            // end of entries is before restart points section
            self.data.len() - (self.restart_points.len() * 4) - 4
        };

        let mut offset = start_offset;
        while offset < end_offset {
            let (key, value, next_offset) = self.parse_entry(offset)?;

            if key.as_slice() == target_key {
                return Ok(Some(value));
            }

            if key.as_slice() > target_key {
                // Keys are sorted, so we won't find it
                return Ok(None);
            }

            offset = next_offset;
        }

        Ok(None)
    }

    /// Returns the rightmost restart point whose key <= target_key
    fn find_restart_point(&self, target_key: &[u8]) -> Result<usize> {
        let mut result = 0;

        for (i, &offset) in self.restart_points.iter().enumerate() {
            let (key, _, _) = self.parse_entry(offset as usize)?;

            if key.as_slice() <= target_key {
                result = i;
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Parse an entry at the given offset
    /// Returns (key, value, next_offset)
    fn parse_entry(&self, offset: usize) -> Result<(Vec<u8>, Vec<u8>, usize)> {
        if offset + 8 > self.data.len() {
            return Err(BlockError::Corrupted("Entry offset out of bounds".to_string()));
        }

        let key_len = u32::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
            self.data[offset + 2],
            self.data[offset + 3],
        ]) as usize;

        let val_len = u32::from_le_bytes([
            self.data[offset + 4],
            self.data[offset + 5],
            self.data[offset + 6],
            self.data[offset + 7],
        ]) as usize;

        let key_start = offset + 8;
        let val_start = key_start + key_len;
        let next_offset = val_start + val_len;

        if next_offset > self.data.len() {
            return Err(BlockError::Corrupted("Entry extends beyond block".to_string()));
        }

        let key = self.data[key_start..val_start].to_vec();
        let value = self.data[val_start..next_offset].to_vec();

        Ok((key, value, next_offset))
    }
}

impl Iterator for BlockIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Calculate end of entries (before restart points)
        let entries_end = self.data.len() - (self.restart_points.len() * 4) - 4;

        if self.current_offset >= entries_end {
            return None;
        }

        if self.current_offset + 8 > self.data.len() {
            return Some(Err(BlockError::Corrupted(
                "Entry offset out of bounds".to_string(),
            )));
        }

        let key_len = u32::from_le_bytes([
            self.data[self.current_offset],
            self.data[self.current_offset + 1],
            self.data[self.current_offset + 2],
            self.data[self.current_offset + 3],
        ]) as usize;

        let val_len = u32::from_le_bytes([
            self.data[self.current_offset + 4],
            self.data[self.current_offset + 5],
            self.data[self.current_offset + 6],
            self.data[self.current_offset + 7],
        ]) as usize;

        let key_start = self.current_offset + 8;
        let val_start = key_start + key_len;
        let next_offset = val_start + val_len;

        if next_offset > entries_end {
            return Some(Err(BlockError::Corrupted(
                "Entry extends beyond block".to_string(),
            )));
        }

        let key = self.data[key_start..val_start].to_vec();
        let value = self.data[val_start..next_offset].to_vec();

        self.current_offset = next_offset;

        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder_add() {
        let mut builder = BlockBuilder::new();

        assert!(builder.add(b"key1", b"value1").unwrap());
        assert!(builder.add(b"key2", b"value2").unwrap());
        assert!(builder.add(b"key3", b"value3").unwrap());

        let block = builder.finish();
        assert!(block.size() > 0);
    }

    #[test]
    fn test_block_iter() {
        let mut builder = BlockBuilder::new();
        builder.add(b"apple", b"red").unwrap();
        builder.add(b"banana", b"yellow").unwrap();
        builder.add(b"cherry", b"red").unwrap();

        let block = builder.finish();
        let mut iter = block.iter();

        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(k, b"apple");
        assert_eq!(v, b"red");

        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(k, b"banana");
        assert_eq!(v, b"yellow");

        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(k, b"cherry");
        assert_eq!(v, b"red");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_block_get() {
        let mut builder = BlockBuilder::new();
        builder.add(b"apple", b"red").unwrap();
        builder.add(b"banana", b"yellow").unwrap();
        builder.add(b"cherry", b"red").unwrap();

        let block = builder.finish();

        assert_eq!(block.get(b"apple").unwrap(), Some(b"red".to_vec()));
        assert_eq!(block.get(b"banana").unwrap(), Some(b"yellow".to_vec()));
        assert_eq!(block.get(b"cherry").unwrap(), Some(b"red".to_vec()));
        assert_eq!(block.get(b"durian").unwrap(), None);
    }

    #[test]
    fn test_block_roundtrip() {
        let mut builder = BlockBuilder::new();
        builder.add(b"key1", b"value1").unwrap();
        builder.add(b"key2", b"value2").unwrap();

        let block1 = builder.finish();
        let bytes = block1.as_bytes().to_vec();

        let block2 = Block::from_bytes(bytes).unwrap();
        assert_eq!(block2.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(block2.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_block_restart_points() {
        let mut builder = BlockBuilder::new();

        // Add more than 16 entries to trigger multiple restart points
        for i in 0..20 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let block = builder.finish();

        // Should have at least 2 restart points (0 and 16)
        assert!(block.restart_points.len() >= 2);

        // Verify we can still retrieve all entries
        for i in 0..20 {
            let key = format!("key{:03}", i);
            let value = format!("value{:03}", i);
            assert_eq!(
                block.get(key.as_bytes()).unwrap(),
                Some(value.as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_block_size_limit() {
        let mut builder = BlockBuilder::new();

        // Try to fill block until it's full
        let mut count = 0;
        loop {
            let key = format!("key{:06}", count);
            let value = vec![b'x'; 100]; // 100 byte value

            if !builder.add(key.as_bytes(), &value).unwrap() {
                break; // Block is full
            }
            count += 1;
        }

        let block = builder.finish();

        // Block should be close to BLOCK_SIZE but not exceed it
        assert!(block.size() <= BLOCK_SIZE);
        assert!(block.size() > BLOCK_SIZE / 2); // Should have filled at least half

        println!("Added {} entries, block size: {}", count, block.size());
    }
}
