use std::collections::BTreeMap;

/// in-memory sorted key-value store backed by BTreeMap
#[derive(Debug)]
pub struct Memtable {
    data: BTreeMap<Vec<u8>, MemtableEntry>,

    size: usize,

    max_size: usize,

    seq_num: u64,
}

/// entry in the memtable
#[derive(Debug, Clone)]
pub struct MemtableEntry {
    /// value, none indicates deletion/tombstone
    pub value: Option<Vec<u8>>,

    pub seq_num: u64,
}

impl Memtable {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: BTreeMap::new(),
            size: 0,
            max_size,
            seq_num: 0,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.seq_num += 1;

        let entry = MemtableEntry {
            value: Some(value.to_vec()),
            seq_num: self.seq_num,
        };

        let key_vec = key.to_vec();
        let size_delta = if let Some(old_entry) = self.data.get(&key_vec) {
            let old_value_size = old_entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
            let new_value_size = value.len();

            if new_value_size > old_value_size {
                new_value_size - old_value_size
            } else {
                0 // don't decrease size on overwrites
            }
        } else {
            key.len() + value.len() + 24 // 24 bytes overhead (seq_num, Option, Vec headers)
        };

        self.data.insert(key_vec, entry);
        self.size += size_delta;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemtableEntry> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), String> {
        self.seq_num += 1;

        let entry = MemtableEntry {
            value: None,
            seq_num: self.seq_num,
        };

        let key_vec = key.to_vec();
        let size_delta = if self.data.contains_key(&key_vec) {
            0 // already exists, just updating
        } else {
            key.len() + 24 // new tombstone
        };

        self.data.insert(key_vec, entry);
        self.size += size_delta;

        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &MemtableEntry)> {
        self.data.iter()
    }

    pub fn range<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a MemtableEntry)> + 'a {
        self.data.range(start.to_vec()..end.to_vec())
    }

    pub fn seq_num(&self) -> u64 {
        self.seq_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut memtable = Memtable::new(1024);

        memtable.put(b"key1", b"value1").unwrap();
        memtable.put(b"key2", b"value2").unwrap();

        let entry1 = memtable.get(b"key1").unwrap();
        assert_eq!(entry1.value.as_ref().unwrap(), b"value1");

        let entry2 = memtable.get(b"key2").unwrap();
        assert_eq!(entry2.value.as_ref().unwrap(), b"value2");

        assert!(memtable.get(b"key3").is_none());
    }

    #[test]
    fn test_delete() {
        let mut memtable = Memtable::new(1024);

        memtable.put(b"key1", b"value1").unwrap();
        assert!(memtable.get(b"key1").unwrap().value.is_some());

        memtable.delete(b"key1").unwrap();
        let entry = memtable.get(b"key1").unwrap();
        assert!(entry.value.is_none()); // tombstone
    }

    #[test]
    fn test_overwrite() {
        let mut memtable = Memtable::new(1024);

        memtable.put(b"key1", b"value1").unwrap();
        memtable.put(b"key1", b"value2").unwrap();

        let entry = memtable.get(b"key1").unwrap();
        assert_eq!(entry.value.as_ref().unwrap(), b"value2");
    }

    #[test]
    fn test_size_tracking() {
        let mut memtable = Memtable::new(1024);

        assert_eq!(memtable.size(), 0);

        memtable.put(b"key1", b"value1").unwrap();
        let size1 = memtable.size();
        assert!(size1 > 0);

        memtable.put(b"key2", b"value2").unwrap();
        let size2 = memtable.size();
        assert!(size2 > size1);
    }

    #[test]
    fn test_is_full() {
        let mut memtable = Memtable::new(100);

        assert!(!memtable.is_full());

        // Fill until full
        memtable.put(b"key1", b"value1_long_enough").unwrap();
        memtable.put(b"key2", b"value2_long_enough").unwrap();
        memtable.put(b"key3", b"value3_long_enough").unwrap();

        assert!(memtable.is_full());
    }

    #[test]
    fn test_iterator() {
        let mut memtable = Memtable::new(1024);

        memtable.put(b"c", b"3").unwrap();
        memtable.put(b"a", b"1").unwrap();
        memtable.put(b"b", b"2").unwrap();

        let mut iter = memtable.iter();

        let (k, v) = iter.next().unwrap();
        assert_eq!(k.as_slice(), b"a");
        assert_eq!(v.value.as_ref().unwrap().as_slice(), b"1");

        let (k, v) = iter.next().unwrap();
        assert_eq!(k.as_slice(), b"b");
        assert_eq!(v.value.as_ref().unwrap().as_slice(), b"2");

        let (k, v) = iter.next().unwrap();
        assert_eq!(k.as_slice(), b"c");
        assert_eq!(v.value.as_ref().unwrap().as_slice(), b"3");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_range_iterator() {
        let mut memtable = Memtable::new(1024);

        memtable.put(b"a", b"1").unwrap();
        memtable.put(b"c", b"3").unwrap();
        memtable.put(b"e", b"5").unwrap();
        memtable.put(b"g", b"7").unwrap();

        let results: Vec<_> = memtable.range(b"b", b"f").collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.as_slice(), b"c");
        assert_eq!(results[1].0.as_slice(), b"e");
    }

    #[test]
    fn test_seq_num_ordering() {
        let mut memtable = Memtable::new(1024);

        memtable.put(b"key1", b"value1").unwrap();
        let seq1 = memtable.get(b"key1").unwrap().seq_num;

        memtable.put(b"key2", b"value2").unwrap();
        let seq2 = memtable.get(b"key2").unwrap().seq_num;

        assert!(seq2 > seq1);
    }
}
