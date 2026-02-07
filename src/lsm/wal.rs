use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct WalWriter {
    file: File,
    path: PathBuf,
    offset: u64,
}

pub struct WalReader {
    reader: BufReader<File>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalEntry {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

const OP_PUT: u8 = 0x01;
const OP_DELETE: u8 = 0x02;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Corrupted(String),
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        WalError::Io(err)
    }
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {}", e),
            WalError::Corrupted(msg) => write!(f, "WAL corrupted: {}", msg),
        }
    }
}

impl std::error::Error for WalError {}

pub type Result<T> = std::result::Result<T, WalError>;

impl WalWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            file,
            path,
            offset: 0,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new().write(true).append(true).open(&path)?;

        let offset = file.seek(SeekFrom::End(0))?;

        Ok(Self { file, path, offset })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let bytes = encode_entry(entry)?;
        self.file.write_all(&bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn truncate(&mut self) -> Result<()> {
        drop(std::mem::replace(
            &mut self.file,
            File::open(&self.path)?, // temporary placeholder
        ));

        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;

        self.offset = 0;
        Ok(())
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl WalReader {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { reader })
    }

    pub fn next(&mut self) -> Result<Option<WalEntry>> {
        decode_entry(&mut self.reader)
    }
}

/// encode a WAL entry to bytes
///
/// format:
/// ┌─────────┬────────┬────────┬─────────┬───────────┬─────┬───────┐
/// │Checksum │ Length │ OpType │ Key Len │ Value Len │ Key │ Value │
/// │ (4B)    │ (4B)   │ (1B)   │ (4B)    │ (4B)      │ var │ var   │
/// └─────────┴────────┴────────┴─────────┴───────────┴─────┴───────┘
fn encode_entry(entry: &WalEntry) -> Result<Vec<u8>> {
    let (op_type, key, value) = match entry {
        WalEntry::Put { key, value } => (OP_PUT, key.as_slice(), Some(value.as_slice())),
        WalEntry::Delete { key } => (OP_DELETE, key.as_slice(), None),
    };

    let key_len = key.len() as u32;
    let value_len = value.map(|v| v.len() as u32).unwrap_or(0);

    // payload size without checksum
    let payload_size = 4 + 1 + 4 + 4 + key.len() + value_len as usize;

    let mut payload = Vec::with_capacity(payload_size);

    // write length excluding checksum field
    payload.extend_from_slice(&(payload_size as u32 - 4).to_le_bytes());

    payload.push(op_type);

    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(&value_len.to_le_bytes());
    payload.extend_from_slice(key);

    if let Some(v) = value {
        payload.extend_from_slice(v);
    }

    let checksum = crc32(&payload[4..]);

    // prepend checksum
    let mut result = Vec::with_capacity(4 + payload.len());
    result.extend_from_slice(&checksum.to_le_bytes());
    result.extend_from_slice(&payload);

    Ok(result)
}

fn decode_entry<R: Read>(reader: &mut R) -> Result<Option<WalEntry>> {
    let mut checksum_buf = [0u8; 4];
    match reader.read_exact(&mut checksum_buf) {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let expected_checksum = u32::from_le_bytes(checksum_buf);

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let length = u32::from_le_bytes(len_buf) as usize;

    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;

    let actual_checksum = crc32(&payload);
    if actual_checksum != expected_checksum {
        return Err(WalError::Corrupted(format!(
            "Checksum mismatch: expected {}, got {}",
            expected_checksum, actual_checksum
        )));
    }

    let mut cursor = 0;

    let op_type = payload[cursor];
    cursor += 1;

    let key_len = u32::from_le_bytes([
        payload[cursor],
        payload[cursor + 1],
        payload[cursor + 2],
        payload[cursor + 3],
    ]) as usize;
    cursor += 4;

    let value_len = u32::from_le_bytes([
        payload[cursor],
        payload[cursor + 1],
        payload[cursor + 2],
        payload[cursor + 3],
    ]) as usize;
    cursor += 4;

    let key = payload[cursor..cursor + key_len].to_vec();
    cursor += key_len;

    let entry = match op_type {
        OP_PUT => {
            let value = payload[cursor..cursor + value_len].to_vec();
            WalEntry::Put { key, value }
        }
        OP_DELETE => WalEntry::Delete { key },
        _ => {
            return Err(WalError::Corrupted(format!(
                "Unknown operation type: {}",
                op_type
            )))
        }
    };

    Ok(Some(entry))
}

/// simple CRC32 implementation
fn crc32(data: &[u8]) -> u32 {
    const POLYNOMIAL: u32 = 0xEDB88320;
    let mut crc: u32 = 0xFFFFFFFF;

    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLYNOMIAL;
            } else {
                crc >>= 1;
            }
        }
    }

    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_encode_decode_put() {
        let entry = WalEntry::Put {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
        };

        let encoded = encode_entry(&entry).unwrap();
        let mut reader = &encoded[..];
        let decoded = decode_entry(&mut reader).unwrap().unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_encode_decode_delete() {
        let entry = WalEntry::Delete {
            key: b"test_key".to_vec(),
        };

        let encoded = encode_entry(&entry).unwrap();
        let mut reader = &encoded[..];
        let decoded = decode_entry(&mut reader).unwrap().unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_wal_writer_reader() {
        let temp_dir = env::temp_dir();
        let wal_path = temp_dir.join("test_wal.log");

        {
            let mut writer = WalWriter::create(&wal_path).unwrap();

            writer
                .append(&WalEntry::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                })
                .unwrap();

            writer
                .append(&WalEntry::Put {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                })
                .unwrap();

            writer
                .append(&WalEntry::Delete {
                    key: b"key1".to_vec(),
                })
                .unwrap();

            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::new(&wal_path).unwrap();

            let entry1 = reader.next().unwrap().unwrap();
            assert_eq!(
                entry1,
                WalEntry::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec()
                }
            );

            let entry2 = reader.next().unwrap().unwrap();
            assert_eq!(
                entry2,
                WalEntry::Put {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec()
                }
            );

            let entry3 = reader.next().unwrap().unwrap();
            assert_eq!(
                entry3,
                WalEntry::Delete {
                    key: b"key1".to_vec()
                }
            );

            assert!(reader.next().unwrap().is_none());
        }

        std::fs::remove_file(wal_path).ok();
    }

    #[test]
    fn test_wal_truncate() {
        let temp_dir = env::temp_dir();
        let wal_path = temp_dir.join("test_wal_truncate.log");

        let mut writer = WalWriter::create(&wal_path).unwrap();

        writer
            .append(&WalEntry::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .unwrap();
        writer.sync().unwrap();

        assert!(writer.offset() > 0);

        writer.truncate().unwrap();
        assert_eq!(writer.offset(), 0);

        let mut reader = WalReader::new(&wal_path).unwrap();
        assert!(reader.next().unwrap().is_none());

        std::fs::remove_file(wal_path).ok();
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let crc = crc32(data);

        assert_eq!(crc, crc32(data));

        let crc2 = crc32(b"hello world!");
        assert_ne!(crc, crc2);
    }

    #[test]
    fn test_corrupted_checksum() {
        let entry = WalEntry::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        };

        let mut encoded = encode_entry(&entry).unwrap();

        encoded[0] ^= 0xFF;

        let mut reader = &encoded[..];
        let result = decode_entry(&mut reader);

        assert!(matches!(result, Err(WalError::Corrupted(_))));
    }
}
