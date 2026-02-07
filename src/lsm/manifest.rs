use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Manifest tracks all SSTable files and LSM state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,

    pub levels: Vec<Level>,

    pub next_sstable_id: u64,

    pub wal_seq: u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Level {
    pub level: usize,

    pub sstables: Vec<SSTableMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    pub id: u64,

    pub level: usize,

    pub path: PathBuf,

    pub size: u64,

    pub num_entries: u64,

    pub min_key: Vec<u8>,

    pub max_key: Vec<u8>,
}

#[derive(Debug)]
pub enum ManifestError {
    Io(io::Error),
    Serialization(serde_json::Error),
    Corrupted(String),
}

impl From<io::Error> for ManifestError {
    fn from(err: io::Error) -> Self {
        ManifestError::Io(err)
    }
}

impl From<serde_json::Error> for ManifestError {
    fn from(err: serde_json::Error) -> Self {
        ManifestError::Serialization(err)
    }
}

impl std::fmt::Display for ManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestError::Io(e) => write!(f, "I/O error: {}", e),
            ManifestError::Serialization(e) => write!(f, "Serialization error: {}", e),
            ManifestError::Corrupted(msg) => write!(f, "Corrupted manifest: {}", msg),
        }
    }
}

impl std::error::Error for ManifestError {}

pub type Result<T> = std::result::Result<T, ManifestError>;

impl Manifest {
    /// create a new empty manifest
    pub fn new(max_levels: usize) -> Self {
        let mut levels = Vec::with_capacity(max_levels);
        for level in 0..max_levels {
            levels.push(Level {
                level,
                sstables: Vec::new(),
            });
        }

        Self {
            version: 1,
            levels,
            next_sstable_id: 1,
            wal_seq: 1,
        }
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(ManifestError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "Manifest file not found",
            )));
        }

        let contents = fs::read_to_string(path)?;
        let manifest: Manifest = serde_json::from_str(&contents)?;

        if manifest.levels.is_empty() {
            return Err(ManifestError::Corrupted(
                "Manifest has no levels".to_string(),
            ));
        }

        Ok(manifest)
    }

    /// save manifest to disk atomically (write temp, sync, rename)
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");

        let json = serde_json::to_string_pretty(self)?;

        let mut file = File::create(&temp_path)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        drop(file);

        fs::rename(&temp_path, path)?;

        // Sync parent directory for durability (cross-platform)
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }

        Ok(())
    }

    pub fn add_sstable(&mut self, level: usize, metadata: SSTableMetadata) {
        if level < self.levels.len() {
            self.levels[level].sstables.push(metadata);
            self.version += 1;
        }
    }

    pub fn remove_sstables(&mut self, sstables: &[SSTableMetadata]) {
        for sst in sstables {
            if sst.level < self.levels.len() {
                self.levels[sst.level]
                    .sstables
                    .retain(|s| s.id != sst.id);
            }
        }
        self.version += 1;
    }

    pub fn get_level(&self, level: usize) -> &[SSTableMetadata] {
        if level < self.levels.len() {
            &self.levels[level].sstables
        } else {
            &[]
        }
    }

    pub fn find_overlapping(
        &self,
        level: usize,
        min_key: &[u8],
        max_key: &[u8],
    ) -> Vec<SSTableMetadata> {
        if level >= self.levels.len() {
            return Vec::new();
        }

        self.levels[level]
            .sstables
            .iter()
            .filter(|sst| {
                !(sst.max_key.as_slice() < min_key || sst.min_key.as_slice() > max_key)
            })
            .cloned()
            .collect()
    }

    pub fn next_sstable_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }

    pub fn next_wal_seq(&mut self) -> u64 {
        let seq = self.wal_seq;
        self.wal_seq += 1;
        seq
    }
}

/// Sync directory metadata to disk (Unix/Linux)
#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()?;
    Ok(())
}

/// Sync directory metadata to disk (Windows)
#[cfg(windows)]
fn sync_dir(path: &Path) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    // FILE_FLAG_BACKUP_SEMANTICS (0x02000000) allows opening directories on Windows
    if let Ok(dir) = OpenOptions::new()
        .read(true)
        .custom_flags(0x02000000)
        .open(path)
    {
        let _ = dir.sync_all();
    }

    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn sync_dir(_path: &Path) -> Result<()> {
    // No-op on unsupported platforms
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_new_manifest() {
        let manifest = Manifest::new(5);
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.levels.len(), 5);
        assert_eq!(manifest.next_sstable_id, 1);
        assert_eq!(manifest.wal_seq, 1);
    }

    #[test]
    fn test_save_and_load() {
        let temp_dir = env::temp_dir();
        let manifest_path = temp_dir.join("test_manifest.json");

        let mut manifest = Manifest::new(3);
        manifest.add_sstable(
            0,
            SSTableMetadata {
                id: 1,
                level: 0,
                path: PathBuf::from("test.sst"),
                size: 1024,
                num_entries: 10,
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
            },
        );

        manifest.save(&manifest_path).unwrap();

        let loaded = Manifest::load(&manifest_path).unwrap();
        assert_eq!(loaded.version, 2); // Version incremented by add_sstable
        assert_eq!(loaded.levels[0].sstables.len(), 1);
        assert_eq!(loaded.levels[0].sstables[0].id, 1);

        fs::remove_file(manifest_path).ok();
    }

    #[test]
    fn test_find_overlapping() {
        let mut manifest = Manifest::new(3);

        manifest.add_sstable(
            1,
            SSTableMetadata {
                id: 1,
                level: 1,
                path: PathBuf::from("sst1.sst"),
                size: 1024,
                num_entries: 10,
                min_key: b"a".to_vec(),
                max_key: b"c".to_vec(),
            },
        );

        manifest.add_sstable(
            1,
            SSTableMetadata {
                id: 2,
                level: 1,
                path: PathBuf::from("sst2.sst"),
                size: 1024,
                num_entries: 10,
                min_key: b"e".to_vec(),
                max_key: b"g".to_vec(),
            },
        );

        let overlapping = manifest.find_overlapping(1, b"b", b"f");
        assert_eq!(overlapping.len(), 2); // Both overlap

        let overlapping = manifest.find_overlapping(1, b"a", b"b");
        assert_eq!(overlapping.len(), 1); // Only first overlaps
        assert_eq!(overlapping[0].id, 1);

        let overlapping = manifest.find_overlapping(1, b"x", b"z");
        assert_eq!(overlapping.len(), 0); // No overlap
    }

    #[test]
    fn test_remove_sstables() {
        let mut manifest = Manifest::new(3);

        let sst1 = SSTableMetadata {
            id: 1,
            level: 0,
            path: PathBuf::from("sst1.sst"),
            size: 1024,
            num_entries: 10,
            min_key: b"a".to_vec(),
            max_key: b"c".to_vec(),
        };

        let sst2 = SSTableMetadata {
            id: 2,
            level: 0,
            path: PathBuf::from("sst2.sst"),
            size: 1024,
            num_entries: 10,
            min_key: b"d".to_vec(),
            max_key: b"f".to_vec(),
        };

        manifest.add_sstable(0, sst1.clone());
        manifest.add_sstable(0, sst2);
        assert_eq!(manifest.levels[0].sstables.len(), 2);

        manifest.remove_sstables(&[sst1]);
        assert_eq!(manifest.levels[0].sstables.len(), 1);
        assert_eq!(manifest.levels[0].sstables[0].id, 2);
    }
}
