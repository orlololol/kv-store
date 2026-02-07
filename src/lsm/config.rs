#[derive(Debug, Clone)]
pub struct LSMConfig {
    pub memtable_size: usize,

    pub l0_compaction_trigger: usize,

    pub level_multiplier: usize,

    pub target_file_size: usize,

    pub block_size: usize,

    pub block_cache_size: usize,

    pub bloom_bits_per_key: usize,

    pub max_levels: usize,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size: 2 * 1024 * 1024,        // 2 MB
            l0_compaction_trigger: 3,               // 3 files
            level_multiplier: 10,                   // 10x growth
            target_file_size: 4 * 1024 * 1024,     // 4 MB
            block_size: 4096,                       // 4 KB
            block_cache_size: 4 * 1024 * 1024,     // 4 MB
            bloom_bits_per_key: 10,                 // ~1% false positive
            max_levels: 5,                          // Supports ~400 MB
        }
    }
}

impl LSMConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_level_size(&self, level: usize) -> u64 {
        if level == 0 {
            // L0 is based on number of files, not total size
            (self.l0_compaction_trigger * self.target_file_size) as u64
        } else {
            (self.target_file_size as u64)
                * (self.level_multiplier as u64).pow(level as u32)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LSMConfig::default();
        assert_eq!(config.memtable_size, 2 * 1024 * 1024);
        assert_eq!(config.l0_compaction_trigger, 3);
        assert_eq!(config.block_size, 4096);
    }

    #[test]
    fn test_level_sizes() {
        let config = LSMConfig::default();

        // L0: 3 files × 4 MB = 12 MB
        assert_eq!(config.max_level_size(0), 12 * 1024 * 1024);

        // L1: 4 MB × 10^1 = 40 MB
        assert_eq!(config.max_level_size(1), 40 * 1024 * 1024);

        // L2: 4 MB × 10^2 = 400 MB
        assert_eq!(config.max_level_size(2), 400 * 1024 * 1024);
    }
}
