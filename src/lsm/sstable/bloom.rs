/// Bloom filter for probabilistic membership testing
/// - may say "yes" when it's actually "no"
/// - never says "no" when it's actually "yes"
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,

    num_hashes: u32,
}

impl BloomFilter {
    /// # Arguments
    /// * `num_keys` - Expected number of keys to insert
    /// * `bits_per_key` - Number of bits to use per key (10 = ~1% false positive rate)
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let total_bits = num_keys * bits_per_key;

        // at least 64 bits
        let total_bits = total_bits.max(64);

        // calculate optimal number of hash functions: k = ln(2) * (m/n)
        // for bits_per_key, this simplifies to: k = 0.69 * bits_per_key
        let num_hashes = ((bits_per_key as f64) * 0.69).ceil() as u32;
        let num_hashes = num_hashes.clamp(1, 30);

        let num_bytes = (total_bits + 7) / 8;

        Self {
            bits: vec![0u8; num_bytes],
            num_hashes,
        }
    }

    pub fn with_bytes(bytes: Vec<u8>, num_hashes: u32) -> Self {
        Self {
            bits: bytes,
            num_hashes,
        }
    }

    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash(key);
        let total_bits = (self.bits.len() * 8) as u64;

        for i in 0..self.num_hashes {
            // use double hashing: hash_i = h1 + i * h2
            let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % total_bits;
            self.set_bit(bit_pos as usize);
        }
    }

    /// returns true if possibly present, false if definitely absent
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash(key);
        let total_bits = (self.bits.len() * 8) as u64;

        for i in 0..self.num_hashes {
            // use double hashing: hash_i = h1 + i * h2
            let bit_pos = (h1.wrapping_add((i as u64).wrapping_mul(h2))) % total_bits;

            if !self.is_bit_set(bit_pos as usize) {
                // if any bit is not set, definitely not present
                return false;
            }
        }

        // all bits are set, probably present
        true
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }

    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    pub fn size(&self) -> usize {
        self.bits.len()
    }

    /// double hashing using a simple custom hash function
    fn hash(&self, key: &[u8]) -> (u64, u64) {
        // Simple FNV-1a inspired hash for h1
        let mut h1: u64 = 0xcbf29ce484222325; // FNV offset basis
        for &byte in key {
            h1 ^= byte as u64;
            h1 = h1.wrapping_mul(0x100000001b3); // FNV prime
        }

        // Different seed for h2
        let mut h2: u64 = 0x9e3779b97f4a7c15; // Random constant
        for &byte in key {
            h2 = h2.rotate_left(5).wrapping_add(byte as u64);
        }

        // Ensure h2 is non-zero (required for double hashing)
        let h2 = if h2 == 0 { 1 } else { h2 };

        (h1, h2)
    }

    /// Set a bit at the given position
    fn set_bit(&mut self, pos: usize) {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;

        if byte_idx < self.bits.len() {
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    /// Check if a bit is set at the given position
    fn is_bit_set(&self, pos: usize) -> bool {
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;

        if byte_idx < self.bits.len() {
            (self.bits[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }
}

/// Calculate optimal bits per key for a target false positive rate
pub fn bits_per_key_for_fp_rate(fp_rate: f64) -> usize {
    // m/n = -ln(p) / (ln(2)^2)
    // where p is the false positive rate
    let bits_per_key = -fp_rate.ln() / (std::f64::consts::LN_2.powi(2));
    bits_per_key.ceil() as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom = BloomFilter::new(100, 10);

        // Add some keys
        bloom.add(b"apple");
        bloom.add(b"banana");
        bloom.add(b"cherry");

        // Test membership
        assert!(bloom.may_contain(b"apple"));
        assert!(bloom.may_contain(b"banana"));
        assert!(bloom.may_contain(b"cherry"));

        // These should return false (not added)
        // Note: False positives are possible but unlikely with 10 bits/key
        assert!(!bloom.may_contain(b"durian"));
        assert!(!bloom.may_contain(b"elderberry"));
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut bloom = BloomFilter::new(1000, 10);

        // Add many keys
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("key{:03}", i).into_bytes())
            .collect();

        for key in &keys {
            bloom.add(key);
        }

        // All added keys must return true (no false negatives)
        for key in &keys {
            assert!(
                bloom.may_contain(key),
                "False negative for key: {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let num_keys = 1000;
        let bits_per_key = 10;

        let mut bloom = BloomFilter::new(num_keys, bits_per_key);

        // Add keys
        for i in 0..num_keys {
            let key = format!("key{:06}", i);
            bloom.add(key.as_bytes());
        }

        // Test with keys not in the filter
        let mut false_positives = 0;
        let test_count = 10000;

        for i in num_keys..(num_keys + test_count) {
            let key = format!("key{:06}", i);
            if bloom.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let fp_rate = false_positives as f64 / test_count as f64;

        // With 10 bits/key, false positive rate should be around 1%
        // Allow some tolerance (0.5% - 2%)
        println!("False positive rate: {:.2}%", fp_rate * 100.0);
        assert!(
            fp_rate < 0.02,
            "False positive rate too high: {:.2}%",
            fp_rate * 100.0
        );
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut bloom1 = BloomFilter::new(100, 10);

        bloom1.add(b"test1");
        bloom1.add(b"test2");
        bloom1.add(b"test3");

        // Serialize
        let bytes = bloom1.as_bytes().to_vec();
        let num_hashes = bloom1.num_hashes();

        // Deserialize
        let bloom2 = BloomFilter::with_bytes(bytes, num_hashes);

        // Should have same membership results
        assert!(bloom2.may_contain(b"test1"));
        assert!(bloom2.may_contain(b"test2"));
        assert!(bloom2.may_contain(b"test3"));
        assert!(!bloom2.may_contain(b"test4"));
    }

    #[test]
    fn test_bits_per_key_calculation() {
        // For 1% false positive rate
        let bits = bits_per_key_for_fp_rate(0.01);
        assert!(bits >= 9 && bits <= 10);

        // For 0.1% false positive rate
        let bits = bits_per_key_for_fp_rate(0.001);
        assert!(bits >= 14 && bits <= 15);
    }

    #[test]
    fn test_bloom_filter_empty() {
        let bloom = BloomFilter::new(100, 10);

        // Empty filter should return false for everything
        assert!(!bloom.may_contain(b"anything"));
        assert!(!bloom.may_contain(b"test"));
    }

    #[test]
    fn test_bloom_filter_num_hashes() {
        let bloom = BloomFilter::new(100, 10);

        // With 10 bits/key, num_hashes should be around 7
        // (0.69 * 10 = 6.9, rounded up to 7)
        assert_eq!(bloom.num_hashes(), 7);
    }
}
