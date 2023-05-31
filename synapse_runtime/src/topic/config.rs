#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub rw_buffer_capacity: usize,
    pub target_shard_size: usize,
    pub min_shard_size: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            rw_buffer_capacity: 1024,              // ~1K rows
            min_shard_size: 1024 * 1024,           // ~1M rows
            target_shard_size: 1024 * 1024 * 1024, // ~1B rows
        }
    }
}

impl TopicConfig {
    pub fn with_min_shard_size(mut self, size: usize) -> Self {
        self.min_shard_size = size;
        self
    }

    pub fn with_target_shard_size(mut self, size: usize) -> Self {
        self.target_shard_size = size;
        self
    }

    pub fn with_rw_buffer_capacity(mut self, capacity: usize) -> Self {
        self.rw_buffer_capacity = capacity;
        self
    }

    pub(crate) fn rw_buffer_config(&self) -> RwBufferConfig {
        RwBufferConfig {
            capacity: self.rw_buffer_capacity,
        }
    }

    pub(crate) fn shard_config(&self) -> ShardConfig {
        ShardConfig {
            target_shard_size: self.target_shard_size,
            min_shard_size: self.min_shard_size,
            row_group_size: self.min_shard_size,
            write_batch_size: self.rw_buffer_capacity,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RwBufferConfig {
    pub capacity: usize,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub target_shard_size: usize,
    pub min_shard_size: usize,
    pub row_group_size: usize,
    pub write_batch_size: usize,
}
