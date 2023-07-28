#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopicConfig {
    pub write_batch_size: usize,
    pub rw_buffer_capacity: usize,
    pub target_shard_size: usize,
    pub min_shard_size: usize,
    pub subscriber_queue_size: usize,
    pub rw_queue_size: usize,
    pub shard_queue_size: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            write_batch_size: 1024,
            rw_buffer_capacity: 1024 * 1024,
            min_shard_size: 1024 * 1024,
            target_shard_size: 32 * 1024 * 1024,
            subscriber_queue_size: 1024,
            rw_queue_size: 1024,
            shard_queue_size: 128,
        }
    }
}

impl TopicConfig {
    pub fn with_write_batch_size(mut self, size: usize) -> Self {
        self.write_batch_size = size;
        self
    }

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

    pub fn with_streaming_queue_size(mut self, size: usize) -> Self {
        self.subscriber_queue_size = size;
        self
    }

    pub fn with_rw_queue_size(mut self, size: usize) -> Self {
        self.rw_queue_size = size;
        self
    }

    pub fn with_shard_queue_size(mut self, size: usize) -> Self {
        self.shard_queue_size = size;
        self
    }

    pub(crate) fn channel_config(&self) -> ChannelConfig {
        ChannelConfig {
            subscriber_queue_size: self.subscriber_queue_size,
        }
    }

    pub(crate) fn rw_buffer_config(&self) -> RwBufferConfig {
        RwBufferConfig {
            capacity: self.rw_buffer_capacity,
            queue_size: self.rw_queue_size,
            write_batch_size: self.write_batch_size,
        }
    }

    pub(crate) fn shard_config(&self) -> ShardConfig {
        ShardConfig {
            target_shard_size: self.target_shard_size,
            min_shard_size: self.min_shard_size,
            row_group_size: self.min_shard_size,
            write_batch_size: self.write_batch_size,
            queue_size: self.shard_queue_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RwBufferConfig {
    pub capacity: usize,
    pub queue_size: usize,
    pub write_batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub target_shard_size: usize,
    pub min_shard_size: usize,
    pub row_group_size: usize,
    pub write_batch_size: usize,
    pub queue_size: usize,
}

#[derive(Debug, Clone)]
pub struct ChannelConfig {
    pub subscriber_queue_size: usize,
}
