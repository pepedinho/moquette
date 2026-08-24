use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

const CUSTOM_EPOCH: u64 = 1_767_225_600_000;

pub struct SnowFlake {
    node_id: u64,
    state: Mutex<SnowFlakeState>,
}

struct SnowFlakeState {
    last_timestamp: u64,
    sequence: u64,
}

impl SnowFlake {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id: node_id & 0x3FF, // keep only 10 bits
            state: Mutex::new(SnowFlakeState {
                last_timestamp: 0,
                sequence: 0,
            }),
        }
    }

    pub fn generate_id(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        let mut now = current_time_ms();

        if now == state.last_timestamp {
            state.sequence = (state.sequence + 1) & 0xFFF;

            if state.sequence == 0 {
                while now <= state.last_timestamp {
                    now = current_time_ms();
                }
            }
        } else {
            state.sequence = 0;
        }

        state.last_timestamp = now;

        ((now - CUSTOM_EPOCH) << 22) | (self.node_id << 12) | state.sequence
    }

    pub fn generate_client_id(&self) -> String {
        format!("auto-{}", self.generate_id())
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}
