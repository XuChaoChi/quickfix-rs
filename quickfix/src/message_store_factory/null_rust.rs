use std::collections::HashMap;

use crate::SessionId;

use super::custom::{MessageStoreCallback, MessageStoreFactoryCallback};

/// A no-op message store implemented in Rust via [`MessageStoreCallback`].
///
/// Stores messages in memory and discards them on reset. Useful as a starting
/// point or for applications that do not require message persistence.
pub struct RustNullStore {
    msgs: HashMap<u64, String>,
    next_sender: u64,
    next_target: u64,
    creation_time: i64,
}

impl RustNullStore {
    fn new(creation_time: i64) -> Self {
        Self {
            msgs: HashMap::new(),
            next_sender: 1,
            next_target: 1,
            creation_time,
        }
    }
}

impl MessageStoreCallback for RustNullStore {
    fn set(&mut self, seq_num: u64, msg: &str) -> bool {
        self.msgs.insert(seq_num, msg.to_owned());
        true
    }

    fn get(&self, begin_seq_num: u64, end_seq_num: u64, msgs: &mut Vec<String>) {
        for seq in begin_seq_num..=end_seq_num {
            if let Some(msg) = self.msgs.get(&seq) {
                msgs.push(msg.clone());
            }
        }
    }

    fn next_sender_seq_num(&self) -> u64 {
        self.next_sender
    }

    fn next_target_seq_num(&self) -> u64 {
        self.next_target
    }

    fn set_next_sender_seq_num(&mut self, value: u64) {
        self.next_sender = value;
    }

    fn set_next_target_seq_num(&mut self, value: u64) {
        self.next_target = value;
    }

    fn incr_next_sender_seq_num(&mut self) {
        self.next_sender += 1;
    }

    fn incr_next_target_seq_num(&mut self) {
        self.next_target += 1;
    }

    fn creation_time(&self) -> i64 {
        self.creation_time
    }

    fn reset(&mut self, now: i64) {
        self.msgs.clear();
        self.next_sender = 1;
        self.next_target = 1;
        self.creation_time = now;
    }

    fn refresh(&mut self) {}
}

/// Factory that creates [`RustNullStore`] instances for each session.
pub struct RustNullStoreFactory;

impl MessageStoreFactoryCallback for RustNullStoreFactory {
    type Store = RustNullStore;

    fn create(&self, _session: &SessionId, now: i64) -> RustNullStore {
        RustNullStore::new(now)
    }
}
