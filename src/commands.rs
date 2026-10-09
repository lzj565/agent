//! Per-session bounded waiting queue with a process-wide execution limit.
//! Started commands own their permits and finish even when a session closes;
//! dropping the queue discards commands that have not started.

use std::collections::VecDeque;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::Message;

use crate::singbox::Manager;

pub const MAX_RUNNING: usize = 4;
const MAX_PENDING: usize = 16;

pub struct CommandQueue {
    pending: VecDeque<Value>,
    slots: Arc<Semaphore>,
    manager: Manager,
    results: mpsc::Sender<Message>,
}

impl CommandQueue {
    pub fn new(slots: Arc<Semaphore>, manager: Manager, results: mpsc::Sender<Message>) -> Self {
        Self { pending: VecDeque::new(), slots, manager, results }
    }

    /// Admission happens before spawning, so waiting commands cannot turn into
    /// unbounded tasks retaining full configuration payloads.
    pub fn submit(&mut self, command: Value) -> Result<(), Value> {
        if self.pending.is_empty() {
            if let Ok(permit) = self.slots.clone().try_acquire_owned() {
                self.start(command, permit);
                return Ok(());
            }
        }
        if self.pending.len() < MAX_PENDING {
            self.pending.push_back(command);
            Ok(())
        } else {
            Err(json!({
                "type": "command_result", "id": command.get("id").unwrap_or(&Value::Null),
                "ok": false, "error": {"code": "BUSY", "message": "command queue is full"}
            }))
        }
    }

    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Cancellation while waiting for a permit leaves the queue untouched.
    pub async fn start_next(&mut self) {
        let permit = self.slots.clone().acquire_owned().await.expect("command slots are never closed");
        if let Some(command) = self.pending.pop_front() {
            self.start(command, permit);
        }
    }

    fn start(&self, command: Value, permit: OwnedSemaphorePermit) {
        let manager = self.manager.clone();
        let results = self.results.clone();
        tokio::spawn(async move {
            let response = crate::command_result(&manager, &command).await;
            // Hold the slot through delivery, bounding completed responses too.
            // A disconnected session drops the receiver, so delivery cannot
            // strand a permit after a configuration transaction finishes.
            let _ = results.send(Message::Text(response.to_string().into())).await;
            drop(permit);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn admission_is_bounded_and_disconnect_discards_the_waiting_queue() {
        let slots = Arc::new(Semaphore::new(MAX_RUNNING));
        let (tx, mut rx) = mpsc::channel(1);
        let mut queue = CommandQueue::new(slots.clone(), Manager::default(), tx);
        for id in 0..MAX_RUNNING {
            queue.submit(json!({"id": id, "action": "unknown"})).unwrap();
        }
        assert_eq!(slots.available_permits(), 0);
        for id in 0..MAX_PENDING {
            queue.submit(json!({"id": id + 10, "action": "unknown"})).unwrap();
        }
        let busy = queue.submit(json!({"id": "overflow"})).unwrap_err();
        assert_eq!(busy["id"], "overflow");
        assert_eq!(busy["error"]["code"], "BUSY");
        assert_eq!(queue.pending.len(), MAX_PENDING);

        // Fill the result channel: even completed responses keep the process
        // within four active tasks until the session receiver is dropped.
        tokio::task::yield_now().await;
        assert!(slots.available_permits() <= 1);
        drop(queue);
        let mut seen = Vec::new();
        while let Some(Message::Text(text)) = rx.recv().await {
            seen.push(serde_json::from_str::<Value>(&text).unwrap()["id"].as_u64().unwrap());
        }
        seen.sort_unstable();
        assert_eq!(seen, [0, 1, 2, 3], "queued commands must never start after disconnect");
        assert_eq!(slots.available_permits(), MAX_RUNNING);
    }

    #[tokio::test]
    async fn queued_commands_start_in_order_and_slots_are_shared_between_sessions() {
        let slots = Arc::new(Semaphore::new(MAX_RUNNING));
        let held = slots.clone().acquire_many_owned(MAX_RUNNING as u32).await.unwrap();
        let (tx, mut rx) = mpsc::channel(4);
        let mut queue = CommandQueue::new(slots.clone(), Manager::default(), tx.clone());
        queue.submit(json!({"id": 1, "action": "unknown"})).unwrap();
        queue.submit(json!({"id": 2, "action": "unknown"})).unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(10), queue.start_next())
            .await
            .is_err());
        assert_eq!(queue.pending.len(), 2, "cancelled permit wait must not consume a command");
        let mut next_session = CommandQueue::new(slots.clone(), Manager::default(), tx);
        next_session.submit(json!({"id": 3, "action": "unknown"})).unwrap();
        assert!(next_session.has_pending(), "reconnect cannot bypass running old commands");
        drop(held);
        queue.start_next().await;
        let Message::Text(text) = rx.recv().await.unwrap() else { panic!("expected command result") };
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["id"], 1);
        queue.start_next().await;
        let Message::Text(text) = rx.recv().await.unwrap() else { panic!("expected command result") };
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap()["id"], 2);
    }
}
