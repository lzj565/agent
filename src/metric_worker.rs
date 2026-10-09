//! A single process-lifetime thread owns metric baselines and blocking Linux
//! I/O. Requests are bounded and cancelled, unstarted requests are skipped.

use anyhow::{Context, Result};
use tokio::sync::{mpsc, oneshot};

use crate::collect::{Collector, Facts, Metrics};

pub trait Source: Send + 'static {
    fn facts(&self) -> Facts;
    fn collect(&mut self) -> Metrics;
}

impl Source for Collector {
    fn facts(&self) -> Facts {
        Collector::facts(self)
    }

    fn collect(&mut self) -> Metrics {
        Collector::collect(self)
    }
}

enum Request {
    Facts(oneshot::Sender<Facts>),
    Metrics(oneshot::Sender<Metrics>),
}

#[derive(Clone)]
pub struct Worker {
    requests: mpsc::Sender<Request>,
}

impl Worker {
    pub fn new(mut source: impl Source) -> Result<Self> {
        let (requests, mut rx) = mpsc::channel(1);
        // A dedicated thread rather than one spawn_blocking per tick: an I/O
        // stall can occupy one thread, never an expanding blocking-task pool.
        std::thread::Builder::new()
            .name("monitor-metrics".into())
            .spawn(move || {
                while let Some(request) = rx.blocking_recv() {
                    match request {
                        Request::Facts(reply) if !reply.is_closed() => {
                            let _ = reply.send(source.facts());
                        }
                        Request::Metrics(reply) if !reply.is_closed() => {
                            let _ = reply.send(source.collect());
                        }
                        _ => {}
                    }
                }
            })
            .context("start metrics worker")?;
        Ok(Self { requests })
    }

    pub async fn facts(&self) -> Result<Facts> {
        let (reply, response) = oneshot::channel();
        self.requests.send(Request::Facts(reply)).await.context("metrics worker stopped")?;
        response.await.context("metrics worker stopped while collecting facts")
    }

    pub async fn collect(&self) -> Result<Metrics> {
        let (reply, response) = oneshot::channel();
        self.requests.send(Request::Metrics(reply)).await.context("metrics worker stopped")?;
        response.await.context("metrics worker stopped while collecting metrics")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Counter {
        calls: u32,
        threads: std::sync::Arc<std::sync::Mutex<Vec<std::thread::ThreadId>>>,
    }

    impl Source for Counter {
        fn facts(&self) -> Facts {
            Collector::default().facts()
        }

        fn collect(&mut self) -> Metrics {
            self.calls += 1;
            self.threads.lock().unwrap().push(std::thread::current().id());
            Metrics { procs: self.calls, ..Metrics::default() }
        }
    }

    #[tokio::test]
    async fn reconnects_keep_state_and_all_samples_run_on_one_background_thread() {
        let threads = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let worker = Worker::new(Counter { calls: 0, threads: threads.clone() }).unwrap();
        assert_eq!(worker.collect().await.unwrap().procs, 1);
        let next_session = worker.clone();
        assert_eq!(next_session.collect().await.unwrap().procs, 2);
        let (a, b) = tokio::join!(worker.collect(), next_session.collect());
        assert_eq!([a.unwrap().procs, b.unwrap().procs], [3, 4]);
        let threads = threads.lock().unwrap();
        assert!(threads.iter().all(|id| *id == threads[0]));
        assert_ne!(threads[0], std::thread::current().id());
    }

    #[tokio::test]
    async fn cancelled_queued_samples_are_skipped_and_the_request_queue_is_bounded() {
        struct Stalled {
            entered: Option<oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            calls: u32,
        }
        impl Source for Stalled {
            fn facts(&self) -> Facts {
                Collector::default().facts()
            }
            fn collect(&mut self) -> Metrics {
                self.calls += 1;
                if let Some(entered) = self.entered.take() {
                    entered.send(()).unwrap();
                    self.release.recv().unwrap();
                }
                Metrics { procs: self.calls, ..Metrics::default() }
            }
        }
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let worker = Worker::new(Stalled { entered: Some(entered), release: wait, calls: 0 }).unwrap();
        let clone = worker.clone();
        let first = tokio::spawn(async move { clone.collect().await });
        started.await.unwrap();
        let (reply, cancelled) = oneshot::channel();
        worker.requests.try_send(Request::Metrics(reply)).unwrap();
        let (reply, _) = oneshot::channel();
        assert!(matches!(
            worker.requests.try_send(Request::Metrics(reply)),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        drop(cancelled);
        release.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap().procs, 1);
        assert_eq!(worker.collect().await.unwrap().procs, 2, "cancelled queued request must not sample");
    }
}
