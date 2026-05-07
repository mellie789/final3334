use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_channel::Receiver;
use crate::task::Task;
use crate::metrics::Metrics;

pub struct Worker {
    id: usize,
    receiver: Receiver<Task>,
    metrics: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
}

impl Worker {
    pub fn new(
        id: usize,
        receiver: Receiver<Task>,
        metrics: Arc<Metrics>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            id,
            receiver,
            metrics,
            shutdown,
        }
    }
    
    pub fn start(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            while !self.shutdown.load(Ordering::Relaxed) {
                match self.receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(mut task) => {
                        let start = Instant::now();
                        task.start(start);
                        
                        thread::sleep(task.duration);
                        
                        task.complete(Instant::now());
                        self.metrics.record_completion(&task, self.id, start.elapsed());
                    }
                    Err(_) => continue,
                }
            }
        })
    }
}