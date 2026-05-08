use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::collections::VecDeque;
use crossbeam_channel::{Sender, Receiver, unbounded};
use crate::task::{Task, TaskKind};
use crate::metrics::Metrics;
use crate::config::SchedulingPolicy;

pub struct Dispatcher {
    cpu_queue: Arc<Mutex<VecDeque<Task>>>,
    io_queue: Arc<Mutex<VecDeque<Task>>>,
    worker_senders: Vec<Sender<Task>>,
    worker_active: Arc<Mutex<Vec<bool>>>,
    metrics: Arc<Metrics>,
    shutdown: Arc<AtomicBool>,
    policy: SchedulingPolicy,
}

impl Dispatcher {
    pub fn new_with_monitor(
        num_workers: usize,
        metrics: Arc<Metrics>,
        shutdown: Arc<AtomicBool>,
        policy: SchedulingPolicy,
    ) -> Self {
        let mut worker_senders: Vec<Sender<Task>> = Vec::new();
        let mut worker_receivers: Vec<Receiver<Task>> = Vec::new();
        
        for _ in 0..num_workers {
            let (tx, rx) = unbounded();
            worker_senders.push(tx);
            worker_receivers.push(rx);
        }
        
        let worker_active = Arc::new(Mutex::new(vec![false; num_workers]));
        
        for (id, rx) in worker_receivers.into_iter().enumerate() {
            let worker_active_clone = worker_active.clone();
            let metrics_clone = metrics.clone();
            let shutdown_clone = shutdown.clone();
            
            thread::spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    match rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(mut task) => {
                            {
                                let mut active = worker_active_clone.lock().unwrap();
                                active[id] = true;
                            }
                            
                            let start = std::time::Instant::now();
                            task.start(start);
                            thread::sleep(task.duration);
                            task.complete(std::time::Instant::now());
                            metrics_clone.record_completion(&task, id, start.elapsed());
                            
                            {
                                let mut active = worker_active_clone.lock().unwrap();
                                active[id] = false;
                            }
                        }
                        Err(_) => continue,
                    }
                }
            });
        }
        
        Self {
            cpu_queue: Arc::new(Mutex::new(VecDeque::new())),
            io_queue: Arc::new(Mutex::new(VecDeque::new())),
            worker_senders,
            worker_active,
            metrics,
            shutdown,
            policy,
        }
    }
    
    pub fn submit_task(&self, task: Task) {
        match task.kind {
            TaskKind::Cpu => self.cpu_queue.lock().unwrap().push_back(task),
            TaskKind::Io => self.io_queue.lock().unwrap().push_back(task),
        }
    }
    
    pub fn run(&self) {
    let (cpu_weight, io_weight) = match &self.policy {
        SchedulingPolicy::Fifo => (1, 1),
        SchedulingPolicy::WeightedRoundRobin { cpu_weight, io_weight } => (*cpu_weight, *io_weight),
    };
    
    let mut cpu_tokens = cpu_weight;
    let mut io_tokens = io_weight;
    let mut next_worker = 0;
    
    while !self.shutdown.load(Ordering::Relaxed) {
        let cpu_len = self.cpu_queue.lock().unwrap().len();
        let io_len = self.io_queue.lock().unwrap().len();
        
        let active_count = self.worker_active.lock().unwrap().iter().filter(|&a| *a).count();
        self.metrics.record_queue_length(cpu_len + io_len, active_count);
        
        // If both queues are empty, sleep and continue
        if cpu_len == 0 && io_len == 0 {
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        
        let task = match self.policy {
            SchedulingPolicy::Fifo => {
                if let Some(task) = self.cpu_queue.lock().unwrap().pop_front() {
                    Some(task)
                } else {
                    self.io_queue.lock().unwrap().pop_front()
                }
            }
            SchedulingPolicy::WeightedRoundRobin { .. } => {
                let mut selected = None;
                
                // Try CPU if tokens available AND queue not empty
                if cpu_tokens > 0 && cpu_len > 0 {
                    if let Some(task) = self.cpu_queue.lock().unwrap().pop_front() {
                        selected = Some(task);
                        cpu_tokens -= 1;
                    }
                }
                
                // Try IO if tokens available AND queue not empty AND no CPU task selected
                if selected.is_none() && io_tokens > 0 && io_len > 0 {
                    if let Some(task) = self.io_queue.lock().unwrap().pop_front() {
                        selected = Some(task);
                        io_tokens -= 1;
                    }
                }
                
                // If still no task selected (tokens but empty queues, or both tokens zero)
                if selected.is_none() {
                    // Replenish tokens
                    cpu_tokens = cpu_weight;
                    io_tokens = io_weight;
                    
                    // Try again with replenished tokens
                    if cpu_tokens > 0 && cpu_len > 0 {
                        if let Some(task) = self.cpu_queue.lock().unwrap().pop_front() {
                            selected = Some(task);
                            cpu_tokens -= 1;
                        }
                    }
                    
                    if selected.is_none() && io_tokens > 0 && io_len > 0 {
                        if let Some(task) = self.io_queue.lock().unwrap().pop_front() {
                            selected = Some(task);
                            io_tokens -= 1;
                        }
                    }
                }
                
                selected
            }
        };
        
        if let Some(task) = task {
            let worker_id = next_worker;
            next_worker = (next_worker + 1) % self.worker_senders.len();
            if let Err(e) = self.worker_senders[worker_id].send(task) {
                eprintln!("Failed to send task: {}", e);
            }
        } else {
            // No tasks available, sleep briefly
            thread::sleep(Duration::from_millis(10));
            }
        }
    }
}