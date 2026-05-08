use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::fs::File;
use std::io::Write;
use crate::task::Task;

#[derive(Default)]
struct MetricsData {
    completed_tasks: usize,
    cpu_completed: usize,
    io_completed: usize,
    total_wait_time: Duration,
    total_wait_time_io: Duration,
    total_wait_time_cpu: Duration,
    total_turnaround_time: Duration,
    max_wait_time: Duration,
    queue_length_samples: Vec<usize>,
    worker_busy_time: Vec<Duration>,
    active_workers_samples: Vec<usize>,
    monitor_samples: Vec<MonitorSample>,
}

#[derive(Clone)]
struct MonitorSample {
    timestamp: Duration,
    queue_length: usize,
    active_workers: usize,
    completed_tasks: usize,
}

pub struct Metrics {
    data: Arc<Mutex<MetricsData>>,
    start_time: Instant,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(MetricsData::default())),
            start_time: Instant::now(),
        }
    }
    
    pub fn record_completion(&self, task: &Task, worker_id: usize, execution_time: Duration) {
        let mut data = self.data.lock().unwrap();
        data.completed_tasks += 1;
        
        match task.kind {
            crate::task::TaskKind::Cpu => data.cpu_completed += 1,
            crate::task::TaskKind::Io => data.io_completed += 1,
        }
        
        if let Some(wait) = task.wait_time() {
            data.total_wait_time += wait;
            
            match task.kind {
                crate::task::TaskKind::Cpu => data.total_wait_time_cpu += wait,
                crate::task::TaskKind::Io => data.total_wait_time_io += wait,
            }
            
            if wait > data.max_wait_time {
                data.max_wait_time = wait;
            }
        }
        
        if let Some(turnaround) = task.turnaround_time() {
            data.total_turnaround_time += turnaround;
        }
        
        if worker_id >= data.worker_busy_time.len() {
            data.worker_busy_time.resize(worker_id + 1, Duration::ZERO);
        }
        data.worker_busy_time[worker_id] += execution_time;
    }
    
    pub fn record_queue_length(&self, len: usize, active_workers: usize) {
        let mut data = self.data.lock().unwrap();
        data.queue_length_samples.push(len);
        data.active_workers_samples.push(active_workers);
        
        // Record monitor sample every 100ms
        let now = self.start_time.elapsed();
        
        // Clone the completed_tasks value before using it
        let completed = data.completed_tasks;
        
        if data.monitor_samples.is_empty() || 
           (now - data.monitor_samples.last().unwrap().timestamp) >= Duration::from_millis(100) {
            data.monitor_samples.push(MonitorSample {
                timestamp: now,
                queue_length: len,
                active_workers,
                completed_tasks: completed,
            });
        }
    }
    
    pub fn get_completed_count(&self) -> usize {
        self.data.lock().unwrap().completed_tasks
    }
    
    pub fn get_makespan(&self) -> Duration {
        self.start_time.elapsed()
    }
    
    pub fn get_avg_wait_time(&self) -> Duration {
        let data = self.data.lock().unwrap();
        if data.completed_tasks > 0 {
            data.total_wait_time / data.completed_tasks as u32
        } else {
            Duration::ZERO
        }
    }
    
    pub fn get_avg_wait_time_io(&self) -> Duration {
        let data = self.data.lock().unwrap();
        if data.io_completed > 0 {
            data.total_wait_time_io / data.io_completed as u32
        } else {
            Duration::ZERO
        }
    }
    
    pub fn get_avg_wait_time_cpu(&self) -> Duration {
        let data = self.data.lock().unwrap();
        if data.cpu_completed > 0 {
            data.total_wait_time_cpu / data.cpu_completed as u32
        } else {
            Duration::ZERO
        }
    }
    
    pub fn get_avg_turnaround_time(&self) -> Duration {
        let data = self.data.lock().unwrap();
        if data.completed_tasks > 0 {
            data.total_turnaround_time / data.completed_tasks as u32
        } else {
            Duration::ZERO
        }
    }
    
    pub fn get_max_wait_time(&self) -> Duration {
        self.data.lock().unwrap().max_wait_time
    }
    
    pub fn get_avg_cpu_usage(&self) -> f64 {
        let data = self.data.lock().unwrap();
        let makespan = self.start_time.elapsed();
        let total_busy: Duration = data.worker_busy_time.iter().sum();
        let max_possible = makespan * data.worker_busy_time.len() as u32;
        
        if max_possible.as_secs_f64() > 0.0 {
            total_busy.as_secs_f64() / max_possible.as_secs_f64() * 100.0
        } else {
            0.0
        }
    }
    
    pub fn get_avg_workers_active(&self) -> f64 {
        let data = self.data.lock().unwrap();
        if !data.active_workers_samples.is_empty() {
            data.active_workers_samples.iter().sum::<usize>() as f64 / data.active_workers_samples.len() as f64
        } else {
            0.0
        }
    }
    
    pub fn get_monitor_sample_count(&self) -> usize {
        self.data.lock().unwrap().monitor_samples.len()
    }
    
    pub fn save_monitor_csv(&self, filename: &str) -> anyhow::Result<()> {
        let data = self.data.lock().unwrap();
        let mut file = File::create(filename)?;
        
        writeln!(file, "timestamp_ms,queue_length,active_workers,completed_tasks")?;
        
        for sample in &data.monitor_samples {
            writeln!(file, "{:.0},{},{},{}",
                     sample.timestamp.as_secs_f64() * 1000.0,
                     sample.queue_length,
                     sample.active_workers,
                     sample.completed_tasks)?;
        }
        
        Ok(())
    }
}

impl Clone for Metrics {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            start_time: self.start_time,
        }
    }
}