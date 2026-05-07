use rand::prelude::*;
use rand::rngs::StdRng;
use std::time::{Duration, Instant};
use crate::task::{Task, TaskKind};
use crate::config::Config;

pub fn generate_tasks(config: &Config, rng: &mut StdRng) -> Vec<Task> {
    let mut tasks = Vec::with_capacity(config.num_tasks);
    let start_time = Instant::now();
    
    for id in 0..config.num_tasks {
        let kind = if rng.gen_bool(config.cpu_ratio) {
            TaskKind::Cpu
        } else {
            TaskKind::Io
        };
        
        let duration_ms = match kind {
            TaskKind::Cpu => rng.gen_range(config.cpu_duration_ms.0..=config.cpu_duration_ms.1),
            TaskKind::Io => rng.gen_range(config.io_duration_ms.0..=config.io_duration_ms.1),
        };
        
        let arrival_offset_ms = if config.burst_mode {
            if id % 100 < 80 { 0 } else { rng.gen_range(0..=100) }
        } else {
            rng.gen_range(0..=config.arrival_spread_ms)
        };
        
        let task = Task::new(
            id as u64,
            kind,
            Duration::from_millis(duration_ms),
            start_time + Duration::from_millis(arrival_offset_ms),
        );
        
        tasks.push(task);
    }
    
    tasks.sort_by_key(|t| t.created_at);
    tasks
}