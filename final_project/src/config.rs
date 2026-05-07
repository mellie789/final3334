#[derive(Debug, Clone)]
pub struct Config {
    pub num_workers: usize,
    pub num_tasks: usize,
    pub cpu_ratio: f64,
    pub arrival_spread_ms: u64,
    pub cpu_duration_ms: (u64, u64),
    pub io_duration_ms: (u64, u64),
    pub burst_mode: bool,
}

#[derive(Debug, Clone)]
pub enum SchedulingPolicy {
    Fifo,
    WeightedRoundRobin { cpu_weight: usize, io_weight: usize },
}

impl Config {
    pub fn balanced() -> Self {
        Self {
            num_workers: 8,
            num_tasks: 500,
            cpu_ratio: 0.5,
            arrival_spread_ms: 5000,
            cpu_duration_ms: (20, 100),
            io_duration_ms: (5, 30),
            burst_mode: false,
        }
    }
    
    pub fn stressed() -> Self {
        Self {
            num_workers: 8,
            num_tasks: 500,
            cpu_ratio: 0.85,
            arrival_spread_ms: 5000,
            cpu_duration_ms: (50, 150),
            io_duration_ms: (5, 20),
            burst_mode: true,
        }
    }
}