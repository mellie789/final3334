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
