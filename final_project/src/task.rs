use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Task {
    #[allow(dead_code)]
    pub id: u64,
    pub kind: TaskKind,
    pub duration: Duration,
    pub created_at: Instant,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskKind {
    Cpu,
    Io,
}

impl Task {
    pub fn new(id: u64, kind: TaskKind, duration: Duration, created_at: Instant) -> Self {
        Self {
            id,
            kind,
            duration,
            created_at,
            started_at: None,
            completed_at: None,
        }
    }
    
    pub fn wait_time(&self) -> Option<Duration> {
        self.started_at.map(|start| start - self.created_at)
    }
    
    pub fn turnaround_time(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(_start), Some(end)) => Some(end - self.created_at),
            _ => None,
        }
    }
    
    pub fn start(&mut self, start_time: Instant) {
        self.started_at = Some(start_time);
    }
    
    pub fn complete(&mut self, complete_time: Instant) {
        self.completed_at = Some(complete_time);
    }
}