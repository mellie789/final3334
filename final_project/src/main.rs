mod task;
mod config;
mod metrics;
mod generator;
mod dispatcher;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use rand::SeedableRng;
use rand::rngs::StdRng;

use config::{Config, SchedulingPolicy};
use metrics::Metrics;
use generator::generate_tasks;
use dispatcher::Dispatcher;

fn main() -> anyhow::Result<()> {
    let num_tasks = 500;  // Can be 500, 1000, or any number >= 500
    let io_percent = 0.70;  // 70% IO, 30% CPU
    
    let base_config = Config {
        num_workers: 8,
        num_tasks,
        cpu_ratio: 1.0 - io_percent,
        arrival_spread_ms: 5000,
        cpu_duration_ms: (20, 100),
        io_duration_ms: (5, 30),
        burst_mode: false,
    };
    
    // Run FIFO
    println!("\n");
    println!("┌────────────────────────────────────────────────────────┐");
    println!("│                    POLICY 1: FIFO                      │");
    println!("└────────────────────────────────────────────────────────┘");
    run_simulation(base_config.clone(), SchedulingPolicy::Fifo, "fifo")?;
    
    // Run Optimized (Weighted Round-Robin)
    println!("\n");
    println!("┌────────────────────────────────────────────────────────┐");
    println!("│              POLICY 2: OPTIMIZED (Weighted RR)         │");
    println!("│                 CPU Weight: 2, IO Weight: 1            │");
    println!("└────────────────────────────────────────────────────────┘");
    run_simulation(base_config, SchedulingPolicy::WeightedRoundRobin { 
        cpu_weight: 2, 
        io_weight: 1 
    }, "weighted_rr")?;
    
    Ok(())
}

fn run_simulation(config: Config, policy: SchedulingPolicy, suffix: &str) -> anyhow::Result<()> {
    let policy_name = match &policy {
        SchedulingPolicy::Fifo => "FIFO",
        SchedulingPolicy::WeightedRoundRobin { .. } => "Weighted RR (2:1)",
    };
    
    println!("\n=== OPTIMIZED SIMULATION ===");
    println!("{} tasks, {:.0}% IO / {:.0}% CPU, {} workers, cap 100%", 
             config.num_tasks, 
             (1.0 - config.cpu_ratio) * 100.0,
             config.cpu_ratio * 100.0,
             config.num_workers);
    println!("Policy: {}", policy_name);
    
    let mut rng = StdRng::seed_from_u64(42);
    let tasks = generate_tasks(&config, &mut rng);
    
    let metrics = Arc::new(Metrics::new());
    let shutdown = Arc::new(AtomicBool::new(false));
    let dispatcher = Arc::new(Dispatcher::new_with_monitor(
        config.num_workers,
        metrics.clone(),
        shutdown.clone(),
        policy,
    ));
    
    let dispatcher_clone = dispatcher.clone();
    let generator_handle = thread::spawn(move || {
        for task in tasks {
            let now = Instant::now();
            if now < task.created_at {
                thread::sleep(task.created_at - now);
            }
            dispatcher_clone.submit_task(task);
        }
    });
    
    let dispatcher_handle = thread::spawn(move || {
        dispatcher.run();
    });
    
    generator_handle.join().unwrap();
    
    // Wait for tasks to complete
    while metrics.get_completed_count() < config.num_tasks {
        thread::sleep(Duration::from_millis(100));
    }
    
    shutdown.store(true, Ordering::Relaxed);
    dispatcher_handle.join().unwrap();
    
    // Print results
    print_results(&metrics, config.num_tasks, policy_name, suffix)?;
    
    Ok(())
}

fn print_results(metrics: &Metrics, total_tasks: usize, policy_name: &str, suffix: &str) -> anyhow::Result<()> {
    let makespan = metrics.get_makespan();
    let completed = metrics.get_completed_count();
    let avg_wait = metrics.get_avg_wait_time();
    let avg_wait_io = metrics.get_avg_wait_time_io();
    let avg_wait_cpu = metrics.get_avg_wait_time_cpu();
    let avg_turnaround = metrics.get_avg_turnaround_time();
    let max_wait = metrics.get_max_wait_time();
    let avg_cpu_usage = metrics.get_avg_cpu_usage();
    let avg_workers_active = metrics.get_avg_workers_active();
    let monitor_samples = metrics.get_monitor_sample_count();
    
    println!("\n{}", "=".repeat(55));
    println!("results");
    println!("{}", "=".repeat(55));
    println!("policy:               {}", policy_name);
    println!("total runtime:        {:.2?}", makespan);
    println!("makespan:             {:.2?}", makespan);
    println!("tasks completed:      {}/{}", completed, total_tasks);
    println!("avg wait time:        {:.2?}", avg_wait);
    println!("avg wait (IO only):   {:.2?}", avg_wait_io);
    println!("avg wait (CPU only):  {:.2?}", avg_wait_cpu);
    println!("avg turnaround time:  {:.2?}", avg_turnaround);
    println!("max wait time:        {:.2?}", max_wait);
    println!("avg CPU usage:        {:.1}%", avg_cpu_usage);
    println!("avg workers active:   {:.2}", avg_workers_active);
    println!("monitor samples:      {}", monitor_samples);
    
    // Save CSV with policy suffix
    let csv_file = format!("monitor_{}_{}.csv", suffix, chrono::Local::now().format("%Y%m%d_%H%M%S"));
    metrics.save_monitor_csv(&csv_file)?;
    println!("monitor csv:          {}", csv_file);
    
    Ok(())
}