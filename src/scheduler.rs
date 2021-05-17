//! Internal module used to encapsulate the tokio_rs dependency.
//! This could be moved to its own crate.
use tokio::runtime;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub struct ScheduledExecutor {
    pub runtime: runtime::Runtime,
}

impl ScheduledExecutor {
    pub fn new() -> Self {
        let rt = runtime::Runtime::new().unwrap();

        ScheduledExecutor { runtime: rt }
    }

    pub fn submit_task<F>(&self, task: F, timeout: std::time::Duration) -> JoinHandle<()>
    where
        F: FnOnce() -> () + Send + 'static,
    {
        self.runtime.spawn(async move {
            sleep(timeout).await;
            task();
        })
    }
}
