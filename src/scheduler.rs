use tokio::runtime;
use tokio::time::sleep;
use tokio::task::JoinHandle;

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
