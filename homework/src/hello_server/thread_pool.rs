//! Thread pool that joins all thread when dropped.

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use crossbeam_channel::{Receiver, Sender, unbounded};

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    _id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.
    ///
    /// NOTE: The thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        self.thread.take().unwrap().join().unwrap()
    }
}

/// Internal data structure for tracking the current job status. This is shared by worker closures
/// via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Mutex<usize>,
    empty_condvar: Condvar,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        let mut guard = self.job_count.lock().expect("start_job lock error");
        *guard += 1;
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        let mut guard = self.job_count.lock().expect("finish job lock error");
        *guard -= 1;
        self.empty_condvar.notify_all();
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        let mut guard = self.job_count.lock().unwrap();
        while *guard != 0 {
            guard = self.empty_condvar.wait(guard).unwrap();
        }
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    _workers: Vec<Worker>,
    job_sender: Option<Sender<Job>>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads.
    ///
    /// # Panics
    ///
    /// Panics if `size` is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (job_sender, job_receiver) = unbounded();
        let pool_inner = Arc::new(ThreadPoolInner::default());

        let mut _workers = Vec::with_capacity(size);
        for id in 0..size {
            let job_receiver: Receiver<Job> = job_receiver.clone();
            let pool_inner = pool_inner.clone();

            let thread = thread::spawn(move || {
                while let Ok(job) = job_receiver.recv() {
                    pool_inner.start_job();
                    job.0();
                    pool_inner.finish_job();
                }
            });
            _workers.push(Worker {
                _id: id,
                thread: Some(thread),
            });
        }

        Self {
            _workers,
            job_sender: Some(job_sender),
            pool_inner,
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.job_sender
            .as_ref()
            .unwrap()
            .send(Job(Box::new(f)))
            .unwrap();
    }

    /// Block the current thread until all jobs in the pool have been executed.
    ///
    /// NOTE: This method has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        self.pool_inner.wait_empty();
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        drop(self.job_sender.take());
    }
}
