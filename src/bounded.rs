//! Bounded single-producer multiple-consumer queue.
//!
//!

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{TryRecvError, TrySendError},
    Arc,
};

use crate::common::Common;

unsafe impl<T, const SIZE: usize> Send for Sender<T, SIZE> {}
unsafe impl<T, const SIZE: usize> Send for Receiver<T, SIZE> {}

pub struct Sender<T, const SIZE: usize> {
    inner: Arc<Inner<T, SIZE>>,
}

pub struct Receiver<T, const SIZE: usize> {
    inner: Arc<Inner<T, SIZE>>,
}

struct Inner<T, const SIZE: usize> {
    pub(crate) common: Common<T, SIZE>,
    recv_count: AtomicUsize,
    is_sender_alive: AtomicBool,
}

pub fn channel<T, const SIZE: usize>() -> (Sender<T, SIZE>, Receiver<T, SIZE>) {
    let inner = Arc::new(Inner {
        common: Common::new(),
        recv_count: AtomicUsize::new(1),
        is_sender_alive: AtomicBool::new(true),
    });

    let sender = Sender {
        inner: Arc::clone(&inner),
    };
    let receiver = Receiver { inner };
    (sender, receiver)
}

impl<T, const SIZE: usize> Sender<T, SIZE> {
    /// Sends a value to this queue.
    ///
    /// Note that a successful `send` does not guarantee that there exists a receiver that
    /// observed the sent value.
    pub fn try_send(&self, x: T) -> Result<(), TrySendError<T>> {
        if self.inner.recv_count.load(Ordering::Relaxed) == 0 {
            return Err(TrySendError::Disconnected(x));
        }
        self.inner
            .common
            .try_send(x)
            .map_err(|e| TrySendError::Full(e.0))
    }
}

impl<T, const SIZE: usize> Receiver<T, SIZE> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.inner.common.try_recv() {
            Ok(x) => Ok(x),
            Err(_) => {
                if self.inner.is_sender_alive.load(Ordering::Relaxed) {
                    Err(TryRecvError::Empty)
                } else {
                    Err(TryRecvError::Disconnected)
                }
            }
        }
    }
}

impl<T, const SIZE: usize> Clone for Receiver<T, SIZE> {
    fn clone(&self) -> Self {
        self.inner.recv_count.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, const SIZE: usize> Drop for Receiver<T, SIZE> {
    fn drop(&mut self) {
        self.inner.recv_count.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T, const SIZE: usize> Drop for Sender<T, SIZE> {
    fn drop(&mut self) {
        self.inner.is_sender_alive.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use super::*;

    struct DropCount<'a> {
        counter: &'a AtomicUsize,
    }

    impl<'a> Drop for DropCount<'a> {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_spmc_single() {
        const SIZE: usize = 64;
        let (s, r) = channel::<usize, SIZE>();
        assert!(s.try_send(1usize).is_ok());
        assert_eq!(r.try_recv(), Ok(1usize));
        assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
        for x in 0..SIZE {
            assert!(s.try_send(x).is_ok());
        }
        assert!(s.try_send(usize::MAX).is_err());
        for x in 0..SIZE {
            assert_eq!(r.try_recv(), Ok(x));
        }
        assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn test_spmc_multi() {
        const SIZE: usize = 1024;
        let thread_num: usize = num_cpus::get();
        let drop_count: usize = SIZE * thread_num * 100;
        let flag = Arc::new(AtomicBool::new(true));
        let counter = AtomicUsize::new(0);

        let recv_count = thread::scope(|s| {
            let (sender, r) = channel::<DropCount, SIZE>();

            let mut ts = vec![];
            for _ in 0..thread_num {
                let flag = Arc::clone(&flag);
                let r = r.clone();
                ts.push(s.spawn(move || {
                    let mut i = 0usize;
                    while flag.load(Ordering::Relaxed) {
                        loop {
                            match r.try_recv() {
                                Ok(_d) => i += 1,
                                Err(TryRecvError::Empty) => break,
                                Err(_e) => std::thread::yield_now(),
                            }
                        }
                    }
                    i
                }));
            }

            let mut i = 0;
            while i < drop_count {
                match sender.try_send(DropCount { counter: &counter }) {
                    Ok(()) => {
                        i += 1;
                        continue;
                    }
                    Err(TrySendError::Disconnected(_)) => unreachable!("disconnected"),
                    Err(TrySendError::Full(dc)) => std::mem::forget(dc), // Avoid calling `drop` more than once per each iteration
                };
            }
            flag.store(false, Ordering::SeqCst);

            let mut recv_count = 0;
            for t in ts {
                recv_count += t.join().unwrap();
            }
            recv_count
        });

        assert_eq!(recv_count, drop_count);
        assert_eq!(counter.load(Ordering::SeqCst), drop_count);
    }

    #[test]
    fn drop_by_drop() {
        const SIZE: usize = 1024;
        const THREAD_NUM: usize = 128;
        let counter = AtomicUsize::new(0);

        thread::scope(|s| {
            let (sender, r) = channel::<DropCount, SIZE>();

            for _ in 0..SIZE {
                assert!(sender.try_send(DropCount { counter: &counter }).is_ok());
            }

            let mut ts = vec![];
            for _ in 0..THREAD_NUM {
                let r = r.clone();
                ts.push(s.spawn(move || {
                    assert!(r.try_recv().is_ok());
                }));
            }
            for t in ts {
                t.join().unwrap();
            }
        });

        assert_eq!(counter.load(Ordering::SeqCst), SIZE);
    }

    #[test]
    fn test_error() {
        {
            let (s, r) = channel::<usize, 1>();
            assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
            drop(s);
            assert_eq!(r.try_recv(), Err(TryRecvError::Disconnected));
        }
        {
            let (s, r) = channel::<usize, 1>();
            drop(r);
            assert_eq!(s.try_send(0), Err(TrySendError::Disconnected(0)));
        }
    }
}
