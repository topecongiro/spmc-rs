//! Unbound single-producer multiple-consumer queue.

use std::{
    ptr::NonNull,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{RecvError, SendError, TryRecvError},
        Arc,
    },
};

use parking_lot::{RwLock, RwLockUpgradableReadGuard};

use crate::common::{Common, TrySendError};

pub struct Sender<T, const SIZE: usize> {
    inner: Arc<Inner<T, SIZE>>,
}

pub struct Receiver<T, const SIZE: usize> {
    inner: Arc<Inner<T, SIZE>>,
}

unsafe impl<T, const SIZE: usize> Send for Receiver<T, SIZE> {}
unsafe impl<T, const SIZE: usize> Send for Sender<T, SIZE> {}

struct Inner<T, const SIZE: usize> {
    list: RwLock<List<T, SIZE>>,
    /// A number of receivers associated to this queue.
    recv_count: AtomicUsize,
    /// A flag indicating whether there exists a live sender.
    is_sender_alive: AtomicBool,
    /// An approximate number of elements in this queue.
    len: AtomicUsize,
}

struct List<T, const SIZE: usize> {
    id_gen: u64,
    head: NonNull<Node<T, SIZE>>,
    tail: NonNull<Node<T, SIZE>>,
}

struct Node<T, const SIZE: usize> {
    id: u64,
    common: Common<T, SIZE>,
    next: Option<NonNull<Node<T, SIZE>>>,
}

pub fn channel<T, const SIZE: usize>() -> (Sender<T, SIZE>, Receiver<T, SIZE>) {
    let head_tail = Box::into_raw(Box::new(Node::new(0, None)));
    let inner = Arc::new(Inner {
        list: RwLock::new(List {
            id_gen: 2,
            head: unsafe { NonNull::new_unchecked(head_tail) },
            tail: unsafe { NonNull::new_unchecked(head_tail) },
        }),
        recv_count: AtomicUsize::new(1),
        is_sender_alive: AtomicBool::new(true),
        len: AtomicUsize::new(0),
    });
    let sender = Sender {
        inner: Arc::clone(&inner),
    };
    let receiver = Receiver { inner };
    (sender, receiver)
}

impl<T, const SIZE: usize> Receiver<T, SIZE> {
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            match self.try_recv() {
                Ok(x) => return Ok(x),
                Err(TryRecvError::Disconnected) => return Err(RecvError),
                Err(TryRecvError::Empty) => {
                    // FIXME: try other blocking strategy
                    // 1. condvar
                    // 2. sleep (w/ exponential backoff)
                    std::thread::yield_now();
                }
            }
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.try_recv_inner() {
            ok @ Ok(_) => {
                self.inner.len.fetch_sub(1, Ordering::Relaxed);
                ok
            }
            Err(_) => {
                if self.inner.is_sender_alive.load(Ordering::Relaxed) {
                    Err(TryRecvError::Empty)
                } else {
                    Err(TryRecvError::Disconnected)
                }
            }
        }
    }

    fn try_recv_inner(&self) -> Result<T, TryRecvError> {
        let list = self.inner.list.read();

        let tail = unsafe { list.tail.as_ref() };
        let mut head = unsafe { list.head.as_ref() };
        match head.common.try_recv() {
            Ok(x) => Ok(x),
            Err(_) => {
                while head.id != tail.id && self.inner.len.load(Ordering::Relaxed) > 0 {
                    match head.next {
                        None => return Err(TryRecvError::Empty),
                        Some(next) => {
                            let next = unsafe { next.as_ref() };
                            match next.common.try_recv() {
                                Ok(x) => {
                                    // FIXME: update head.
                                    return Ok(x);
                                }
                                Err(_) => head = next,
                            }
                        }
                    }
                }
                Err(TryRecvError::Empty)
            }
        }
    }
}

impl<T, const SIZE: usize> Sender<T, SIZE> {
    pub fn send(&self, x: T) -> Result<(), SendError<T>> {
        let result = self.send_inner(x);
        if result.is_ok() {
            self.inner.len.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn send_inner(&self, x: T) -> Result<(), SendError<T>> {
        if self.inner.recv_count.load(Ordering::Relaxed) == 0 {
            return Err(SendError(x));
        }

        let list = self.inner.list.upgradable_read();
        let tail = unsafe { list.tail.as_ref() };
        let head = unsafe { list.head.as_ref() };

        match tail.common.try_send(x) {
            Ok(()) => Ok(()),
            Err(TrySendError(x)) => {
                // The tail node is full; see if we have an empty space in the next node.
                let mut list = RwLockUpgradableReadGuard::upgrade(list);

                let x = {
                    match tail.next {
                        Some(next) if unsafe { next.as_ref().id == head.id } => x,
                        Some(next) => match unsafe { next.as_ref().common.try_send(x) } {
                            Ok(()) => {
                                list.tail = next;
                                return Ok(());
                            }
                            Err(TrySendError(x)) => x,
                        },
                        None => x,
                    }
                };

                // No empty space; insert a new node.
                list.id_gen += 1;
                let mut new_tail = unsafe {
                    NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                        id: list.id_gen,
                        common: Common::new(),
                        next: Some(match tail.next {
                            None => list.tail,
                            Some(n) => n,
                        }),
                    })))
                };

                unsafe {
                    list.tail.as_mut().next = Some(new_tail);
                    new_tail.as_mut().common.try_send(x).unwrap(); // Must succeed.
                }

                list.tail = new_tail;

                Ok(())
            }
        }
    }
}

impl<T, const SIZE: usize> Node<T, SIZE> {
    fn new(id: u64, next: Option<NonNull<Node<T, SIZE>>>) -> Self {
        Node {
            id,
            common: Common::new(),
            next,
        }
    }
}

impl<T, const SIZE: usize> Drop for Sender<T, SIZE> {
    fn drop(&mut self) {
        self.inner.is_sender_alive.store(false, Ordering::SeqCst);
    }
}

impl<T, const SIZE: usize> Drop for Receiver<T, SIZE> {
    fn drop(&mut self) {
        self.inner.recv_count.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T, const SIZE: usize> Clone for Receiver<T, SIZE> {
    fn clone(&self) -> Self {
        self.inner.recv_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T, const SIZE: usize> Drop for Inner<T, SIZE> {
    fn drop(&mut self) {
        unsafe {
            let list = self.list.get_mut();
            let head = list.head.as_ref();

            if let Some(mut node) = head.next {
                while let Some(next) = node.as_ref().next {
                    if next.as_ref().id == head.id {
                        break;
                    }
                    drop(Box::from_raw(node.as_ptr()));
                    node = next;
                }
            }
            drop(Box::from_raw(list.head.as_ptr()));
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::atomic::AtomicUsize, thread};

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
    fn test_unbounded_single() {
        const SIZE: usize = 8;
        let (s, r) = channel::<usize, SIZE>();
        for _ in 0..10 {
            assert!(s.send(1usize).is_ok());
            assert_eq!(r.try_recv(), Ok(1usize));
            assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
            for x in 0..SIZE * 8 {
                assert!(s.send(x).is_ok());
            }
            for x in 0..SIZE * 8 {
                assert_eq!(r.try_recv(), Ok(x));
            }
            assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
        }
    }

    #[test]
    fn test_unbounded_spmc_multi() {
        const SIZE: usize = 64;
        let thread_count: usize = num_cpus::get();
        let drop_count: usize = SIZE * thread_count * 100;
        let counter = AtomicUsize::new(0);

        let recv_count = thread::scope(|s| {
            let (sender, r) = channel::<DropCount, SIZE>();

            let mut ts = vec![];
            for _ in 0..thread_count {
                let r = r.clone();
                ts.push(s.spawn(move || {
                    let mut i = 0usize;
                    loop {
                        match r.recv() {
                            Ok(_d) => i += 1,
                            Err(RecvError) => break,
                        }
                    }
                    i
                }));
            }

            let mut i = 0;
            while i < drop_count {
                match sender.send(DropCount { counter: &counter }) {
                    Ok(()) => {
                        i += 1;
                        continue;
                    }
                    Err(SendError(dc)) => std::mem::forget(dc), // Avoid calling `drop` more than once per each iteration
                };
            }

            drop(sender);
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
    fn test_unbounded_drop_by_drop() {
        const SIZE: usize = 1024;
        const THREAD_NUM: usize = 128;
        let counter = AtomicUsize::new(0);

        thread::scope(|s| {
            let (sender, r) = channel::<DropCount, SIZE>();

            for _ in 0..SIZE {
                assert!(sender.send(DropCount { counter: &counter }).is_ok());
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
    fn test_unbounded_fifo() {
        const LOOP: usize = 1024 * 1024;
        let thread_count: usize = num_cpus::get();

        thread::scope(|s| {
            let (sender, r) = channel::<usize, 64>();

            let mut ts = vec![];
            for _ in 0..thread_count {
                let r = r.clone();
                ts.push(s.spawn(move || {
                    let mut prev = 0usize;
                    loop {
                        match r.recv() {
                            Ok(x) => {
                                assert!(prev < x, "{} < {}", prev, x);
                                prev = x;
                            }
                            Err(_) => break,
                        }
                    }
                }));
            }

            for i in 1..=LOOP {
                assert!(sender.send(i).is_ok());
            }

            drop(sender);

            for t in ts {
                t.join().unwrap();
            }
        });
    }

    #[test]
    fn test_unbounded_error() {
        {
            let (s, r) = channel::<usize, 1>();
            drop(r);
            assert_eq!(s.send(0), Err(SendError(0)));
        }
        {
            let (s, r) = channel::<usize, 1>();
            assert_eq!(r.try_recv(), Err(TryRecvError::Empty));
            drop(s);
            assert_eq!(r.try_recv(), Err(TryRecvError::Disconnected));
            assert_eq!(r.recv(), Err(RecvError));
        }
    }
}
