use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicUsize, Ordering},
    }, fmt::Debug,
};

pub(crate) struct TrySendError<T>(pub(crate) T);

impl<T> Debug for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TrySendError").finish()
    }
}

#[derive(Debug)]
pub(crate) struct TryRecvError;

pub(crate) struct Common<T, const SIZE: usize> {
    /// An array of items.
    items: [MaybeUninit<UnsafeCell<T>>; SIZE],
    /// An index of the next slot which will be consumed by receivers.
    head: AtomicUsize,
    /// An index of the next available slot for the sender to fill.
    tail: AtomicUsize,
}

impl<T, const SIZE: usize> Common<T, SIZE> {
    pub fn new() -> Self {
        // `MaybeUnint` do not require initialization.
        let items = unsafe { MaybeUninit::uninit().assume_init() };

        Common {
            items,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    pub fn try_send(&self, x: T) -> Result<(), TrySendError<T>> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        if head + SIZE == tail {
            return Err(TrySendError(x));
        }
        unsafe {
            let slot = self.items.get_unchecked(tail % SIZE);
            UnsafeCell::raw_get(slot.as_ptr()).write(x);
        }
        // A write to slot must be visible before updating the tail index.
        self.tail.store(tail + 1, Ordering::Release);

        Ok(())
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut head = self.head.load(Ordering::Relaxed);

        loop {
            let tail = self.tail.load(Ordering::Relaxed);
            if head == tail {
                return Err(TryRecvError);
            }

            let success =
                self.head
                    .compare_exchange_weak(head, head + 1, Ordering::SeqCst, Ordering::SeqCst);
            match success {
                Ok(head) => unsafe {
                    let m = self.items.get_unchecked(head % SIZE);
                    let item = m.assume_init_read().into_inner();
                    return Ok(item);
                },
                Err(new_head) => head = new_head,
            }
        }
    }
}

impl<T, const SIZE: usize> Drop for Common<T, SIZE> {
    fn drop(&mut self) {
        loop {
            let mut head = self.head.load(Ordering::SeqCst);
            let tail = self.tail.load(Ordering::SeqCst);
            if head == tail {
                break;
            }
            while head < tail {
                unsafe {
                    let slot = self.items.get_unchecked(head % SIZE);
                    std::ptr::drop_in_place(UnsafeCell::get(slot.assume_init_ref()));
                }
                head += 1;
            }
            self.head.store(head, Ordering::SeqCst);
        }
    }
}
