//! # The Devils
//!
//! Work in progress thread-based [structured
//! concurrency](https://en.wikipedia.org/wiki/Structured_concurrency) library
//! inspired by Dostoevsky.
//!
//! ## Problem Space
//!
//! In my experience, getting concurrent software right is devilishly hard. The
//! frustration stems from the fact that it's relatively easy to come up with a
//! solution that works in a steady state, but it's hard to ensure that the
//! solution handles all the edge cases correctly. Some typical problems I
//! observed in real-world applications:
//!
//! * Broken shutdown. Many applications include dedicated code to handle
//!   shutdown in every place where concurrency happens. This code is rarely
//!   tested and often broken.
//! * Abruptly killed threads. Rust program exits when the main thread exits.
//!   All other currently running threads are terminated forcefully, without
//!   running destructors. This might lead to application's logical invariants
//!   being broken (ie, buffered readers not flushed to disk).
//! * Concurrency leaks. When running tests, it often happens that one tests
//!   schedules a piece of concurrent work which outlives the test itself and
//!   interferers with other tests.
//! * Loss of causality. Sometimes, and especially in tests, software has loops,
//!   which wait for a condition to become true with some sleep in between.
//!
//! This library tries to encode a particular pattern which eliminates these
//! problems by construction. As such, be warned -- this library doesn't _do_
//! anything useful (besides a particular implementation of spsc channels, which
//! is a private implementation detail), and is just a helper for structuring
//! code.
//!
//! ## Handles
//!
//! Handles are the core concept of this library. A [`Handle`] owns a unit of
//! concurrent work: a dedicated thread or a task running on a thread pool
//! (called a "devil", or, in Russian, "бес"). The main invariant is that this
//! concurrent work continues only as long as the corresponding `Handle` is
//! alive. In other words, handles *block* in [`Drop`], waiting for the
//! corresponding work to complete.
//!
//! ## Cancellation, Input & Output
//!
//! It's possible to signal a running devil. The simplest signal is cancellation
//! -- a note that the work is no longer interesting and should be wrapped up.
//!
//! A closure which is used to spawn a devil gets a [`Receiver<T>`] channel. The
//! [`Receiver::recv`] blocks and returns the next message or, if cancellation
//! was requested,  [`None`].
//!
//! The corresponding `Handle` has a [`Handle::send`] method for sending
//! messages to the devil. If you only interested in cancellation, use [`Void`]
//! as a message type. [`Handle::stop`] method requests cancellation. It is
//! automatically called on `Drop`. The `stop` method doesn't block -- drop or
//! join the `Handle` to wait for stopped devil to finish.
//!
//! A devil can produce a single value ([`Handle`]) or a sequence of values
//! ([`StreamHandle`]). You can receive the output using [`Handle::join`] or
//! [`StreamHandle::recv`] methods.
//!
//! ## Spawning
//!
//! The library defines concurrent work, but is decoupled from its execution.
//! That is, you can run the devils on a thread pool, but you'll need to bring
//! your own thread pool. [`spawn`] and [`spawn_stream`] return a pair of a
//! handle and a [`Devil`]. The `Devil` has only a single method,
//! [`Devil::run`], which you use to start the work. A typical usage looks like
//! this:
//!
//! ```
//! use devils::{Void, Handle};
//! # struct Pool; impl Pool { fn submit<T>(&self, _: T) {} } let pool = Pool;
//! let (devil, handle) = Handle::spawn(|_: &mut devils::Receiver<Void>| 92);
//! pool.submit(devil);
//! let res = handle.join();
//! ```
//!
//! As a convenience, [`spawn_new_thread`] and [`spawn_stream_new_thread`]
//! shortcut functions exist to spawn a devil onto a dedicated thread.
//!
//! ## Tree Of Ownership
//!
//! At runtime, devils form a tree, with a parent devil owning the handles for
//! children. This tree has ownership semantics -- children do not outlive the
//! parents. Communication is also restricted to the ownership tree -- a devil
//! can `send` a message or `stop` one of the handles it owns, and only the
//! devil's parent can `recv` the messages it outputs.
//!
//! Panics are also propagated along the tree. If a child panics, the parent
//! will panic as well the next time it tries to interact with it, propagating
//! the panic's payload. If several children panic concurrently, only the first
//! payload will be propagated. The `Drop` impl always waits for the children,
//! but it is careful to not cause a double panic.
//!
//! ## Select
//!
//! The [`Selector`] struct provides the API for waiting for one of the several
//! events. It's API is similar to that of `poll`. [`Selector::add`] method adds
//! an event to watch, together with associated user-provided key.
//! [(`Selector::wait`)] method returns the key for the event which is ready.
//! One of the `_now` functions is then used to complete the operation. Example:
//!
//! ```
//! use devils::Void;
//!
//! devils::spawn_new_thread(|receiver: &mut devils::Receiver<i32>| {
//!     # let spawn_workers = || vec![];
//!     let mut workers: Vec<devils::Handle<Void, i32>> = spawn_workers();
//!
//!     enum Key { Receiver, Worker(usize) }
//!
//!     let key = {
//!         let mut sel = devils::Selector::new();
//!         sel.add(Key::Receiver, receiver.event());
//!         for (idx, w) in workers.iter_mut().enumerate() {
//!             sel.add(Key::Worker(idx), w.event());
//!         }
//!         sel.wait()
//!     };
//!
//!     match key {
//!         Key::Receiver => {
//!             match receiver.recv_now() {
//!                 Some(number) => println!("received {}", number),
//!                 None => println!("stopped"),
//!             }
//!         }
//!         Key::Worker(idx) => {
//!             let w = workers.swap_remove(idx);
//!             match w.join_now() {
//!                 Some(number) => println!("worker compted {}", number),
//!                 None => println!("worker stopped"),
//!             }
//!         }
//!     }
//! });

use std::{
    collections::VecDeque,
    mem, panic,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};

pub type Void = std::convert::Infallible;

pub struct Handle<I, T> {
    sender: Option<Arc<Chan<I, Void>>>,
    receiver: Arc<Chan<Void, thread::Result<T>>>,
}

pub struct StreamHandle<I, T> {
    sender: Option<Arc<Chan<I, Void>>>,
    receiver: Arc<Chan<T, thread::Result<()>>>,
}

impl<I, T> Handle<I, T> {
    pub fn spawn<F>(f: F) -> (impl FnOnce() + Send + 'static, Handle<I, T>)
    where
        I: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut Receiver<I>) -> T + Send + 'static,
    {
        let mailbox = Chan::<I, Void>::new();
        let mailbox = Arc::new(mailbox);

        let res = Chan::<Void, thread::Result<T>>::new();
        let res = Arc::new(res);

        let f = {
            let mut receiver = Receiver { chan: Arc::clone(&mailbox) };
            let res = Arc::clone(&res);
            let defer = defer({
                let res = Arc::clone(&res);
                move || res.close_with(None)
            });
            move || {
                let _d = defer;
                let f = panic::AssertUnwindSafe(f);
                let r = panic::catch_unwind(move || f.0(&mut receiver));
                res.close_with(Some(r))
            }
        };
        (f, Handle { sender: Some(mailbox), receiver: res })
    }
    pub fn start_thread<F>(f: F) -> Handle<I, T>
    where
        I: Send + 'static,
        T: Send + 'static,
        F: FnOnce(&mut Receiver<I>) -> T + Send + 'static,
    {
        let (r, h) = Self::spawn(f);
        std::thread::spawn(r);
        h
    }

    pub fn send(&mut self, value: I) {
        match &mut self.sender {
            Some(it) => match it.send(value) {
                Ok(()) => (),
                Err(_) => unreachable!(),
            },
            None => panic!("the handle is stopped"),
        }
    }

    pub fn event(&self) -> Event<'_> {
        self.receiver.event()
    }
    pub fn join(mut self) -> Option<T> {
        self.stop();
        devoid(self.receiver.recv()).map(propagate)
    }
    pub fn join_now(mut self) -> Option<T> {
        self.stop();
        match self.receiver.recv_now().map(devoid) {
            Ok(it) => it.map(propagate),
            Err(()) => panic!("join_now not ready"),
        }
    }
    pub fn stop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.close_with(None)
        }
    }
}

impl<I, T> Drop for Handle<I, T> {
    fn drop(&mut self) {
        self.stop();
        let res = devoid(self.receiver.recv());
        if !thread::panicking() {
            res.map(propagate);
        }
    }
}

impl<I, T> StreamHandle<I, T> {
    pub fn send(&mut self, value: I) {
        match &mut self.sender {
            Some(it) => match it.send(value) {
                Ok(()) => (),
                Err(_) => unreachable!(),
            },
            None => panic!("the handle is stopped"),
        }
    }
    pub fn event(&mut self) -> Event<'_> {
        self.receiver.event()
    }
    pub fn recv(&mut self) -> Option<T> {
        match self.receiver.recv() {
            Ok(it) => Some(it),
            Err(err) => {
                err.map(propagate);
                None
            }
        }
    }
    pub fn recv_now(&mut self) -> Option<T> {
        match self.receiver.recv_now() {
            Ok(Ok(it)) => Some(it),
            Ok(Err(err)) => {
                err.map(propagate);
                None
            }
            Err(()) => panic!("recv_now not ready"),
        }
    }
    pub fn stop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.close_with(None)
        }
    }
}

impl<I, T> Drop for StreamHandle<I, T> {
    fn drop(&mut self) {
        self.stop();
        loop {
            match self.receiver.recv() {
                Ok(_) => (),
                Err(err) => {
                    if !thread::panicking() {
                        err.map(propagate);
                    }
                    break;
                }
            }
        }
        while let Ok(_) = self.receiver.recv() {}
    }
}

pub struct Sender<I> {
    chan: Arc<Chan<I, thread::Result<()>>>,
}

impl<I> Sender<I> {
    pub fn send(&mut self, item: I) {
        match self.chan.send(item) {
            Ok(()) => (),
            Err(_item) => panic!("channel is closed"),
        }
    }
}

pub struct Receiver<I> {
    chan: Arc<Chan<I, Void>>,
}

impl<I> Receiver<I> {
    pub fn recv(&mut self) -> Option<I> {
        self.chan.recv().ok()
    }
    pub fn event(&mut self) -> Event<'_> {
        Event { select: Box::new(ChanSelect { chan: &*self.chan, guard: None }) }
    }
    pub fn recv_now(&mut self) -> Option<I> {
        match self.chan.recv_now() {
            Ok(it) => it.ok(),
            Err(()) => panic!("receiver is not ready"),
        }
    }
    pub fn on_close<F: FnOnce() + Sync>(&mut self, _cb: F) {
        todo!()
    }
}

impl<I> Iterator for Receiver<I> {
    type Item = I;

    fn next(&mut self) -> Option<I> {
        self.recv()
    }
}

pub struct Devil {
    f: Box<dyn FnOnce() + Send + 'static>,
}

impl Devil {
    pub fn run(self) {
        (self.f)()
    }
}

pub fn spawn_new_thread<I, T, F>(f: F) -> Handle<I, T>
where
    I: Send + 'static,
    T: Send + 'static,
    F: FnOnce(&mut Receiver<I>) -> T + Send + 'static,
{
    let (r, handle) = Handle::<I, T>::spawn(f);
    let _ = thread::spawn(r);
    handle
}

pub fn spawn_stream<I, T, F>(f: F) -> (StreamHandle<I, T>, Devil)
where
    I: Send + 'static,
    T: Send + 'static,
    F: FnOnce(&mut Receiver<I>, &mut Sender<T>) + Send + 'static,
{
    let mailbox = Chan::<I, Void>::new();
    let mailbox = Arc::new(mailbox);

    let res = Chan::<T, thread::Result<()>>::new();
    let res = Arc::new(res);

    let f = Box::new({
        let mut receiver = Receiver { chan: Arc::clone(&mailbox) };
        let res = Arc::clone(&res);
        let defer = defer({
            let res = Arc::clone(&res);
            move || res.close_with(None)
        });
        move || {
            let _d = defer;
            let f = panic::AssertUnwindSafe(f);
            let mut sender = Sender { chan: Arc::clone(&res) };
            let r = panic::catch_unwind(move || f.0(&mut receiver, &mut sender));
            res.close_with(Some(r))
        }
    });

    (StreamHandle { sender: Some(mailbox), receiver: res }, Devil { f })
}

pub fn spawn_stream_new_thread<I, T, F>(f: F) -> StreamHandle<I, T>
where
    I: Send + 'static,
    T: Send + 'static,
    F: FnOnce(&mut Receiver<I>, &mut Sender<T>) + Send + 'static,
{
    let (handle, devil) = spawn_stream(f);
    let _ = thread::spawn(move || devil.run());
    handle
}

pub struct Event<'a> {
    select: Box<dyn Select<'a> + 'a>,
}

pub struct Selector<'a, K> {
    events: Vec<(K, Event<'a>)>,
}

impl<'a, K> Selector<'a, K> {
    pub fn new() -> Selector<'a, K> {
        Selector { events: Vec::new() }
    }
    pub fn add(&mut self, key: K, event: Event<'a>) {
        self.events.push((key, event))
    }
    pub fn wait(mut self) -> K {
        if self.events.is_empty() {
            panic!("selecting among empty set of events")
        }
        loop {
            let mut idx = 0;
            for (_key, event) in &mut self.events {
                if !event.select.lock() {
                    break;
                }
                idx += 1;
            }

            if idx != self.events.len() {
                for (_key, event) in self.events[..idx].iter_mut().rev() {
                    event.select.unlock(true);
                }
                let (key, _event) = self.events.swap_remove(idx);
                return key;
            }

            for (_key, event) in self.events.iter_mut().rev() {
                event.select.unlock(false)
            }
            thread::park();
        }
    }
}

// region:implementation

fn propagate<T>(res: thread::Result<T>) -> T {
    res.unwrap_or_else(|a| panic::resume_unwind(a))
}

fn devoid<T>(r: Result<Void, T>) -> T {
    match r {
        Ok(void) => match void {},
        Err(it) => it,
    }
}

struct Chan<T, C> {
    state: Mutex<ChanState<T, C>>,
}

enum ChanState<T, C> {
    Closed {
        value: Option<C>,
        messages: VecDeque<T>,
    },
    Open {
        messages: VecDeque<T>,
        /// Invariant: !messages.is_empty() => reader.is_none()
        reader: Option<thread::Thread>,
    },
}

impl<T, C> Chan<T, C> {
    fn new() -> Chan<T, C> {
        let state = Mutex::new(ChanState::Open { messages: VecDeque::new(), reader: None });
        Chan { state }
    }
    fn send(&self, value: T) -> Result<(), T> {
        let mut guard = self.state.lock().unwrap();
        match &mut *guard {
            ChanState::Closed { .. } => Err(value),
            ChanState::Open { messages, reader } => {
                messages.push_back(value);
                if let Some(t) = reader.take() {
                    t.unpark()
                }
                Ok(())
            }
        }
    }
    fn recv(&self) -> Result<T, Option<C>> {
        loop {
            let mut guard = self.state.lock().unwrap();
            match &mut *guard {
                ChanState::Closed { messages, value } => {
                    return messages.pop_front().ok_or_else(|| value.take())
                }
                ChanState::Open { messages, reader } => match messages.pop_front() {
                    Some(it) => return Ok(it),
                    None => {
                        let me = thread::current();
                        match reader.take() {
                            Some(prev) => assert_eq!(prev.id(), me.id()),
                            None => (),
                        }
                        *reader = Some(me)
                    }
                },
            }
            drop(guard);
            thread::park()
        }
    }
    fn event(&self) -> Event<'_> {
        Event { select: Box::new(ChanSelect { chan: self, guard: None }) }
    }
    fn recv_now(&self) -> Result<Result<T, Option<C>>, ()> {
        let mut guard = self.state.lock().unwrap();
        match &mut *guard {
            ChanState::Closed { messages, value } => {
                Ok(messages.pop_front().ok_or_else(|| value.take()))
            }
            ChanState::Open { messages, reader: _ } => match messages.pop_front() {
                Some(it) => Ok(Ok(it)),
                None => Err(()),
            },
        }
    }
    fn close_with(&self, value: Option<C>) {
        let mut guard = self.state.lock().unwrap();
        match &mut *guard {
            ChanState::Closed { .. } => (),
            ChanState::Open { messages, reader } => {
                if let Some(t) = reader.take() {
                    t.unpark();
                }
                let messages = mem::take(messages);
                *guard = ChanState::Closed { messages, value };
            }
        }
    }
}

trait Select<'a> {
    fn lock(&mut self) -> bool;
    fn unlock(&mut self, unwatch: bool);
}

struct ChanSelect<'a, T, C> {
    chan: &'a Chan<T, C>,
    guard: Option<MutexGuard<'a, ChanState<T, C>>>,
}

impl<'a, T, C> Select<'a> for ChanSelect<'a, T, C> {
    fn lock(&mut self) -> bool {
        let mut guard = self.chan.state.lock().unwrap();
        match &mut *guard {
            ChanState::Closed { .. } => return false,
            ChanState::Open { messages, reader } => {
                if !messages.is_empty() {
                    return false;
                }
                let me = thread::current();
                match reader.take() {
                    Some(prev) => assert_eq!(prev.id(), me.id()),
                    None => (),
                }
                *reader = Some(me);
            }
        }
        let prev = self.guard.replace(guard);
        assert!(prev.is_none());
        true
    }

    fn unlock(&mut self, unwatch: bool) {
        match self.guard.take() {
            Some(mut guard) => match &mut *guard {
                ChanState::Closed { .. } => unreachable!(),
                ChanState::Open { messages: _, reader } => {
                    assert!(reader.is_some());
                    if unwatch {
                        *reader = None
                    }
                }
            },
            None => unreachable!(),
        }
    }
}

trait AnyChan {
    fn recv(&self) -> Option<()>;
    fn recv_now(&self) -> Result<Option<()>, ()>;
    fn event(&self) -> Event<'_>;
}

impl<T> AnyChan for Chan<T, Void> {
    fn recv(&self) -> Option<()> {
        self.recv().ok().map(|_| ())
    }
    fn recv_now(&self) -> Result<Option<()>, ()> {
        self.recv_now().map(|r| r.ok().map(|_| ()))
    }
    fn event(&self) -> Event<'_> {
        self.event()
    }
}

fn defer<F: FnOnce()>(f: F) -> impl Drop {
    struct D<F: FnOnce()>(Option<F>);
    impl<F: FnOnce()> Drop for D<F> {
        fn drop(&mut self) {
            if let Some(f) = self.0.take() {
                f()
            }
        }
    }
    D(Some(f))
}
// endregion:implementation
