use std::{
    collections::VecDeque,
    fmt, mem, panic,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};

pub type Void = std::convert::Infallible;

#[derive(Debug)]
pub struct WouldBlock;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Act<Y, C> {
    Yield(Y),
    Complete(C),
}

impl<C> Act<Void, C> {
    pub fn completed(self) -> C {
        match self {
            Act::Yield(void) => match void {},
            Act::Complete(it) => it,
        }
    }
}

pub struct Activity<I, Y, C> {
    sender: Option<Arc<Chan<I, Void>>>,
    receiver: Arc<Chan<Y, thread::Result<C>>>,
}

impl<I, Y, C> fmt::Debug for Activity<I, Y, C> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

pub struct Mailbox<I, Y, C> {
    sender: Arc<Chan<Y, thread::Result<C>>>,
    receiver: Arc<Chan<I, Void>>,
}

impl<I, Y, C> fmt::Debug for Mailbox<I, Y, C> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl<I, Y, C> Drop for Activity<I, Y, C> {
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
    }
}

impl<I, Y, C> Activity<I, Y, C>
where
    I: Send + 'static,
    Y: Send + 'static,
    C: Send + 'static,
{
    pub fn new(
        f: impl FnOnce(&mut Mailbox<I, Y, Option<C>>) -> C + Send + 'static,
    ) -> (impl FnOnce() + Send + 'static, Activity<I, Y, Option<C>>) {
        Activity::new_(|mailbox| Some(f(mailbox)), Option::default)
    }

    pub fn spawn(f: impl FnOnce(&mut Mailbox<I, Y, C>) -> C + Send + 'static) -> Self {
        let (f, h) = Self::new_(f, || unreachable!());
        std::thread::spawn(f);
        h
    }

    fn new_(
        f: impl FnOnce(&mut Mailbox<I, Y, C>) -> C + Send + 'static,
        cancelled: impl FnOnce() -> C + Send + 'static,
    ) -> (impl FnOnce() + Send + 'static, Self) {
        let ichan = Chan::<I, Void>::new();
        let ichan = Arc::new(ichan);

        let ychan = Chan::<Y, thread::Result<C>>::new();
        let ychan = Arc::new(ychan);

        let f = {
            let mut mailbox = Mailbox { sender: Arc::clone(&ychan), receiver: Arc::clone(&ichan) };
            let ychan = Arc::clone(&ychan);

            struct D<F: FnOnce()>(Option<F>);
            impl<F: FnOnce()> Drop for D<F> {
                fn drop(&mut self) {
                    if let Some(f) = self.0.take() {
                        f()
                    }
                }
            }
            let mut defer = D(Some({
                let ychan = Arc::clone(&ychan);
                move || ychan.close_with(Some(Ok(cancelled())))
            }));

            move || {
                let f = panic::AssertUnwindSafe(f);
                let r = panic::catch_unwind(move || f.0(&mut mailbox));
                ychan.close_with(Some(r));
                defer.0.take();
            }
        };

        (f, Activity { sender: Some(ichan), receiver: ychan })
    }
}

impl<I, Y, C> Activity<I, Y, C> {
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
    pub fn recv(&mut self) -> Act<Y, C> {
        match self.receiver.recv() {
            Ok(it) => Act::Yield(it),
            Err(Some(res)) => Act::Complete(propagate(res)),
            Err(None) => unreachable!(),
        }
    }
    pub fn recv_now(&mut self) -> Result<Act<Y, C>, WouldBlock> {
        let res = match self.receiver.recv_now().map_err(|()| WouldBlock)? {
            Ok(it) => Act::Yield(it),
            Err(Some(res)) => Act::Complete(propagate(res)),
            Err(None) => unreachable!(),
        };
        Ok(res)
    }
    pub fn stop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.close_with(None)
        }
    }
}

impl<I, Y, C> Mailbox<I, Y, C>
where
    I: Send + 'static,
    Y: Send + 'static,
    C: Send + 'static,
{
    pub fn send(&mut self, value: Y) {
        self.sender.send(value).unwrap_or_else(|_| unreachable!())
    }
    pub fn event(&mut self) -> Event<'_> {
        Event { select: Box::new(ChanSelect { chan: &*self.receiver, guard: None }) }
    }
    pub fn recv(&mut self) -> Option<I> {
        self.receiver.recv().ok()
    }
    pub fn recv_now(&mut self) -> Result<Option<I>, WouldBlock> {
        let res = self.receiver.recv_now().map_err(|()| WouldBlock)?;
        Ok(res.ok())
    }
}

// region:implementation

fn propagate<T>(res: thread::Result<T>) -> T {
    res.unwrap_or_else(|a| panic::resume_unwind(a))
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

// endregion:implementation
