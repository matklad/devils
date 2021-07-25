use std::{
    any::{Any, TypeId},
    collections::{HashMap, VecDeque},
    fmt,
    marker::PhantomData,
    mem, ops, panic,
    sync::{Arc, Mutex, MutexGuard},
    thread::{self},
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
    sender: Option<Arc<Chan<I, ()>>>,
    receiver: Arc<Chan<Y, thread::Result<C>>>,
}

impl<I, Y, C> fmt::Debug for Activity<I, Y, C> {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

pub struct Mailbox<I, Y, C> {
    sender: Arc<Chan<Y, thread::Result<C>>>,
    receiver: Arc<Chan<I, ()>>,
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
        f: impl FnOnce(Mailbox<I, Y, Option<C>>) -> C + Send + 'static,
    ) -> (impl FnOnce() + Send + 'static, Activity<I, Y, Option<C>>) {
        Activity::new_(|mailbox| Some(f(mailbox)), Option::default)
    }

    pub fn spawn(f: impl FnOnce(Mailbox<I, Y, C>) -> C + Send + 'static) -> Self {
        let (f, h) = Self::new_(f, || unreachable!());
        std::thread::spawn(f);
        h
    }

    fn new_(
        f: impl FnOnce(Mailbox<I, Y, C>) -> C + Send + 'static,
        cancelled: impl FnOnce() -> C + Send + 'static,
    ) -> (impl FnOnce() + Send + 'static, Self) {
        let ichan = Chan::<I, ()>::new();
        let ichan = Arc::new(ichan);

        let ychan = Chan::<Y, thread::Result<C>>::new();
        let ychan = Arc::new(ychan);

        let f = {
            let mailbox = Mailbox { sender: Arc::clone(&ychan), receiver: Arc::clone(&ichan) };
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
                move || ychan.close_with(Ok(cancelled()))
            }));

            move || {
                let f = panic::AssertUnwindSafe(f);
                let r = panic::catch_unwind(move || f.0(mailbox));
                ychan.close_with(r);
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
            sender.close_with(())
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

pub struct Hub<E> {
    dynmap: HashMap<TypeId, Entries>,
    chan: Arc<Chan<Act<E, (TypeId, usize, thread::Result<E>)>, Void>>,
}

#[derive(Default)]
struct Entries {
    n: usize,
    m: HashMap<usize, Box<dyn Any>>,
}

pub struct Addr<A> {
    ghost: PhantomData<A>,
    n: usize,
}

impl<A> PartialEq for Addr<A> {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n
    }
}

impl<A> Eq for Addr<A> {}
impl<A> fmt::Debug for Addr<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Addr").field(&self.n).finish()
    }
}

impl<A> Clone for Addr<A> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<A> Copy for Addr<A> {}

impl<E: Send + 'static> Hub<E> {
    pub fn new() -> Hub<E> {
        Hub { dynmap: HashMap::new(), chan: Arc::new(Chan::new()) }
    }
    pub fn from_mailbox<I, Y, C, M>(
        mailbox: Mailbox<I, Y, C>,
        mut m: M,
    ) -> (Hub<E>, Addr<Mailbox<I, Y, C>>)
    where
        I: Send + 'static,
        Y: Send + 'static,
        C: Send + 'static,
        M: FnMut(Act<I, ()>) -> E + Send + 'static,
    {
        let mut hub = Hub::new();
        let me = Arc::clone(&hub.chan);
        let type_id = TypeId::of::<Mailbox<I, Y, C>>();
        let entires = hub.dynmap.entry(type_id).or_default();
        let n = entires.n;
        entires.n += 1;
        let addr = Addr { ghost: PhantomData, n };
        mailbox.receiver.link(Box::new({
            move |it| {
                match it {
                    Act::Yield(it) => me.send(Act::Yield(m(Act::Yield(it)))),
                    Act::Complete(()) => {
                        let r = Ok(m(Act::Complete(())));
                        me.send(Act::Complete((type_id, n, r)))
                    }
                }
                .unwrap_or_else(|_| todo!());
                Ok(())
            }
        }));
        entires.m.insert(n, Box::new(mailbox));
        (hub, addr)
    }
    pub fn link_with_addr<I, Y, C, M>(
        &mut self,
        activity: Activity<I, Y, C>,
        mut m: M,
    ) -> Addr<Activity<I, Y, C>>
    where
        I: Send + 'static,
        Y: Send + 'static,
        C: Send + 'static,
        M: FnMut(Addr<Activity<I, Y, C>>, Act<Y, C>) -> E + Send + 'static,
    {
        let me = Arc::clone(&self.chan);
        let type_id = TypeId::of::<Activity<I, Y, C>>();
        let entires = self.dynmap.entry(type_id).or_default();
        let n = entires.n;
        entires.n += 1;
        let addr = Addr { ghost: PhantomData, n };
        activity.receiver.link(Box::new({
            move |it| {
                match it {
                    Act::Yield(it) => me.send(Act::Yield(m(addr, Act::Yield(it)))),
                    Act::Complete(c) => {
                        let r = match c {
                            Err(err) => Err(err),
                            Ok(it) => Ok(m(addr, Act::Complete(it))),
                        };
                        me.send(Act::Complete((type_id, n, r)))
                    }
                }
                .unwrap_or_else(|_| todo!());
                Ok(())
            }
        }));
        entires.m.insert(n, Box::new(activity));
        addr
    }
    pub fn link<I, Y, C, M>(
        &mut self,
        activity: Activity<I, Y, C>,
        mut m: M,
    ) -> Addr<Activity<I, Y, C>>
    where
        I: Send + 'static,
        Y: Send + 'static,
        C: Send + 'static,
        M: FnMut(Act<Y, C>) -> E + Send + 'static,
    {
        self.link_with_addr(activity, move |_, act| m(act))
    }
    pub fn next(&mut self) -> Option<E> {
        if self.dynmap.iter().map(|it| it.1.m.len()).sum::<usize>() == 0 {
            return None;
        }
        match self.chan.recv().unwrap() {
            Act::Yield(it) => Some(it),
            Act::Complete((tid, n, res)) => {
                self.dynmap.get_mut(&tid).unwrap().m.remove(&n);
                Some(propagate(res))
            }
        }
    }
}

impl<T: 'static, E> ops::Index<Addr<T>> for Hub<E> {
    type Output = T;

    fn index(&self, index: Addr<T>) -> &Self::Output {
        self.dynmap[&TypeId::of::<T>()].m[&index.n].downcast_ref().unwrap()
    }
}

impl<T: 'static, E> ops::IndexMut<Addr<T>> for Hub<E> {
    fn index_mut(&mut self, index: Addr<T>) -> &mut Self::Output {
        self.dynmap
            .get_mut(&TypeId::of::<T>())
            .unwrap()
            .m
            .get_mut(&index.n)
            .unwrap()
            .downcast_mut()
            .unwrap()
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
    Forward {
        to: Box<dyn FnMut(Act<T, C>) -> Result<(), T> + Send + 'static>,
    },
}

impl<T, C> Chan<T, C> {
    fn new() -> Chan<T, C> {
        let state = Mutex::new(ChanState::Open { messages: VecDeque::new(), reader: None });
        Chan { state }
    }
    fn link(&self, mut to: Box<dyn FnMut(Act<T, C>) -> Result<(), T> + Send + 'static>) {
        let mut guard = self.state.lock().unwrap();
        match &mut *guard {
            ChanState::Closed { value, .. } => {
                if let Some(c) = value.take() {
                    to(Act::Complete(c)).unwrap_or_else(|_| panic!("cant' link to closed channel"))
                }
            }
            ChanState::Forward { to: _ } => panic!("can't link twice"),
            ChanState::Open { messages, reader } => {
                assert!(reader.is_none());
                for m in messages.drain(..) {
                    to(Act::Yield(m)).unwrap_or_else(|_| panic!("can't link to closed channel"));
                }
            }
        }
        *guard = ChanState::Forward { to }
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
            ChanState::Forward { to } => to(Act::Yield(value)),
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
                ChanState::Forward { .. } => return Err(None),
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
            ChanState::Forward { .. } => panic!("recv on forwarding channel"),
        }
    }
    fn close_with(&self, value: C) {
        let mut guard = self.state.lock().unwrap();
        match &mut *guard {
            ChanState::Closed { .. } => (),
            ChanState::Open { messages, reader } => {
                if let Some(t) = reader.take() {
                    t.unpark();
                }
                let messages = mem::take(messages);
                *guard = ChanState::Closed { messages, value: Some(value) };
            }
            ChanState::Forward { to } => {
                let _ = to(Act::Complete(value));
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
            ChanState::Forward { to: _ } => todo!(),
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
                ChanState::Forward { to: _ } => todo!(),
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
