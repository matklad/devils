use std::{
    collections::VecDeque,
    iter,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

use devils::{Act, Activity, Addr, Hub, Mailbox, Void};

type ATask<T> = Activity<Void, Void, T>;
type AStream<Y> = Activity<Void, Y, ()>;

#[test]
fn run_to_completion() {
    let mut a = ATask::<i32>::spawn(|_inbox| 92);
    let res = a.recv();
    assert_eq!(res, Act::Complete(92));
}

#[test]
fn block_in_drop() {
    static FLAG: AtomicBool = AtomicBool::new(false);
    let a = ATask::<()>::spawn(|_| FLAG.store(true, Relaxed));
    drop(a);
    assert!(FLAG.load(Relaxed));
}

#[test]
fn drop_before_start() {
    let (r, mut a) = ATask::<i32>::new(|_inbox| 92);
    drop(r);
    let res = a.recv();
    assert_eq!(res, Act::Complete(None));
}

#[test]
fn drop_before_start2() {
    let (d, h) = ATask::<i32>::new(|_inbox| 92);
    drop(d);
    drop(h);
}

#[test]
fn drop_before_start3() {
    #![allow(unused)]

    ATask::<i32>::new(|_inbox| 92);
}

#[test]
#[should_panic]
fn propagates_panic() {
    ATask::<Void>::spawn(|_| unwind());
}

#[test]
fn stream_run_to_completion() {
    let mut h = AStream::<i32>::spawn(|mut mailbox| {
        for it in 1..=3 {
            mailbox.send(it)
        }
    });
    assert_eq!(h.recv(), Act::Yield(1));
    assert_eq!(h.recv(), Act::Yield(2));
    assert_eq!(h.recv(), Act::Yield(3));
    assert_eq!(h.recv(), Act::Complete(()));
}

#[test]
fn stream_drop_before_start() {
    let (d, mut h) = AStream::<Void>::new(|_| ());
    drop(d);
    let res = h.recv();
    assert_eq!(res, Act::Complete(None));
}

#[test]
fn stream_drop_before_start2() {
    let (d, h) = AStream::<Void>::new(|_| ());
    drop(d);
    drop(h);
}

#[test]
#[should_panic]
fn stream_propagates_panic() {
    AStream::<Void>::spawn(|_| unwind());
}

#[test]
fn sum() {
    let mut h = Activity::<u32, Void, u32>::spawn(|mut mailbox| {
        let mut total: u32 = 0;
        while let Some(msg) = mailbox.recv() {
            total += msg
        }
        total
    });
    for i in 1..=3 {
        h.send(i)
    }
    h.stop();
    let res = h.recv();
    assert_eq!(res, Act::Complete(6));
}

#[test]
fn two_devils() {
    let mut child = Activity::<u32, u32, ()>::spawn(move |mut mailbox| {
        while let Some(msg) = mailbox.recv() {
            mailbox.send(msg * 2)
        }
    });

    for i in 1..=3 {
        child.send(i);
    }
    child.stop();
    let mut i = 0;
    while let Act::Yield(j) = child.recv() {
        i += 1;
        assert_eq!(j, i * 2);
    }
    assert_eq!(i, 3)
}

#[test]
fn worker_devils() {
    static CNT: AtomicUsize = AtomicUsize::new(0);
    let mut hub = Hub::new();
    let perm = [7, 3, 1, 5, 4, 8, 6, 2, 10, 9];
    for i in perm {
        let a =
            ATask::<()>::spawn(
                move |_| {
                    while CNT.compare_exchange(i, i + 1, Relaxed, Relaxed).is_err() {}
                },
            );
        hub.link(a, |a| a.completed());
    }
    CNT.store(1, Relaxed);
    while let Some(()) = hub.next() {}
}

type Work = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    leader: Activity<Work, Void, ()>,
}

impl ThreadPool {
    pub fn new(n_threads: usize) -> ThreadPool {
        let leader = Activity::spawn(move |mailbox| {
            type Worker = Activity<Work, (), ()>;
            enum TPE {
                NewWork(Act<Work, ()>),
                WorkDone(Addr<Worker>, Act<(), ()>),
            }

            let (mut hub, _) = Hub::from_mailbox(mailbox, TPE::NewWork);

            let mut task_queue = VecDeque::new();
            let mut idle = Vec::new();
            for _ in 0..n_threads {
                let a = Activity::<Work, (), ()>::spawn(move |mut mailbox| {
                    while let Some(work) = mailbox.recv() {
                        work();
                        mailbox.send(())
                    }
                });
                let key = hub.link_with_addr(a, |addr, act| TPE::WorkDone(addr, act));
                idle.push(key)
            }

            let mut inbox_closed = false;
            while let Some(e) = hub.next() {
                match e {
                    TPE::NewWork(Act::Yield(work)) => task_queue.push_back(work),
                    TPE::NewWork(Act::Complete(())) => inbox_closed = true,
                    TPE::WorkDone(addr, work) => match work {
                        Act::Yield(()) => idle.push(addr),
                        Act::Complete(()) => (),
                    },
                }

                if !task_queue.is_empty() {
                    if let Some(addr) = idle.pop() {
                        let task = task_queue.pop_front().unwrap();
                        hub[addr].send(task);
                    }
                }

                if task_queue.is_empty() && inbox_closed {
                    for addr in idle.drain(..) {
                        hub[addr].stop()
                    }
                }
            }
        });

        ThreadPool { leader }
    }
    pub fn submit(&mut self, f: impl FnOnce() + Send + 'static) {
        self.leader.send(Box::new(f))
    }
    pub fn submit_activity<F, I, Y, C>(&mut self, f: F) -> Activity<I, Y, Option<C>>
    where
        I: Send + 'static,
        Y: Send + 'static,
        C: Send + 'static,
        F: FnOnce(Mailbox<I, Y, Option<C>>) -> C + Send + 'static,
    {
        let (f, h) = Activity::new(f);
        self.submit(f);
        h
    }
}

fn collatz(n: u32) -> impl Iterator<Item = u32> {
    iter::successors(Some(n), |&n| {
        if n == 1 || n == 0 {
            return None;
        }
        Some(if n % 2 == 0 { n / 2 } else { 3 * n + 1 })
    })
}

#[test]
fn test_pool() {
    fn step(_i: usize) {
        // std::thread::sleep(std::time::Duration::from_millis(fastrand::u64(..10)))
    }

    let rng = fastrand::Rng::new();

    let mut pool = ThreadPool::new(4);

    enum E {
        Proc(Act<(usize, u32), Option<()>>),
        Job(Option<(u32, usize)>),
    }
    let mut hub = Hub::new();

    for i in 0..100 {
        let start = rng.u32(..100);
        if rng.bool() {
            let h = pool.submit_activity(
                move |mut mailbox: Mailbox<Void, (usize, u32), Option<()>>| {
                    for c in collatz(start) {
                        step(i);
                        mailbox.send((i, c));
                    }
                },
            );
            hub.link(h, E::Proc);
        } else {
            let h = pool.submit_activity(move |_: Mailbox<Void, Void, Option<(u32, usize)>>| {
                let len = collatz(start).inspect(|_| step(i)).count();
                (start, len)
            });
            hub.link(h, |a| E::Job(a.completed()));
        }
    }

    while let Some(e) = hub.next() {
        match e {
            E::Job(Some((_start, _len))) => (),
            E::Job(None) => (),
            E::Proc(Act::Yield((_i, _c))) => (),
            E::Proc(Act::Complete(Some(()))) => (),
            E::Proc(Act::Complete(None)) => panic!(),
        }
    }
}

#[test]
#[should_panic]
fn test_pool_panics() {
    let mut pool = ThreadPool::new(3);
    pool.submit(|| sleep_ms(300));
    pool.submit(|| {
        sleep_ms(100);
        unwind()
    });
    pool.submit(|| {
        sleep_ms(200);
        unwind()
    });
}

#[test]
fn mini_rust_analyzer() {
    #[derive(Debug)]
    struct LspRequest {
        data: u32,
    }

    #[derive(Debug)]
    struct Response {
        data: u32,
    }

    #[derive(Debug)]
    struct VfsConfig;
    #[derive(Debug)]
    struct VfsChange;

    #[derive(Debug)]
    struct CheckMessage {
        message: String,
    }

    let mut ra = Activity::spawn(main_loop);

    for i in 1..3 {
        ra.send(LspRequest { data: i });
        sleep_ms(100);
    }

    fn main_loop(inbox: Mailbox<LspRequest, Void, ()>) {
        let mut pool = ThreadPool::new(4);
        type Checker = Activity<Void, CheckMessage, ()>;

        #[derive(Debug)]
        enum RAE {
            Inbox(Act<LspRequest, ()>),
            Vfs(Act<VfsChange, ()>),
            Response(Response),
            Checker(Addr<Checker>, Act<CheckMessage, ()>),
        }
        let (mut hub, _mailbox) = Hub::<RAE>::from_mailbox(inbox, RAE::Inbox);

        let vfs = Activity::<VfsConfig, VfsChange, ()>::spawn(move |mut mailbox| {
            while let Some(VfsConfig) = mailbox.recv() {
                mailbox.send(VfsChange);
                mailbox.send(VfsChange);
                mailbox.send(VfsChange);
            }
        });
        let vfs = hub.link(vfs, RAE::Vfs);

        let mut active_checker: Option<Addr<Checker>> = None;

        hub[vfs].send(VfsConfig);
        while let Some(event) = hub.next() {
            let mut restart_checker = false;
            match event {
                RAE::Inbox(Act::Complete(())) => {
                    hub[vfs].stop();
                    if let Some(addr) = active_checker {
                        hub[addr].stop();
                    }
                }
                RAE::Inbox(Act::Yield(request)) => {
                    let task =
                        pool.submit_activity(move |_: Mailbox<Void, Void, Option<Response>>| {
                            Response { data: request.data * 2 }
                        });
                    hub.link(task, |a| RAE::Response(a.completed().unwrap()));
                }
                RAE::Response(response) => {
                    eprintln!("responding: {}", response.data);
                }
                RAE::Vfs(mut event) => loop {
                    match event {
                        Act::Yield(change) => {
                            eprintln!("received vfs change: {:?}", change);
                            restart_checker = true;
                            // match hub[vfs].recv_now() {
                            //     Ok(it) => {
                            //         event = it;
                            //         continue;
                            //     }
                            //     Err(devils::WouldBlock) => (),
                            // }
                        }
                        Act::Complete(_) => (),
                    }
                    break;
                },
                RAE::Checker(addr, Act::Yield(CheckMessage { message })) => {
                    if Some(addr) == active_checker {
                        eprintln!("received check msg: {:?}", message)
                    }
                }
                RAE::Checker(addr, Act::Complete(())) => {
                    if Some(addr) == active_checker.take() {
                        active_checker = None;
                    }
                }
            }

            if restart_checker {
                if let Some(checker) = active_checker.take() {
                    hub[checker].stop()
                }
                let a = Activity::spawn(checker);
                let addr = hub.link_with_addr(a, |addr, a| RAE::Checker(addr, a));
                active_checker = Some(addr)
            }
        }
    }

    fn checker(mailbox: Mailbox<Void, CheckMessage, ()>) {
        #[derive(Default)]
        struct Process {
            killed: AtomicBool,
            i: AtomicUsize,
        }

        impl Process {
            fn kill(&self) {
                self.killed.store(true, Relaxed);
            }
            fn read(&self) -> Option<String> {
                if self.killed.load(Relaxed) {
                    return None;
                }
                sleep_ms(10);
                let i = self.i.fetch_add(1, Relaxed);
                if i == 6 {
                    return None;
                }
                Some(format!("check message {}", i))
            }
        }

        let process: Arc<Process> = Arc::new(Process::default());

        enum CE {
            Stop,
            Message(Act<String, ()>),
        }

        let (mut hub, mailbox) = Hub::from_mailbox(mailbox, |_| CE::Stop);
        let child = AStream::<String>::spawn({
            let process = Arc::clone(&process);
            move |mut mailbox| {
                while let Some(msg) = process.read() {
                    mailbox.send(msg);
                }
            }
        });
        hub.link(child, CE::Message);

        while let Some(event) = hub.next() {
            match event {
                CE::Stop => {
                    process.kill();
                    break;
                }
                CE::Message(Act::Yield(message)) => hub[mailbox].send(CheckMessage { message }),
                CE::Message(Act::Complete(())) => break,
            }
        }
    }
}

fn unwind() -> ! {
    std::panic::resume_unwind(Box::new(()))
}

#[allow(unused)]
fn sleep_ms(ms: u64) {
    std::thread::sleep(std::time::Duration::from_millis(ms))
}
