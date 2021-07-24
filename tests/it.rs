use std::{
    collections::VecDeque,
    iter,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

use devils::{Act, Activity, Mailbox, Selector, Void};

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
    let mut h = AStream::<i32>::spawn(|mailbox| {
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
    let mut h = Activity::<u32, Void, u32>::spawn(|mailbox| {
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
    let mut child = Activity::<u32, u32, ()>::spawn(move |mailbox| {
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
    let mut handles = Vec::new();
    let perm = [7, 3, 1, 5, 4, 8, 6, 2, 10, 9];
    for i in perm {
        handles.push(ATask::<()>::spawn(move |_| {
            while CNT.compare_exchange(i, i + 1, Relaxed, Relaxed).is_err() {}
        }))
    }
    CNT.store(1, Relaxed);
    while !handles.is_empty() {
        let mut sel = devils::Selector::new();
        for (i, h) in handles.iter_mut().enumerate() {
            sel.add(i, h.event());
        }
        let i = sel.wait();
        let mut h = handles.swap_remove(i);
        assert_eq!(h.recv_now().unwrap(), Act::Complete(()));
    }
}

type Work = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    leader: Activity<Work, Void, ()>,
}

impl ThreadPool {
    pub fn new(n_threads: usize) -> ThreadPool {
        let leader = Activity::spawn(move |mailbox| {
            let mut task_queue = VecDeque::new();
            let mut idle: Vec<usize> = (0..n_threads).collect();
            let mut workers: Vec<_> = (0..n_threads)
                .map(|_worker| {
                    Activity::<Work, (), ()>::spawn(move |mailbox| {
                        while let Some(work) = mailbox.recv() {
                            work();
                            mailbox.send(())
                        }
                    })
                })
                .collect();

            let mut inbox_open = true;
            loop {
                let key = {
                    let mut sel = devils::Selector::new();
                    if inbox_open {
                        sel.add(workers.len(), mailbox.event());
                    }
                    for (i, w) in workers.iter_mut().enumerate() {
                        sel.add(i, w.event());
                    }

                    sel.wait()
                };

                if key == workers.len() {
                    match mailbox.recv_now().unwrap() {
                        Some(task) => task_queue.push_back(task),
                        None => inbox_open = false,
                    }
                } else {
                    match workers[key].recv_now().unwrap() {
                        Act::Yield(()) => idle.push(key),
                        Act::Complete(()) => panic!("worker thread died"),
                    }
                }

                if !task_queue.is_empty() {
                    if let Some(idx) = idle.pop() {
                        let task = task_queue.pop_front().unwrap();
                        workers[idx].send(task);
                    }
                }
                if !inbox_open && task_queue.is_empty() && idle.len() == workers.len() {
                    break;
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
        F: FnOnce(&mut Mailbox<I, Y, Option<C>>) -> C + Send + 'static,
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
    let mut procs = Vec::new();
    let mut jobs = Vec::new();

    for i in 0..100 {
        let start = rng.u32(..100);
        if rng.bool() {
            let h = pool.submit_activity(
                move |mailbox: &mut Mailbox<Void, (usize, u32), Option<()>>| {
                    for c in collatz(start) {
                        step(i);
                        mailbox.send((i, c));
                    }
                },
            );
            procs.push(h)
        } else {
            let h =
                pool.submit_activity(move |_: &mut Mailbox<Void, Void, Option<(u32, usize)>>| {
                    let len = collatz(start).inspect(|_| step(i)).count();
                    (start, len)
                });
            jobs.push(h)
        }
    }

    while !(procs.is_empty() && jobs.is_empty()) {
        enum Key {
            Proc(usize),
            Job(usize),
        }
        let key = {
            let mut sel = devils::Selector::new();
            for (i, p) in procs.iter_mut().enumerate() {
                sel.add(Key::Proc(i), p.event());
            }
            for (i, y) in jobs.iter_mut().enumerate() {
                sel.add(Key::Job(i), y.event());
            }
            sel.wait()
        };

        match key {
            Key::Job(i) => match jobs.swap_remove(i).recv_now().unwrap().completed() {
                Some((_start, _len)) => (),
                None => panic!(),
            },
            Key::Proc(i) => match procs[i].recv_now().unwrap() {
                Act::Yield((_i, _c)) => {}
                Act::Complete(Some(())) => {
                    procs.swap_remove(i);
                }
                Act::Complete(None) => panic!(),
            },
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
    struct LspRequest {
        data: u32,
    }

    struct Response {
        data: u32,
    }

    struct VfsConfig;
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

    fn main_loop(inbox: &mut Mailbox<LspRequest, Void, ()>) {
        let mut pool = ThreadPool::new(4);

        let mut vfs = Activity::<VfsConfig, VfsChange, ()>::spawn(move |mailbox| {
            while let Some(VfsConfig) = mailbox.recv() {
                mailbox.send(VfsChange);
                mailbox.send(VfsChange);
                mailbox.send(VfsChange);
            }
        });

        let mut tasks: Vec<Activity<Void, Void, Option<Response>>> = Vec::new();
        let mut active_checker: Option<AStream<CheckMessage>> = None;
        let mut stopped_checkers: Vec<AStream<CheckMessage>> = Vec::new();

        vfs.send(VfsConfig);

        loop {
            #[derive(Debug)]
            enum Key {
                Inbox,
                Task(usize),
                Vfs,
                Checker,
                StoppedChecker(usize),
            }

            let key = {
                let mut sel = devils::Selector::new();
                sel.add(Key::Inbox, inbox.event());
                for (idx, task) in tasks.iter_mut().enumerate() {
                    sel.add(Key::Task(idx), task.event());
                }
                sel.add(Key::Vfs, vfs.event());
                if let Some(checker) = &mut active_checker {
                    sel.add(Key::Checker, checker.event())
                }
                for (idx, checker) in stopped_checkers.iter_mut().enumerate() {
                    sel.add(Key::StoppedChecker(idx), checker.event())
                }
                sel.wait()
            };

            eprintln!("key = {:?}", key);

            let mut restart_checker = false;
            match key {
                Key::Inbox => match inbox.recv_now().unwrap() {
                    Some(request) => {
                        let task = pool.submit_activity(
                            move |_: &mut Mailbox<Void, Void, Option<Response>>| Response {
                                data: request.data * 2,
                            },
                        );
                        tasks.push(task);
                    }
                    None => break,
                },
                Key::Task(idx) => {
                    let mut task = tasks.swap_remove(idx);
                    let resp = match task.recv_now().unwrap().completed() {
                        Some(it) => it,
                        None => unreachable!(),
                    };
                    eprintln!("responding: {}", resp.data);
                }
                Key::Vfs => {
                    let VfsChange = match vfs.recv_now().unwrap() {
                        Act::Yield(it) => it,
                        Act::Complete(()) => panic!("vfs never cancelled"),
                    };
                    eprintln!("received vfs change");
                    restart_checker = true;
                }
                Key::Checker => {
                    let check_message = active_checker.as_mut().unwrap().recv_now().unwrap();
                    match check_message {
                        Act::Yield(CheckMessage { message }) => {
                            eprintln!("received check msg: {:?}", message)
                        }
                        Act::Complete(()) => active_checker = None,
                    }
                }
                Key::StoppedChecker(idx) => match stopped_checkers[idx].recv_now().unwrap() {
                    Act::Yield(CheckMessage { .. }) => (),
                    Act::Complete(()) => {
                        stopped_checkers.swap_remove(idx);
                    }
                },
            }

            if restart_checker {
                if let Some(checker) = active_checker.take() {
                    stopped_checkers.push(checker);
                }
                active_checker = Some(Activity::spawn(checker))
            }
        }
    }

    fn checker(mailbox: &mut Mailbox<Void, CheckMessage, ()>) {
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
        let mut child = AStream::<String>::spawn({
            let process = Arc::clone(&process);
            move |mailbox| {
                while let Some(msg) = process.read() {
                    mailbox.send(msg);
                }
            }
        });

        loop {
            let mut sel = Selector::new();
            sel.add(true, mailbox.event());
            sel.add(false, child.event());
            if sel.wait() {
                match mailbox.recv_now().unwrap() {
                    Some(void) => match void {},
                    None => {
                        process.kill();
                        break;
                    }
                }
            } else {
                match child.recv_now().unwrap() {
                    Act::Yield(message) => mailbox.send(CheckMessage { message }),
                    Act::Complete(()) => break,
                }
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
