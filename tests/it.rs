use std::{
    collections::VecDeque,
    iter,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
        Arc,
    },
};

use devils::{Task, Selector, Void};

#[test]
fn run_to_completion() {
    let h = Task::<Void, i32>::spawn(|_inbox| 92);
    let res = h.join();
    assert_eq!(res, Some(92));
}

#[test]
fn drop_before_start() {
    let (r, h) = Task::<Void, i32>::new(|_inbox| 92);
    drop(r);
    let res = h.join();
    assert_eq!(res, None);
}

#[test]
fn drop_before_start2() {
    let (d, h) = Task::<Void, i32>::new(|_inbox| 92);
    drop(d);
    drop(h);
}

#[test]
fn drop_before_start3() {
    #![allow(unused)]

    Task::<Void, i32>::new(|_inbox| 92);
}

#[test]
#[should_panic]
fn propagates_panic() {
    Task::<Void, Void>::spawn(|_| unwind());
}

#[test]
fn stream_run_to_completion() {
    let mut h = devils::spawn_stream_new_thread(
        |_: &mut devils::Receiver<Void>, outbox: &mut devils::Sender<i32>| {
            (1..=3).for_each(|it| outbox.send(it))
        },
    );
    assert_eq!(h.recv(), Some(1));
    assert_eq!(h.recv(), Some(2));
    assert_eq!(h.recv(), Some(3));
    assert_eq!(h.recv(), None);
}

#[test]
fn stream_drop_before_start() {
    let (mut h, d) =
        devils::spawn_stream(|_: &mut devils::Receiver<Void>, _: &mut devils::Sender<Void>| ());
    drop(d);
    let res = h.recv();
    assert_eq!(res, None);
}

#[test]
fn stream_drop_before_start2() {
    let (h, d) =
        devils::spawn_stream(|_: &mut devils::Receiver<Void>, _: &mut devils::Sender<Void>| ());
    drop(d);
    drop(h);
}

#[test]
#[should_panic]
fn stream_propagates_panic() {
    devils::spawn_stream_new_thread(
        |_: &mut devils::Receiver<Void>, _: &mut devils::Sender<Void>| unwind(),
    );
}

#[test]
fn sum() {
    let mut h = devils::spawn_new_thread(|inbox| {
        let mut total: u32 = 0;
        for msg in inbox {
            total += msg
        }
        total
    });
    for i in 1..=3 {
        h.send(i)
    }
    let res = h.join();
    assert_eq!(res, Some(6));
}

#[test]
fn two_devils() {
    let mut child = devils::spawn_stream_new_thread(move |child_inbox, outbox| {
        for msg in child_inbox {
            outbox.send(msg * 2)
        }
    });

    for i in 1..=3 {
        child.send(i);
    }
    child.stop();
    let mut i = 0;
    while let Some(j) = child.recv() {
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
        handles.push(devils::spawn_new_thread(move |_: &mut devils::Receiver<Void>| {
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
        let h = handles.swap_remove(i);
        assert!(h.join_now().is_some());
    }
}

type Work = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    leader: devils::Task<Work, ()>,
}

impl ThreadPool {
    pub fn new(n_threads: usize) -> ThreadPool {
        let leader = devils::spawn_new_thread(move |inbox| {
            let mut task_queue = VecDeque::new();
            let mut idle: Vec<usize> = (0..n_threads).collect();
            let mut workers: Vec<devils::StreamHandle<Work, ()>> = (0..n_threads)
                .map(|_worker| {
                    devils::spawn_stream_new_thread(
                        move |inbox: &mut devils::Receiver<Work>,
                              outbox: &mut devils::Sender<()>| {
                            for work in inbox {
                                work();
                                outbox.send(())
                            }
                        },
                    )
                })
                .collect();

            let mut inbox_open = true;
            loop {
                let key = {
                    let mut sel = devils::Selector::new();
                    if inbox_open {
                        sel.add(workers.len(), inbox.event());
                    }
                    for (i, w) in workers.iter_mut().enumerate() {
                        sel.add(i, w.event());
                    }

                    sel.wait()
                };

                if key == workers.len() {
                    match inbox.recv_now() {
                        Some(task) => task_queue.push_back(task),
                        None => inbox_open = false,
                    }
                } else {
                    match workers[key].recv_now() {
                        Some(()) => idle.push(key),
                        None => panic!("worker thread died"),
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
    pub fn submit_devil<F, I, T>(&mut self, f: F) -> devils::Task<I, T>
    where
        T: Send + 'static,
        I: Send + 'static,
        F: FnOnce(&mut devils::Receiver<I>) -> T + Send + 'static,
    {
        let (r, h) = Task::<I, T>::new(f);
        self.submit(r);
        h
    }
    pub fn submit_devil_stream<F, I, T>(&mut self, f: F) -> devils::StreamHandle<I, T>
    where
        T: Send + 'static,
        I: Send + 'static,
        F: FnOnce(&mut devils::Receiver<I>, &mut devils::Sender<T>) + Send + 'static,
    {
        let (h, d) = devils::spawn_stream(f);
        self.submit(move || d.run());
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
            let h = pool.submit_devil_stream(move |_: &mut devils::Receiver<Void>, outbox| {
                for c in collatz(start) {
                    step(i);
                    outbox.send((i, c));
                }
            });
            procs.push(h)
        } else {
            let h = pool.submit_devil(move |_: &mut devils::Receiver<Void>| {
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
            Key::Job(i) => {
                let (_start, _len) = jobs.swap_remove(i).join_now().unwrap();
            }
            Key::Proc(i) => match procs[i].recv_now() {
                Some((_i, _c)) => {}
                None => {
                    procs.swap_remove(i);
                }
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

    let mut ra = devils::spawn_new_thread(main_loop);

    for i in 1..3 {
        ra.send(LspRequest { data: i });
        sleep_ms(100);
    }

    fn main_loop(inbox: &mut devils::Receiver<LspRequest>) {
        let mut pool = ThreadPool::new(4);

        let mut vfs = devils::spawn_stream_new_thread(
            move |inbox: &mut devils::Receiver<VfsConfig>,
                  outbox: &mut devils::Sender<VfsChange>| {
                while let Some(VfsConfig) = inbox.next() {
                    outbox.send(VfsChange);
                    outbox.send(VfsChange);
                    outbox.send(VfsChange);
                }
            },
        );

        let mut tasks: Vec<devils::Task<Void, Response>> = Vec::new();
        let mut active_checker: Option<devils::StreamHandle<Void, CheckMessage>> = None;
        let mut stopped_checkers: Vec<devils::StreamHandle<Void, CheckMessage>> = Vec::new();

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
                Key::Inbox => match inbox.recv_now() {
                    Some(request) => {
                        let task = pool.submit_devil(move |_: &mut devils::Receiver<Void>| {
                            Response { data: request.data * 2 }
                        });
                        tasks.push(task);
                    }
                    None => break,
                },
                Key::Task(idx) => {
                    let task = tasks.swap_remove(idx);
                    let resp = task.join_now().expect("task is never cancelled");
                    eprintln!("responding: {}", resp.data);
                }
                Key::Vfs => {
                    let VfsChange = vfs.recv_now().expect("vfs is never cancelled");
                    eprintln!("received vfs change");
                    restart_checker = true;
                }
                Key::Checker => {
                    let check_message = active_checker.as_mut().unwrap().recv_now();
                    match check_message {
                        Some(CheckMessage { message }) => {
                            eprintln!("received check msg: {:?}", message)
                        }
                        None => active_checker = None,
                    }
                }
                Key::StoppedChecker(idx) => match stopped_checkers[idx].recv_now() {
                    Some(CheckMessage { .. }) => (),
                    None => {
                        stopped_checkers.swap_remove(idx);
                    }
                },
            }

            if restart_checker {
                if let Some(checker) = active_checker.take() {
                    stopped_checkers.push(checker);
                }
                active_checker = Some(devils::spawn_stream_new_thread(checker))
            }
        }
    }

    fn checker(inbox: &mut devils::Receiver<Void>, outbox: &mut devils::Sender<CheckMessage>) {
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
        let mut child = devils::spawn_stream_new_thread({
            let process = Arc::clone(&process);
            move |_: &mut devils::Receiver<Void>, outbox: &mut devils::Sender<String>| {
                while let Some(msg) = process.read() {
                    outbox.send(msg);
                }
            }
        });

        loop {
            let mut sel = Selector::new();
            sel.add(true, inbox.event());
            sel.add(false, child.event());
            if sel.wait() {
                match inbox.recv_now() {
                    Some(void) => match void {},
                    None => {
                        process.kill();
                        break;
                    }
                }
            } else {
                match child.recv_now() {
                    Some(message) => outbox.send(CheckMessage { message }),
                    None => break,
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
