//! RPC client(shim 与 cli 共用):阻塞 IO + 一个后台读线程,不引入 tokio。
//! call 带超时 —— 任何情况下不得无限阻塞前端 UI 线程(NFR1)。
use rime_proto::{read_frame, write_frame, Frame, Notify, Op, OpResult, Request};
use std::collections::HashMap;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CallReply {
    pub result: OpResult,
    pub sync_notifications: Vec<Notify>,
}

struct Inner {
    writer: Mutex<UnixStream>,
    pending: Mutex<HashMap<u64, SyncSender<CallReply>>>,
    next_id: AtomicU64,
    dead: AtomicBool,
    notify_tx: Mutex<Option<Sender<Notify>>>,
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    pub fn connect(sock: &Path) -> io::Result<Client> {
        let stream = UnixStream::connect(sock)?;
        let reader = stream.try_clone()?;
        let inner = Arc::new(Inner {
            writer: Mutex::new(stream),
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            dead: AtomicBool::new(false),
            notify_tx: Mutex::new(None),
        });
        let weak = Arc::downgrade(&inner);
        std::thread::Builder::new()
            .name("rime-client-reader".into())
            .spawn(move || {
                let mut reader = reader;
                while let Ok(frame) = read_frame(&mut reader) {
                    let Some(inner) = weak.upgrade() else { break };
                    match frame {
                        Frame::Response(resp) => {
                            if let Some(tx) = inner.pending.lock().unwrap().remove(&resp.id) {
                                let _ = tx.send(CallReply {
                                    result: resp.result,
                                    sync_notifications: resp.sync_notifications,
                                });
                            }
                        }
                        Frame::Notify(n) => {
                            if let Some(tx) = inner.notify_tx.lock().unwrap().as_ref() {
                                let _ = tx.send(n);
                            }
                        }
                        Frame::Request(_) => {} // server 不该发 Request,忽略
                    }
                }
                if let Some(inner) = weak.upgrade() {
                    inner.dead.store(true, Ordering::SeqCst);
                    // 断连:清空 pending(drop sender 即让等待者 recv 出错,不吞不骗)
                    inner.pending.lock().unwrap().clear();
                }
            })
            .expect("spawn reader thread");
        Ok(Client { inner })
    }

    pub fn is_dead(&self) -> bool {
        self.inner.dead.load(Ordering::SeqCst)
    }

    /// 异步 Notify(deploy 类)的接收端;同步通知走 CallReply.sync_notifications
    pub fn set_notify_sink(&self, tx: Sender<Notify>) {
        *self.inner.notify_tx.lock().unwrap() = Some(tx);
    }

    pub fn call(&self, op: Op) -> io::Result<CallReply> {
        self.call_timeout(op, DEFAULT_CALL_TIMEOUT)
    }

    pub fn call_timeout(&self, op: Op, timeout: Duration) -> io::Result<CallReply> {
        if self.is_dead() {
            return Err(io::ErrorKind::NotConnected.into());
        }
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = sync_channel(1);
        self.inner.pending.lock().unwrap().insert(id, tx);
        {
            let mut w = self.inner.writer.lock().unwrap();
            if let Err(e) = write_frame(&mut *w, &Frame::Request(Request { id, op })) {
                self.inner.pending.lock().unwrap().remove(&id); // 失败即清理,防泄漏
                self.inner.dead.store(true, Ordering::SeqCst);
                return Err(e);
            }
        }
        match rx.recv_timeout(timeout) {
            Ok(reply) => Ok(reply),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                self.inner.pending.lock().unwrap().remove(&id);
                Err(io::ErrorKind::TimedOut.into())
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(io::ErrorKind::BrokenPipe.into())
            }
        }
    }
}

/// 连接;失败则 spawn daemon 并退避重试,直到 total_wait 用尽。
/// spawn 由调用方提供 —— shim 里绝不能用 current_exe(那是宿主进程,如 emacs)。
pub fn connect_or_spawn(
    sock: &Path,
    spawn: impl Fn() -> io::Result<std::process::Child>,
    total_wait: Duration,
) -> io::Result<Client> {
    if let Ok(c) = Client::connect(sock) {
        return Ok(c);
    }
    let mut child = Some(spawn()?);
    let deadline = Instant::now() + total_wait;
    let mut backoff = Duration::from_millis(50);
    loop {
        match Client::connect(sock) {
            Ok(c) => return Ok(c),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(e);
                }
                // spawn 的进程可能立即退出(另一个 daemon 持锁)——继续等 socket 即可
                if let Some(ch) = child.as_mut() {
                    let _ = ch.try_wait();
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(500));
            }
        }
    }
}

/// 供 shim 实现 join_maintenance_thread 语义:轮询 Hello 直到 Ready
pub fn wait_ready(client: &Client, total_wait: Duration) -> io::Result<()> {
    let deadline = Instant::now() + total_wait;
    loop {
        let reply = client.call_timeout(
            Op::Hello {
                proto_ver: rime_proto::PROTO_VER,
            },
            Duration::from_secs(2),
        );
        if let Ok(r) = &reply {
            if let OpResult::HelloOk { state, .. } = &r.result {
                if *state == rime_proto::ServerState::Ready {
                    return Ok(());
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

pub use rime_proto::paths;
