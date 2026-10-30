//! daemon 核心:唯一持有 C++ librime 的进程。
//! - 所有 librime API 调用串行在单一 worker 线程(Service::sessions_ 无锁,必须串行)
//! - 通知回调可能来自 deployer 线程(deploy/sync 类)→ 回调只投 channel,不碰共享状态
//! - 先 bind socket 再初始化 librime:首次部署(可达 ~20s)期间业务 op 快速回 Busy
use rime_proto::{
    paths, write_frame, CandidateData, CommitData, CompositionData, ConfigKind, ContextData, Frame,
    MenuData, Notify, Op, OpResult, Response, SchemaItem, ServerState, StatusData, PROTO_VER,
};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;
use std::time::Duration;

use libc::{c_char, c_int, c_void};
use rime_sys as rs;
use rime_sys::rime_struct;

pub struct ServeOpts {
    pub user_data_dir: PathBuf,
    pub shared_data_dir: PathBuf,
}

type ConnId = u64;

enum Job {
    Op { conn: ConnId, id: u64, op: Op },
    ConnClosed(ConnId),
}

struct Conns {
    map: Mutex<HashMap<ConnId, Sender<Frame>>>,
}

impl Conns {
    fn send_to(&self, conn: ConnId, frame: Frame) {
        if let Some(tx) = self.map.lock().unwrap().get(&conn) {
            let _ = tx.send(frame);
        }
    }
    fn broadcast_except(&self, except: Option<ConnId>, n: &Notify) {
        for (id, tx) in self.map.lock().unwrap().iter() {
            if Some(*id) != except {
                let _ = tx.send(Frame::Notify(n.clone()));
            }
        }
    }
}

/// 单例锁:<user_data_dir>/.rime-daemon.lock(必须本地磁盘,NFS 不支持)
fn acquire_singleton_lock(user_data_dir: &Path) -> io::Result<std::fs::File> {
    std::fs::create_dir_all(user_data_dir)?;
    let lock_path = user_data_dir.join(".rime-daemon.lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "another rime-daemon holds the lock for this user_data_dir",
        ));
    }
    Ok(f)
}

fn bind_socket(sock: &Path) -> io::Result<UnixListener> {
    if let Some(dir) = sock.parent() {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    if sock.exists() {
        // 持锁者才走到这里:残留 socket 必属已死 daemon,清掉
        std::fs::remove_file(sock)?;
    }
    let l = UnixListener::bind(sock)?;
    std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600))?;
    Ok(l)
}

pub fn serve(opts: ServeOpts) -> io::Result<()> {
    let _lock = acquire_singleton_lock(&opts.user_data_dir)?;
    let sock = paths::sock_path(&opts.user_data_dir);
    let mut listener = bind_socket(&sock)?;
    listener.set_nonblocking(true)?;
    eprintln!("[rime-daemon] listening on {}", sock.display());

    let (job_tx, job_rx) = channel::<Job>();
    let conns: &'static Conns = Box::leak(Box::new(Conns {
        map: Mutex::new(HashMap::new()),
    }));

    let sock_for_worker = sock.clone();
    std::thread::Builder::new()
        .name("rime-worker".into())
        .spawn(move || worker_main(opts, job_rx, conns, sock_for_worker))
        .expect("spawn rime worker");

    let mut next_conn: ConnId = 1;
    loop {
        let stream = match listener.accept() {
            Ok((stream, _)) => stream,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !sock.exists() {
                    listener = bind_socket(&sock)?;
                    listener.set_nonblocking(true)?;
                }
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(_) => continue,
        };
        let conn_id = next_conn;
        next_conn += 1;
        let (out_tx, out_rx) = channel::<Frame>();
        conns.map.lock().unwrap().insert(conn_id, out_tx);
        spawn_writer(stream.try_clone()?, out_rx);
        spawn_reader(stream, conn_id, job_tx.clone(), conns);
        eprintln!("[rime-daemon] conn {conn_id} connected");
    }
}

fn spawn_writer(mut stream: UnixStream, out_rx: Receiver<Frame>) {
    std::thread::spawn(move || {
        for frame in out_rx {
            if write_frame(&mut stream, &frame).is_err() {
                break;
            }
        }
    });
}

fn spawn_reader(mut stream: UnixStream, conn: ConnId, job_tx: Sender<Job>, conns: &'static Conns) {
    std::thread::spawn(move || {
        loop {
            match rime_proto::read_frame(&mut stream) {
                Ok(Frame::Request(req)) => {
                    if job_tx
                        .send(Job::Op {
                            conn,
                            id: req.id,
                            op: req.op,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {} // client 不该发 Response/Notify
                Err(_) => break,
            }
        }
        conns.map.lock().unwrap().remove(&conn);
        let _ = job_tx.send(Job::ConnClosed(conn));
        eprintln!("[rime-daemon] conn {conn} closed");
    });
}

// ---------------- worker ----------------

/// 通知回调:可能在 worker 线程(option/schema,引擎调用栈内)或 deployer 线程(deploy/sync)
/// 触发。只投 channel。Sender 非 Sync,包 Mutex。
unsafe extern "C" fn notification_cb(
    ctx: *mut c_void,
    session: rs::RimeSessionId,
    ty: *const c_char,
    value: *const c_char,
) {
    let tx = &*(ctx as *const Mutex<Sender<Notify>>);
    let n = Notify {
        session: session as u64,
        message_type: cstr_to_string(ty).unwrap_or_default(),
        value: cstr_to_string(value).unwrap_or_default(),
    };
    let _ = tx.lock().unwrap().send(n);
}

unsafe fn cstr_to_string(p: *const c_char) -> Option<String> {
    if p.is_null() {
        None
    } else {
        Some(CStr::from_ptr(p).to_string_lossy().into_owned())
    }
}

struct GlobalState {
    schema: Option<String>,
    options: HashMap<String, bool>,
}

struct Worker {
    api: &'static rs::RimeApi,
    conns: &'static Conns,
    notif_rx: Receiver<Notify>,
    state: ServerState,
    global: GlobalState,
    /// 异步 sync:记住请求,静默窗口(无 composition + 一段无按键)才执行
    sync_requested: bool,
    sync_attempts: u32,
    last_engine_op: std::time::Instant,
    // conn ⇄ session/config-handle 映射;连接断开即销毁
    sessions: HashMap<u64, ConnId>,
    conn_sessions: HashMap<ConnId, Vec<u64>>,
    configs: HashMap<u64, (ConnId, rs::RimeConfig)>,
    next_config: u64,
    server_rime_ver: String,
    sock_path: PathBuf,
}

macro_rules! capi {
    ($api:expr, $f:ident $(, $a:expr)*) => {{
        #[allow(unused_unsafe)] // 宏可能在已有 unsafe 上下文中展开
        let r = unsafe { ($api.$f.expect(concat!("librime api missing: ", stringify!($f))))($($a),*) };
        r
    }};
}

fn worker_main(opts: ServeOpts, job_rx: Receiver<Job>, conns: &'static Conns, sock_path: PathBuf) {
    let api: &'static rs::RimeApi = unsafe { &*rs::rime_get_api() };

    // traits 的 CString 必须活过 setup/initialize
    let shared = CString::new(opts.shared_data_dir.to_string_lossy().as_bytes()).unwrap();
    let user = CString::new(opts.user_data_dir.to_string_lossy().as_bytes()).unwrap();
    let log_dir_path = opts.user_data_dir.join("log");
    let _ = std::fs::create_dir_all(&log_dir_path);
    let log_dir = CString::new(log_dir_path.to_string_lossy().as_bytes()).unwrap();
    let app_name = CString::new("rime.daemon").unwrap();
    let dist_name = CString::new("Rime").unwrap();
    let dist_code = CString::new("rime-daemon").unwrap();
    let dist_ver = CString::new("0.1.0").unwrap();

    let mut traits = rime_struct!(rs::RimeTraits);
    traits.shared_data_dir = shared.as_ptr();
    traits.user_data_dir = user.as_ptr();
    traits.app_name = app_name.as_ptr();
    traits.distribution_name = dist_name.as_ptr();
    traits.distribution_code_name = dist_code.as_ptr();
    traits.distribution_version = dist_ver.as_ptr();
    traits.min_log_level = 2; // ERROR;详细日志进 log_dir
    traits.log_dir = log_dir.as_ptr();

    capi!(api, setup, &mut traits);

    let (notif_tx, notif_rx) = channel::<Notify>();
    let notif_ctx: &'static Mutex<Sender<Notify>> = Box::leak(Box::new(Mutex::new(notif_tx)));
    capi!(
        api,
        set_notification_handler,
        Some(notification_cb),
        notif_ctx as *const _ as *mut c_void
    );

    capi!(api, initialize, &mut traits);

    // False = 有变化才部署(True 是跳过检测强制全量,POC 的用法,勿沿用)
    let maintaining = capi!(api, start_maintenance, 0) != 0;
    let state = if maintaining {
        ServerState::Maintenancing
    } else {
        ServerState::Ready
    };
    eprintln!("[rime-daemon] librime initialized, state={state:?}");

    let ver = unsafe { cstr_to_string(capi!(api, get_version)) }.unwrap_or_default();

    let mut w = Worker {
        api,
        conns,
        notif_rx,
        state,
        global: GlobalState {
            schema: None,
            options: HashMap::new(),
        },
        sync_requested: false,
        sync_attempts: 0,
        last_engine_op: std::time::Instant::now(),
        sessions: HashMap::new(),
        conn_sessions: HashMap::new(),
        configs: HashMap::new(),
        next_config: 1,
        server_rime_ver: ver,
        sock_path,
    };

    loop {
        // 轮询 job + 异步通知(deployer 线程的 deploy 通知也从 notif_rx 进来)
        match job_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Job::Op { conn, id, op }) => w.handle_op(conn, id, op),
            Ok(Job::ConnClosed(conn)) => w.conn_closed(conn),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
        w.drain_notifications(None, &mut Vec::new());
        w.try_pending_sync();
    }
}

impl Worker {
    fn handle_op(&mut self, conn: ConnId, id: u64, op: Op) {
        // Hello / Shutdown 任何状态可用;业务 op 在 maintenance 期间快速回 Busy
        match &op {
            Op::Hello { proto_ver } => {
                let result = if *proto_ver != PROTO_VER {
                    OpResult::Error(format!(
                        "proto version mismatch: client={proto_ver} server={PROTO_VER}"
                    ))
                } else {
                    OpResult::HelloOk {
                        server_rime_ver: self.server_rime_ver.clone(),
                        proto_ver: PROTO_VER,
                        state: self.state.clone(),
                    }
                };
                self.respond(conn, id, result, Vec::new());
                return;
            }
            Op::Shutdown => {
                eprintln!("[rime-daemon] shutdown requested by conn {conn}");
                self.respond(conn, id, OpResult::Bool(true), Vec::new());
                std::thread::sleep(Duration::from_millis(100)); // 让 writer 把响应刷出去
                let _ = std::fs::remove_file(&self.sock_path);
                capi!(self.api, finalize);
                std::process::exit(0);
            }
            Op::SyncUserData => {
                // 异步语义:立即确认"已排队",静默窗口再执行(Maintenancing 期间也可排队)
                self.sync_requested = true;
                self.sync_attempts = 0;
                eprintln!("[rime-daemon] user-data sync scheduled by conn {conn}");
                self.respond(conn, id, OpResult::Bool(true), Vec::new());
                return;
            }
            _ => {}
        }
        if self.state == ServerState::Maintenancing {
            // 顺便消化可能已到达的 deploy 完成通知
            self.drain_notifications(None, &mut Vec::new());
        }
        if self.state == ServerState::Maintenancing {
            self.respond(conn, id, OpResult::Busy, Vec::new());
            return;
        }

        let op_session = op_session_id(&op);
        let result = self.dispatch(conn, &op);
        self.last_engine_op = std::time::Instant::now();
        let mut sync_notifs = Vec::new();
        self.drain_notifications(op_session, &mut sync_notifs);
        self.respond(conn, id, result, sync_notifs);
    }

    /// 延迟 sync 的执行时机:Ready + 2s 无引擎调用 + 所有 session 无未上屏输入。
    /// librime 的 sync_user_data 会 CleanupAllSessions —— 我们先自己销毁并同步账本,
    /// 客户端经 find_session=False 自愈重建(liberime 的 _ensure_session);
    /// 半截输入已确认不存在,所以自愈无用户可见损失。
    fn try_pending_sync(&mut self) {
        if !self.sync_requested || self.state != ServerState::Ready {
            return;
        }
        if self.last_engine_op.elapsed() < Duration::from_secs(2) {
            return;
        }
        let api = self.api;
        for sid in self.sessions.keys() {
            let mut st = rime_struct!(rs::RimeStatus);
            if capi!(api, get_status, *sid as rs::RimeSessionId, &mut st) != 0 {
                let composing = st.is_composing != 0;
                capi!(api, free_status, &mut st);
                if composing {
                    return; // 有人打到一半,等下一个静默窗口
                }
            }
        }
        self.sync_attempts += 1;
        eprintln!(
            "[rime-daemon] executing deferred user-data sync (attempt {})",
            self.sync_attempts
        );
        for (sid, _) in std::mem::take(&mut self.sessions) {
            capi!(api, destroy_session, sid as rs::RimeSessionId);
        }
        self.conn_sessions.clear();
        if capi!(api, sync_user_data) != 0 {
            // deployer 线程接手;完成经 deploy 通知置回 Ready 并广播
            self.state = ServerState::Maintenancing;
            self.sync_requested = false;
            self.sync_attempts = 0;
        } else if self.sync_attempts >= 3 {
            eprintln!("[rime-daemon] sync failed to start after 3 attempts; giving up");
            self.sync_requested = false;
            self.sync_attempts = 0;
        }
    }

    fn respond(&self, conn: ConnId, id: u64, result: OpResult, sync_notifications: Vec<Notify>) {
        self.conns.send_to(
            conn,
            Frame::Response(Response {
                id,
                result,
                sync_notifications,
            }),
        );
    }

    /// 串行调用 librime;所有 FFI 只发生在本线程
    fn dispatch(&mut self, conn: ConnId, op: &Op) -> OpResult {
        let api = self.api;
        match op {
            Op::Hello { .. } | Op::Shutdown | Op::SyncUserData => unreachable!(),
            Op::CreateSession => {
                let sid = capi!(api, create_session) as u64;
                if sid == 0 {
                    return OpResult::Error("create_session failed".into());
                }
                self.apply_global_state(sid);
                self.sessions.insert(sid, conn);
                self.conn_sessions.entry(conn).or_default().push(sid);
                OpResult::Session(sid)
            }
            Op::FindSession { session } => {
                OpResult::Bool(capi!(api, find_session, *session as rs::RimeSessionId) != 0)
            }
            Op::DestroySession { session } => {
                let ok = capi!(api, destroy_session, *session as rs::RimeSessionId) != 0;
                self.sessions.remove(session);
                if let Some(v) = self.conn_sessions.get_mut(&conn) {
                    v.retain(|s| s != session);
                }
                OpResult::Bool(ok)
            }
            Op::ProcessKey {
                session,
                keycode,
                mask,
            } => OpResult::Bool(
                capi!(
                    api,
                    process_key,
                    *session as rs::RimeSessionId,
                    *keycode,
                    *mask
                ) != 0,
            ),
            Op::SimulateKeySequence { session, keys } => {
                let ck = CString::new(keys.as_str()).unwrap_or_default();
                OpResult::Bool(
                    capi!(
                        api,
                        simulate_key_sequence,
                        *session as rs::RimeSessionId,
                        ck.as_ptr()
                    ) != 0,
                )
            }
            Op::CommitComposition { session } => {
                OpResult::Bool(capi!(api, commit_composition, *session as rs::RimeSessionId) != 0)
            }
            Op::ClearComposition { session } => {
                capi!(api, clear_composition, *session as rs::RimeSessionId);
                OpResult::Bool(true)
            }
            Op::GetContext { session } => self.get_context(*session),
            Op::GetCommit { session } => self.get_commit(*session),
            Op::GetStatus { session } => self.get_status(*session),
            Op::GetInput { session } => {
                let p = capi!(api, get_input, *session as rs::RimeSessionId);
                OpResult::Str(unsafe { cstr_to_string(p) })
            }
            Op::SelectCandidateOnCurrentPage { session, index } => OpResult::Bool(
                capi!(
                    api,
                    select_candidate_on_current_page,
                    *session as rs::RimeSessionId,
                    *index
                ) != 0,
            ),
            Op::GetCandidates {
                session,
                index,
                count,
            } => self.get_candidates(*session, *index, *count),
            Op::GetSchemaList => self.get_schema_list(),
            Op::GetCurrentSchema { session } => {
                let mut buf = [0 as c_char; 256];
                let ok = capi!(
                    api,
                    get_current_schema,
                    *session as rs::RimeSessionId,
                    buf.as_mut_ptr(),
                    buf.len()
                ) != 0;
                if ok {
                    OpResult::Str(unsafe { cstr_to_string(buf.as_ptr()) })
                } else {
                    OpResult::Str(None)
                }
            }
            Op::SelectSchema { session, schema_id } => {
                let cid = CString::new(schema_id.as_str()).unwrap_or_default();
                OpResult::Bool(
                    capi!(
                        api,
                        select_schema,
                        *session as rs::RimeSessionId,
                        cid.as_ptr()
                    ) != 0,
                )
            }
            Op::GetSyncDir => {
                let p = capi!(api, get_sync_dir);
                OpResult::Str(unsafe { cstr_to_string(p) })
            }
            Op::ConfigOpen { kind, id } => self.config_open(conn, kind, id),
            Op::ConfigClose { handle } => {
                if let Some((_, mut cfg)) = self.configs.remove(handle) {
                    capi!(api, config_close, &mut cfg);
                    OpResult::Bool(true)
                } else {
                    OpResult::Bool(false)
                }
            }
            Op::ConfigGetBool { handle, key } => self.with_config(handle, key, |api, cfg, k| {
                let mut v: rs::Bool = 0;
                if capi!(api, config_get_bool, cfg, k, &mut v) != 0 {
                    OpResult::CfgBool(Some(v != 0))
                } else {
                    OpResult::CfgBool(None)
                }
            }),
            Op::ConfigGetInt { handle, key } => self.with_config(handle, key, |api, cfg, k| {
                let mut v: c_int = 0;
                if capi!(api, config_get_int, cfg, k, &mut v) != 0 {
                    OpResult::CfgInt(Some(v))
                } else {
                    OpResult::CfgInt(None)
                }
            }),
            Op::ConfigGetDouble { handle, key } => self.with_config(handle, key, |api, cfg, k| {
                let mut v: f64 = 0.0;
                if capi!(api, config_get_double, cfg, k, &mut v) != 0 {
                    OpResult::CfgDouble(Some(v))
                } else {
                    OpResult::CfgDouble(None)
                }
            }),
            Op::ConfigGetString { handle, key } => self.with_config(handle, key, |api, cfg, k| {
                let p = capi!(api, config_get_cstring, cfg, k);
                OpResult::Str(unsafe { cstr_to_string(p) })
            }),
            Op::ConfigSetBool { handle, key, value } => {
                let v: rs::Bool = if *value { 1 } else { 0 };
                self.with_config(handle, key, move |api, cfg, k| {
                    OpResult::Bool(capi!(api, config_set_bool, cfg, k, v) != 0)
                })
            }
            Op::ConfigSetInt { handle, key, value } => {
                let v = *value;
                self.with_config(handle, key, move |api, cfg, k| {
                    OpResult::Bool(capi!(api, config_set_int, cfg, k, v) != 0)
                })
            }
            Op::ConfigSetDouble { handle, key, value } => {
                let v = *value;
                self.with_config(handle, key, move |api, cfg, k| {
                    OpResult::Bool(capi!(api, config_set_double, cfg, k, v) != 0)
                })
            }
            Op::ConfigSetString { handle, key, value } => {
                let cv = CString::new(value.as_str()).unwrap_or_default();
                self.with_config(handle, key, move |api, cfg, k| {
                    OpResult::Bool(capi!(api, config_set_string, cfg, k, cv.as_ptr()) != 0)
                })
            }
        }
    }

    fn with_config(
        &mut self,
        handle: &u64,
        key: &str,
        f: impl FnOnce(&'static rs::RimeApi, *mut rs::RimeConfig, *const c_char) -> OpResult,
    ) -> OpResult {
        let api = self.api;
        let Some((_, cfg)) = self.configs.get_mut(handle) else {
            return OpResult::Error("bad config handle".into());
        };
        let ck = CString::new(key).unwrap_or_default();
        f(api, cfg as *mut rs::RimeConfig, ck.as_ptr())
    }

    fn config_open(&mut self, conn: ConnId, kind: &ConfigKind, id: &str) -> OpResult {
        let api = self.api;
        let cid = CString::new(id).unwrap_or_default();
        let mut cfg: rs::RimeConfig = unsafe { std::mem::zeroed() };
        let ok = match kind {
            ConfigKind::Schema => capi!(api, schema_open, cid.as_ptr(), &mut cfg),
            ConfigKind::User => capi!(api, user_config_open, cid.as_ptr(), &mut cfg),
        } != 0;
        if !ok {
            return OpResult::Error(format!("config open failed: {id}"));
        }
        let h = self.next_config;
        self.next_config += 1;
        self.configs.insert(h, (conn, cfg));
        OpResult::ConfigHandle(h)
    }

    fn get_context(&mut self, session: u64) -> OpResult {
        let api = self.api;
        let mut ctx = rime_struct!(rs::RimeContext);
        if capi!(api, get_context, session as rs::RimeSessionId, &mut ctx) == 0 {
            return OpResult::Error("get_context failed".into());
        }
        let mut data = ContextData::default();
        unsafe {
            data.commit_text_preview = cstr_to_string(ctx.commit_text_preview);
            data.composition = CompositionData {
                length: ctx.composition.length,
                cursor_pos: ctx.composition.cursor_pos,
                sel_start: ctx.composition.sel_start,
                sel_end: ctx.composition.sel_end,
                preedit: cstr_to_string(ctx.composition.preedit),
            };
            let m = &ctx.menu;
            let mut cands = Vec::with_capacity(m.num_candidates.max(0) as usize);
            if !m.candidates.is_null() {
                for i in 0..m.num_candidates.max(0) as usize {
                    let c = &*m.candidates.add(i);
                    cands.push(CandidateData {
                        text: cstr_to_string(c.text).unwrap_or_default(),
                        comment: cstr_to_string(c.comment),
                    });
                }
            }
            data.menu = MenuData {
                page_size: m.page_size,
                page_no: m.page_no,
                is_last_page: m.is_last_page != 0,
                highlighted_candidate_index: m.highlighted_candidate_index,
                num_candidates: m.num_candidates,
                candidates: cands,
                select_keys: cstr_to_string(m.select_keys),
            };
            if !ctx.select_labels.is_null() && m.page_size > 0 {
                for i in 0..m.page_size as usize {
                    let p = *ctx.select_labels.add(i);
                    data.select_labels
                        .push(cstr_to_string(p).unwrap_or_default());
                }
            }
        }
        capi!(api, free_context, &mut ctx);
        OpResult::Context(Box::new(data))
    }

    fn get_commit(&mut self, session: u64) -> OpResult {
        let api = self.api;
        let mut commit = rime_struct!(rs::RimeCommit);
        if capi!(api, get_commit, session as rs::RimeSessionId, &mut commit) == 0 {
            return OpResult::Commit(None);
        }
        let text = unsafe { cstr_to_string(commit.text) }.unwrap_or_default();
        capi!(api, free_commit, &mut commit);
        OpResult::Commit(Some(CommitData { text }))
    }

    fn get_status(&mut self, session: u64) -> OpResult {
        let api = self.api;
        let mut st = rime_struct!(rs::RimeStatus);
        if capi!(api, get_status, session as rs::RimeSessionId, &mut st) == 0 {
            return OpResult::Error("get_status failed".into());
        }
        let data = unsafe {
            StatusData {
                schema_id: cstr_to_string(st.schema_id),
                schema_name: cstr_to_string(st.schema_name),
                is_disabled: st.is_disabled != 0,
                is_composing: st.is_composing != 0,
                is_ascii_mode: st.is_ascii_mode != 0,
                is_full_shape: st.is_full_shape != 0,
                is_simplified: st.is_simplified != 0,
                is_traditional: st.is_traditional != 0,
                is_ascii_punct: st.is_ascii_punct != 0,
            }
        };
        capi!(api, free_status, &mut st);
        OpResult::Status(Box::new(data))
    }

    fn get_candidates(&mut self, session: u64, index: usize, count: usize) -> OpResult {
        let api = self.api;
        let mut it: rs::RimeCandidateListIterator = unsafe { std::mem::zeroed() };
        let mut list = Vec::new();
        if capi!(
            api,
            candidate_list_from_index,
            session as rs::RimeSessionId,
            &mut it,
            index as c_int
        ) != 0
        {
            while list.len() < count && capi!(api, candidate_list_next, &mut it) != 0 {
                unsafe {
                    list.push(CandidateData {
                        text: cstr_to_string(it.candidate.text).unwrap_or_default(),
                        comment: cstr_to_string(it.candidate.comment),
                    });
                }
            }
            capi!(api, candidate_list_end, &mut it);
        }
        let exhausted = list.len() < count;
        OpResult::Candidates { list, exhausted }
    }

    fn get_schema_list(&mut self) -> OpResult {
        let api = self.api;
        let mut sl: rs::RimeSchemaList = unsafe { std::mem::zeroed() };
        if capi!(api, get_schema_list, &mut sl) == 0 {
            return OpResult::Error("get_schema_list failed".into());
        }
        let mut items = Vec::new();
        unsafe {
            for i in 0..sl.size {
                let item = &*sl.list.add(i);
                items.push(SchemaItem {
                    schema_id: cstr_to_string(item.schema_id).unwrap_or_default(),
                    name: cstr_to_string(item.name).unwrap_or_default(),
                });
            }
        }
        capi!(api, free_schema_list, &mut sl);
        OpResult::SchemaList(items)
    }

    // ---- 全局状态同步(§4):option/schema 全局统一,仅输入串 per-session ----

    fn apply_global_state(&mut self, sid: u64) {
        let api = self.api;
        // librime 默认恢复在前(create_session 内部),全局态 apply 在后,以全局态为准
        if let Some(schema) = self.global.schema.clone() {
            let cid = CString::new(schema).unwrap_or_default();
            capi!(api, select_schema, sid as rs::RimeSessionId, cid.as_ptr());
        }
        for (name, on) in self.global.options.clone() {
            let cn = CString::new(name).unwrap_or_default();
            let v: rs::Bool = if on { 1 } else { 0 };
            capi!(api, set_option, sid as rs::RimeSessionId, cn.as_ptr(), v);
        }
        // 丢弃 apply 自身触发的通知(抑制回环)
        while self.notif_rx.try_recv().is_ok() {}
    }

    /// dispatch 后调用:排空通知。
    /// - op_session 的 schema/option 通知 → 更新全局态 + 广播 apply 到其他 session
    ///   + 作为 sync_notifications 捎带给触发者 + Notify 推给其他 conn
    /// - deploy 通知(session=0,来自 deployer 线程)→ 状态机 + 广播所有 conn
    /// - 其余按 session→conn 定向
    fn drain_notifications(&mut self, op_session: Option<u64>, sync_out: &mut Vec<Notify>) {
        while let Ok(n) = self.notif_rx.try_recv() {
            if n.message_type == "deploy" {
                match n.value.as_str() {
                    "success" | "failure" => {
                        eprintln!("[rime-daemon] deploy {}", n.value);
                        self.state = ServerState::Ready;
                    }
                    _ => self.state = ServerState::Maintenancing,
                }
                self.conns.broadcast_except(None, &n);
                continue;
            }
            let from_op = op_session == Some(n.session);
            match n.message_type.as_str() {
                "schema" => {
                    // value = "schema_id/schema_name"
                    if let Some(id) = n.value.split('/').next() {
                        let id = id.to_string();
                        self.global.schema = Some(id.clone());
                        self.apply_to_others(n.session, |w, other| {
                            let cid = CString::new(id.as_str()).unwrap_or_default();
                            capi!(
                                w.api,
                                select_schema,
                                other as rs::RimeSessionId,
                                cid.as_ptr()
                            );
                        });
                    }
                }
                "option" => {
                    let (name, on) = match n.value.strip_prefix('!') {
                        Some(rest) => (rest.to_string(), false),
                        None => (n.value.clone(), true),
                    };
                    // 白名单:'_' 开头的引擎内部/瞬态 option 不参与全局同步
                    if !name.starts_with('_') {
                        self.global.options.insert(name.clone(), on);
                        let v: rs::Bool = if on { 1 } else { 0 };
                        self.apply_to_others(n.session, |w, other| {
                            let cn = CString::new(name.as_str()).unwrap_or_default();
                            capi!(
                                w.api,
                                set_option,
                                other as rs::RimeSessionId,
                                cn.as_ptr(),
                                v
                            );
                        });
                    }
                }
                _ => {}
            }
            if from_op {
                sync_out.push(n.clone());
                // 也推给其他 conn(它们的 UI 需要跟随全局状态)
                if n.message_type == "schema" || n.message_type == "option" {
                    let from_conn = self.sessions.get(&n.session).copied();
                    self.conns.broadcast_except(from_conn, &n);
                }
            } else if let Some(conn) = self.sessions.get(&n.session) {
                self.conns.send_to(*conn, Frame::Notify(n));
            }
        }
    }

    fn apply_to_others(&mut self, source: u64, f: impl Fn(&Worker, u64)) {
        let others: Vec<u64> = self
            .sessions
            .keys()
            .copied()
            .filter(|s| *s != source)
            .collect();
        for other in others {
            f(self, other);
        }
        // 抑制回环:丢弃 apply 触发的连锁通知
        while self.notif_rx.try_recv().is_ok() {}
    }

    fn conn_closed(&mut self, conn: ConnId) {
        if let Some(sids) = self.conn_sessions.remove(&conn) {
            for sid in sids {
                capi!(self.api, destroy_session, sid as rs::RimeSessionId);
                self.sessions.remove(&sid);
            }
        }
        let leaked: Vec<u64> = self
            .configs
            .iter()
            .filter(|(_, (c, _))| *c == conn)
            .map(|(h, _)| *h)
            .collect();
        for h in leaked {
            if let Some((_, mut cfg)) = self.configs.remove(&h) {
                capi!(self.api, config_close, &mut cfg);
            }
        }
    }
}

fn op_session_id(op: &Op) -> Option<u64> {
    use Op::*;
    match op {
        FindSession { session }
        | DestroySession { session }
        | ProcessKey { session, .. }
        | SimulateKeySequence { session, .. }
        | CommitComposition { session }
        | ClearComposition { session }
        | GetContext { session }
        | GetCommit { session }
        | GetStatus { session }
        | GetInput { session }
        | SelectCandidateOnCurrentPage { session, .. }
        | GetCandidates { session, .. }
        | GetCurrentSchema { session }
        | SelectSchema { session, .. } => Some(*session),
        _ => None,
    }
}
