//! rime-shim:ABI 兼容原始 librime 的替身 cdylib(纯 Rust,不含 librime)。
//! 处置表(design §6.4):
//! - 直接转发(per-session op)→ RPC 到 daemon,liberime 使用面全保真
//! - 本地虚拟化:setup/initialize/finalize/set_notification_handler —— finalize 只断开自己,
//!   sync_user_data 如实拒绝(会摧毁他人会话),绝不盲转发
//! - 句柄代理:config_*(daemon 侧句柄表)、candidate 迭代器(shim 侧分块快照)
//! - 未实现槽位保持 NULL(RIME_API_AVAILABLE 语义 = 该 API 不可用)
//!
//! 通知线程契约:同步通知(option/schema)经 Response 捎带,在调用者线程、API 返回前回调;
//! 仅 deploy 类走后台 pump 线程(原库中它们本就来自 deployer 线程)。
// unnecessary_cast:`vsession as usize` 在本平台是同类型转换,但 RimeSessionId 由 bindgen
// 生成(uintptr_t),显式转换是对类型漂移的防御,保留。
#![allow(non_snake_case, clippy::missing_safety_doc, clippy::unnecessary_cast)]

use libc::{c_char, c_int, c_void};
use rime_client::{connect_or_spawn, wait_ready, Client};
use rime_proto::{paths, ConfigKind, ContextData, Notify, Op, OpResult, ServerState, PROTO_VER};
use rime_sys as rs;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::mem::offset_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const CALL_TIMEOUT: Duration = Duration::from_secs(5);
const CHUNK: usize = 64;

// ---------------- 全局状态 ----------------

#[derive(Default)]
struct ShimState {
    shared_dir: Option<PathBuf>,
    user_dir: Option<PathBuf>,
    client: Option<Client>,
    /// 虚拟 session id → daemon session id(前端持有的 id 永不直接暴露 daemon id)
    vsessions: HashMap<usize, u64>,
    next_vsession: usize,
    /// config 句柄 → 本 bracket 内 get_cstring 返回的字符串(close 时释放)
    config_strings: HashMap<u64, Vec<CString>>,
    pump_started: bool,
}

static STATE: std::sync::LazyLock<Mutex<ShimState>> = std::sync::LazyLock::new(|| {
    Mutex::new(ShimState { next_vsession: 1, ..Default::default() })
});

struct Handler {
    f: rs::RimeNotificationHandler,
    ctx: *mut c_void,
}
unsafe impl Send for Handler {}

static HANDLER: Mutex<Option<Handler>> = Mutex::new(None);
static SYNC_DIR: OnceLock<CString> = OnceLock::new();
static LOGGED_DEAD: AtomicBool = AtomicBool::new(false);

fn log(msg: &str) {
    eprintln!("[rime-shim] {msg}");
}

// ---------------- client 管理 ----------------

fn find_rime_cli() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("RIME_CLI") {
        let p = PathBuf::from(p);
        if p.exists() {
            return Some(p);
        }
    }
    // shim .dylib 所在目录(dladdr 定位自身,绝不能用 current_exe —— 那是宿主 emacs)
    unsafe {
        let mut info: libc::Dl_info = std::mem::zeroed();
        if libc::dladdr(rime_get_api as *const c_void, &mut info) != 0 && !info.dli_fname.is_null()
        {
            let self_path = PathBuf::from(CStr::from_ptr(info.dli_fname).to_string_lossy().as_ref());
            if let Some(dir) = self_path.parent() {
                let cand = dir.join("rime-cli");
                if cand.exists() {
                    return Some(cand);
                }
            }
        }
    }
    Some(PathBuf::from("rime-cli")) // 交给 PATH
}

/// 确保有活的 client(重连 + 懒启动);调用方持有 STATE 锁
fn ensure_client(st: &mut ShimState) -> Option<Client> {
    if let Some(c) = &st.client {
        if !c.is_dead() {
            return Some(c.clone());
        }
        // 断连:旧 daemon session 全部失效,清空映射;liberime 会经 find_session=False 自愈
        if !LOGGED_DEAD.swap(true, Ordering::SeqCst) {
            log("daemon connection lost; will reconnect lazily");
        }
        st.client = None;
        st.vsessions.clear();
        st.config_strings.clear();
    }
    let user = st.user_dir.clone()?;
    let shared = st.shared_dir.clone();
    let sock = paths::sock_path(&user);
    let cli = find_rime_cli()?;
    let user2 = user.clone();
    let spawn = move || {
        let mut cmd = std::process::Command::new(&cli);
        cmd.arg("serve").arg("--user-data-dir").arg(&user2);
        if let Some(s) = &shared {
            cmd.arg("--shared-data-dir").arg(s);
        }
        cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn()
    };
    match connect_or_spawn(&sock, spawn, Duration::from_secs(20)) {
        Ok(client) => {
            LOGGED_DEAD.store(false, Ordering::SeqCst);
            match client.call_timeout(Op::Hello { proto_ver: PROTO_VER }, CALL_TIMEOUT) {
                Ok(r) => match r.result {
                    OpResult::HelloOk { server_rime_ver, .. } => {
                        log(&format!("connected to rime-daemon (librime {server_rime_ver})"));
                    }
                    other => {
                        log(&format!("hello rejected: {other:?}"));
                        return None;
                    }
                },
                Err(e) => {
                    log(&format!("hello failed: {e}"));
                    return None;
                }
            }
            start_pump(&client, st);
            st.client = Some(client.clone());
            Some(client)
        }
        Err(e) => {
            log(&format!("cannot reach rime-daemon: {e}"));
            None
        }
    }
}

/// deploy 类异步通知 → 后台 pump 线程回调前端 handler(原库中它们本就来自别的线程)
fn start_pump(client: &Client, st: &mut ShimState) {
    let (tx, rx) = std::sync::mpsc::channel::<Notify>();
    client.set_notify_sink(tx);
    if st.pump_started {
        return; // 旧 pump 线程随旧 client 的 sender drop 而退出,新 client 重挂 sink 即可
    }
    st.pump_started = true;
    std::thread::Builder::new()
        .name("rime-shim-pump".into())
        .spawn(move || {
            for n in rx {
                deliver_notification(&n);
            }
        })
        .ok();
}

fn deliver_notification(n: &Notify) {
    let h = HANDLER.lock().unwrap();
    if let Some(Handler { f: Some(f), ctx }) = h.as_ref() {
        let ty = CString::new(n.message_type.as_str()).unwrap_or_default();
        let val = CString::new(n.value.as_str()).unwrap_or_default();
        // session 反向映射到虚拟 id;找不到(如 deploy 的 0)原样传
        let vsid = reverse_vsession(n.session).unwrap_or(n.session as usize);
        unsafe { f(*ctx, vsid as rs::RimeSessionId, ty.as_ptr(), val.as_ptr()) };
    }
}

fn reverse_vsession(daemon_sid: u64) -> Option<usize> {
    let st = STATE.lock().unwrap();
    st.vsessions.iter().find(|(_, d)| **d == daemon_sid).map(|(v, _)| *v)
}

/// 统一调用入口:转发 op,返回结果;同步通知在【调用者线程、返回前】回调(锁外)
fn call(op: Op) -> Option<OpResult> {
    let (result, notifs) = {
        let mut st = STATE.lock().unwrap();
        let client = ensure_client(&mut st)?;
        drop(st); // call 期间不持锁(reader 线程回填;handler 可能重入 shim)
        match client.call_timeout(op, CALL_TIMEOUT) {
            Ok(r) => (r.result, r.sync_notifications),
            Err(_) => return None,
        }
    };
    for n in &notifs {
        deliver_notification(n); // 调用者线程,API 返回前 —— 对齐原库线程契约
    }
    Some(result)
}

fn resolve(vsession: usize) -> Option<u64> {
    STATE.lock().unwrap().vsessions.get(&vsession).copied()
}

fn as_bool(r: Option<OpResult>) -> rs::Bool {
    match r {
        Some(OpResult::Bool(true)) => 1,
        _ => 0, // 断连/超时/Busy/错误一律如实失败,绝不假装成功
    }
}

unsafe fn cstr_arg(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

// ---------------- malloc 工具(跨边界内存:shim 分配,对应 free_* 释放) ----------------

unsafe fn malloc_cstr(s: &str) -> *mut c_char {
    let bytes = s.as_bytes();
    let p = libc::malloc(bytes.len() + 1) as *mut c_char;
    if p.is_null() {
        return p;
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
    *p.add(bytes.len()) = 0;
    p
}

unsafe fn free_cstr(p: *mut c_char) {
    if !p.is_null() {
        libc::free(p as *mut c_void);
    }
}

/// RIME_STRUCT_HAS_MEMBER 精确语义:sizeof(data_size) + data_size > offsetof(member)
/// (严格大于、只比 offset、加 sizeof(int) —— 写成 offset < data_size 就差 4 字节)
fn has_member(data_size: c_int, offset: usize) -> bool {
    std::mem::size_of::<c_int>() + data_size.max(0) as usize > offset
}

// ---------------- API 表 ----------------

static API_TABLE: OnceLock<usize> = OnceLock::new(); // &'static RimeApi as usize(Sync 绕行)

#[no_mangle]
pub extern "C" fn rime_get_api() -> *mut rs::RimeApi {
    let addr = *API_TABLE.get_or_init(|| {
        let mut api: rs::RimeApi = unsafe { std::mem::zeroed() };
        api.data_size =
            (std::mem::size_of::<rs::RimeApi>() - std::mem::size_of::<c_int>()) as c_int;
        api.setup = Some(shim_setup);
        api.set_notification_handler = Some(shim_set_notification_handler);
        api.initialize = Some(shim_initialize);
        api.finalize = Some(shim_finalize);
        api.start_maintenance = Some(shim_start_maintenance);
        api.is_maintenance_mode = Some(shim_is_maintenance_mode);
        api.join_maintenance_thread = Some(shim_join_maintenance_thread);
        api.cleanup_stale_sessions = Some(shim_cleanup_stale_sessions);
        api.cleanup_all_sessions = Some(shim_cleanup_all_sessions);
        api.sync_user_data = Some(shim_sync_user_data);
        api.create_session = Some(shim_create_session);
        api.find_session = Some(shim_find_session);
        api.destroy_session = Some(shim_destroy_session);
        api.process_key = Some(shim_process_key);
        api.commit_composition = Some(shim_commit_composition);
        api.clear_composition = Some(shim_clear_composition);
        api.get_commit = Some(shim_get_commit);
        api.free_commit = Some(shim_free_commit);
        api.get_context = Some(shim_get_context);
        api.free_context = Some(shim_free_context);
        api.get_status = Some(shim_get_status);
        api.free_status = Some(shim_free_status);
        api.get_current_schema = Some(shim_get_current_schema);
        api.get_schema_list = Some(shim_get_schema_list);
        api.free_schema_list = Some(shim_free_schema_list);
        api.select_schema = Some(shim_select_schema);
        api.schema_open = Some(shim_schema_open);
        api.user_config_open = Some(shim_user_config_open);
        api.config_open = Some(shim_config_open);
        api.config_close = Some(shim_config_close);
        api.config_get_bool = Some(shim_config_get_bool);
        api.config_get_int = Some(shim_config_get_int);
        api.config_get_double = Some(shim_config_get_double);
        api.config_get_string = Some(shim_config_get_string);
        api.config_get_cstring = Some(shim_config_get_cstring);
        api.config_set_bool = Some(shim_config_set_bool);
        api.config_set_int = Some(shim_config_set_int);
        api.config_set_double = Some(shim_config_set_double);
        api.config_set_string = Some(shim_config_set_string);
        api.simulate_key_sequence = Some(shim_simulate_key_sequence);
        api.get_input = Some(shim_get_input);
        api.select_candidate_on_current_page = Some(shim_select_candidate_on_current_page);
        api.candidate_list_begin = Some(shim_candidate_list_begin);
        api.candidate_list_from_index = Some(shim_candidate_list_from_index);
        api.candidate_list_next = Some(shim_candidate_list_next);
        api.candidate_list_end = Some(shim_candidate_list_end);
        api.get_sync_dir = Some(shim_get_sync_dir);
        api.get_version = Some(shim_get_version);
        Box::leak(Box::new(api)) as *mut rs::RimeApi as usize
    });
    addr as *mut rs::RimeApi
}

// ---------------- 生命周期(本地虚拟化) ----------------

unsafe fn record_traits(traits: *mut rs::RimeTraits) {
    if traits.is_null() {
        return;
    }
    let t = &*traits;
    let mut st = STATE.lock().unwrap();
    if has_member(t.data_size, offset_of!(rs::RimeTraits, shared_data_dir))
        && !t.shared_data_dir.is_null()
    {
        st.shared_dir = Some(PathBuf::from(cstr_arg(t.shared_data_dir)));
    }
    if has_member(t.data_size, offset_of!(rs::RimeTraits, user_data_dir))
        && !t.user_data_dir.is_null()
    {
        st.user_dir = Some(PathBuf::from(cstr_arg(t.user_data_dir)));
    }
}

unsafe extern "C" fn shim_setup(traits: *mut rs::RimeTraits) {
    record_traits(traits);
}

unsafe extern "C" fn shim_initialize(traits: *mut rs::RimeTraits) {
    record_traits(traits);
    let mut st = STATE.lock().unwrap();
    ensure_client(&mut st);
}

unsafe extern "C" fn shim_finalize() {
    // 本地虚拟化:只断开自己。绝不转发(转发即摧毁其他 client 的会话)。
    let mut st = STATE.lock().unwrap();
    if let (Some(client), sids) = (st.client.clone(), st.vsessions.clone()) {
        for (_, did) in sids {
            let _ = client.call_timeout(Op::DestroySession { session: did }, CALL_TIMEOUT);
        }
    }
    st.client = None;
    st.vsessions.clear();
    st.config_strings.clear();
    log("finalize: disconnected (daemon keeps running for other clients)");
}

unsafe extern "C" fn shim_set_notification_handler(
    handler: rs::RimeNotificationHandler,
    ctx: *mut c_void,
) {
    *HANDLER.lock().unwrap() = Some(Handler { f: handler, ctx });
}

unsafe extern "C" fn shim_start_maintenance(_full_check: rs::Bool) -> rs::Bool {
    // daemon 自己管理部署;client 触发的 redeploy 是 S2。这里确保 daemon 可达即算成功。
    let mut st = STATE.lock().unwrap();
    if ensure_client(&mut st).is_some() {
        1
    } else {
        0
    }
}

unsafe extern "C" fn shim_is_maintenance_mode() -> rs::Bool {
    let mut st = STATE.lock().unwrap();
    let Some(client) = ensure_client(&mut st) else { return 0 };
    drop(st);
    match client.call_timeout(Op::Hello { proto_ver: PROTO_VER }, CALL_TIMEOUT) {
        Ok(r) => match r.result {
            OpResult::HelloOk { state: ServerState::Maintenancing, .. } => 1,
            _ => 0,
        },
        Err(_) => 0,
    }
}

unsafe extern "C" fn shim_join_maintenance_thread() {
    // 语义对齐:阻塞直到部署完成(daemon Ready)。首次部署可达数十秒。
    let client = {
        let mut st = STATE.lock().unwrap();
        ensure_client(&mut st)
    };
    if let Some(client) = client {
        let _ = wait_ready(&client, Duration::from_secs(180));
    }
}

unsafe extern "C" fn shim_cleanup_stale_sessions() {
    // 全局副作用 API:本地无操作(daemon 的 session 生命周期由连接管理)
}

unsafe extern "C" fn shim_cleanup_all_sessions() {
    // 只清理【自己的】session —— 原语义会摧毁其他 client 的会话,绝不转发
    let (client, sids) = {
        let mut st = STATE.lock().unwrap();
        let c = ensure_client(&mut st);
        let sids: Vec<u64> = st.vsessions.values().copied().collect();
        st.vsessions.clear();
        (c, sids)
    };
    if let Some(client) = client {
        for did in sids {
            let _ = client.call_timeout(Op::DestroySession { session: did }, CALL_TIMEOUT);
        }
    }
}

unsafe extern "C" fn shim_sync_user_data() -> rs::Bool {
    // 危险全局 API:librime 的 SyncUserData 会 CleanupAllSessions(摧毁所有 client 会话)。
    // S2 编排完成前如实拒绝,绝不盲转发。
    log("sync_user_data rejected: would destroy all clients' sessions (S2 orchestration TODO)");
    0
}

// ---------------- session / 输入 ----------------

unsafe extern "C" fn shim_create_session() -> rs::RimeSessionId {
    match call(Op::CreateSession) {
        Some(OpResult::Session(did)) => {
            let mut st = STATE.lock().unwrap();
            let v = st.next_vsession;
            st.next_vsession += 1;
            st.vsessions.insert(v, did);
            v as rs::RimeSessionId
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_find_session(vsession: rs::RimeSessionId) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    as_bool(call(Op::FindSession { session: did }))
}

unsafe extern "C" fn shim_destroy_session(vsession: rs::RimeSessionId) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    let r = as_bool(call(Op::DestroySession { session: did }));
    STATE.lock().unwrap().vsessions.remove(&(vsession as usize));
    r
}

unsafe extern "C" fn shim_process_key(
    vsession: rs::RimeSessionId,
    keycode: c_int,
    mask: c_int,
) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    as_bool(call(Op::ProcessKey { session: did, keycode, mask }))
}

unsafe extern "C" fn shim_simulate_key_sequence(
    vsession: rs::RimeSessionId,
    key_sequence: *const c_char,
) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    as_bool(call(Op::SimulateKeySequence { session: did, keys: cstr_arg(key_sequence) }))
}

unsafe extern "C" fn shim_commit_composition(vsession: rs::RimeSessionId) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    as_bool(call(Op::CommitComposition { session: did }))
}

unsafe extern "C" fn shim_clear_composition(vsession: rs::RimeSessionId) {
    if let Some(did) = resolve(vsession as usize) {
        let _ = call(Op::ClearComposition { session: did });
    }
}

unsafe extern "C" fn shim_get_input(vsession: rs::RimeSessionId) -> *const c_char {
    // 原语义:返回 session 内部串的指针。shim 侧缓存一份(下次调用前有效)。
    static INPUT_CACHE: Mutex<Option<CString>> = Mutex::new(None);
    let Some(did) = resolve(vsession as usize) else { return std::ptr::null() };
    match call(Op::GetInput { session: did }) {
        Some(OpResult::Str(Some(s))) => {
            let c = CString::new(s).unwrap_or_default();
            let mut g = INPUT_CACHE.lock().unwrap();
            *g = Some(c);
            g.as_ref().unwrap().as_ptr()
        }
        _ => std::ptr::null(),
    }
}

unsafe extern "C" fn shim_select_candidate_on_current_page(
    vsession: rs::RimeSessionId,
    index: usize,
) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    as_bool(call(Op::SelectCandidateOnCurrentPage { session: did, index }))
}

// ---------------- context / commit / status(data_size 自版本化回填) ----------------

unsafe extern "C" fn shim_get_context(
    vsession: rs::RimeSessionId,
    context: *mut rs::RimeContext,
) -> rs::Bool {
    if context.is_null() {
        return 0;
    }
    let Some(did) = resolve(vsession as usize) else { return 0 };
    let data = match call(Op::GetContext { session: did }) {
        Some(OpResult::Context(d)) => d,
        _ => return 0,
    };
    fill_context(&data, &mut *context);
    1
}

unsafe fn fill_context(data: &ContextData, ctx: &mut rs::RimeContext) {
    let ds = ctx.data_size;
    if has_member(ds, offset_of!(rs::RimeContext, composition)) {
        let c = &mut ctx.composition;
        c.length = data.composition.length;
        c.cursor_pos = data.composition.cursor_pos;
        c.sel_start = data.composition.sel_start;
        c.sel_end = data.composition.sel_end;
        c.preedit = data
            .composition
            .preedit
            .as_deref()
            .map_or(std::ptr::null_mut(), |s| malloc_cstr(s));
    }
    if has_member(ds, offset_of!(rs::RimeContext, menu)) {
        let m = &mut ctx.menu;
        m.page_size = data.menu.page_size;
        m.page_no = data.menu.page_no;
        m.is_last_page = if data.menu.is_last_page { 1 } else { 0 };
        m.highlighted_candidate_index = data.menu.highlighted_candidate_index;
        m.num_candidates = data.menu.candidates.len() as c_int;
        m.select_keys =
            data.menu.select_keys.as_deref().map_or(std::ptr::null_mut(), |s| malloc_cstr(s));
        if data.menu.candidates.is_empty() {
            m.candidates = std::ptr::null_mut();
        } else {
            let n = data.menu.candidates.len();
            let arr = libc::calloc(n, std::mem::size_of::<rs::RimeCandidate>())
                as *mut rs::RimeCandidate;
            for (i, cand) in data.menu.candidates.iter().enumerate() {
                let slot = &mut *arr.add(i);
                slot.text = malloc_cstr(&cand.text);
                slot.comment = cand.comment.as_deref().map_or(std::ptr::null_mut(), |s| {
                    malloc_cstr(s)
                });
                slot.reserved = std::ptr::null_mut();
            }
            m.candidates = arr;
        }
    }
    if has_member(ds, offset_of!(rs::RimeContext, commit_text_preview)) {
        ctx.commit_text_preview = data
            .commit_text_preview
            .as_deref()
            .map_or(std::ptr::null_mut(), |s| malloc_cstr(s));
    }
    if has_member(ds, offset_of!(rs::RimeContext, select_labels)) {
        if data.select_labels.is_empty() {
            ctx.select_labels = std::ptr::null_mut();
        } else {
            // 多分配一个 NULL 哨兵位(calloc 置零),free_context 走到哨兵为止
            let n = data.select_labels.len();
            let arr = libc::calloc(n + 1, std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
            for (i, l) in data.select_labels.iter().enumerate() {
                *arr.add(i) = malloc_cstr(l);
            }
            ctx.select_labels = arr;
        }
    }
}

unsafe extern "C" fn shim_free_context(context: *mut rs::RimeContext) -> rs::Bool {
    if context.is_null() {
        return 0;
    }
    let ctx = &mut *context;
    let ds = ctx.data_size;
    if has_member(ds, offset_of!(rs::RimeContext, composition)) {
        free_cstr(ctx.composition.preedit);
        ctx.composition.preedit = std::ptr::null_mut();
    }
    if has_member(ds, offset_of!(rs::RimeContext, menu)) {
        let m = &mut ctx.menu;
        if !m.candidates.is_null() {
            for i in 0..m.num_candidates.max(0) as usize {
                let slot = &mut *m.candidates.add(i);
                free_cstr(slot.text);
                free_cstr(slot.comment);
            }
            libc::free(m.candidates as *mut c_void);
            m.candidates = std::ptr::null_mut();
        }
        free_cstr(m.select_keys);
        m.select_keys = std::ptr::null_mut();
        m.num_candidates = 0;
    }
    if has_member(ds, offset_of!(rs::RimeContext, commit_text_preview)) {
        free_cstr(ctx.commit_text_preview);
        ctx.commit_text_preview = std::ptr::null_mut();
    }
    if has_member(ds, offset_of!(rs::RimeContext, select_labels)) && !ctx.select_labels.is_null() {
        // fill_context 以 NULL 哨兵结尾分配,这里走到哨兵为止
        let mut p = ctx.select_labels;
        while !(*p).is_null() {
            free_cstr(*p);
            p = p.add(1);
        }
        libc::free(ctx.select_labels as *mut c_void);
        ctx.select_labels = std::ptr::null_mut();
    }
    1
}

unsafe extern "C" fn shim_get_commit(
    vsession: rs::RimeSessionId,
    commit: *mut rs::RimeCommit,
) -> rs::Bool {
    if commit.is_null() {
        return 0;
    }
    let Some(did) = resolve(vsession as usize) else { return 0 };
    match call(Op::GetCommit { session: did }) {
        Some(OpResult::Commit(Some(c))) => {
            let out = &mut *commit;
            if has_member(out.data_size, offset_of!(rs::RimeCommit, text)) {
                out.text = malloc_cstr(&c.text);
            }
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_free_commit(commit: *mut rs::RimeCommit) -> rs::Bool {
    if commit.is_null() {
        return 0;
    }
    let c = &mut *commit;
    if has_member(c.data_size, offset_of!(rs::RimeCommit, text)) {
        free_cstr(c.text);
        c.text = std::ptr::null_mut();
    }
    1
}

unsafe extern "C" fn shim_get_status(
    vsession: rs::RimeSessionId,
    status: *mut rs::RimeStatus,
) -> rs::Bool {
    if status.is_null() {
        return 0;
    }
    let Some(did) = resolve(vsession as usize) else { return 0 };
    let data = match call(Op::GetStatus { session: did }) {
        Some(OpResult::Status(d)) => d,
        _ => return 0,
    };
    let out = &mut *status;
    let ds = out.data_size;
    if has_member(ds, offset_of!(rs::RimeStatus, schema_id)) {
        out.schema_id =
            data.schema_id.as_deref().map_or(std::ptr::null_mut(), |s| malloc_cstr(s));
    }
    if has_member(ds, offset_of!(rs::RimeStatus, schema_name)) {
        out.schema_name =
            data.schema_name.as_deref().map_or(std::ptr::null_mut(), |s| malloc_cstr(s));
    }
    macro_rules! set_flag {
        ($field:ident) => {
            if has_member(ds, offset_of!(rs::RimeStatus, $field)) {
                out.$field = if data.$field { 1 } else { 0 };
            }
        };
    }
    set_flag!(is_disabled);
    set_flag!(is_composing);
    set_flag!(is_ascii_mode);
    set_flag!(is_full_shape);
    set_flag!(is_simplified);
    set_flag!(is_traditional);
    set_flag!(is_ascii_punct);
    1
}

unsafe extern "C" fn shim_free_status(status: *mut rs::RimeStatus) -> rs::Bool {
    if status.is_null() {
        return 0;
    }
    let s = &mut *status;
    if has_member(s.data_size, offset_of!(rs::RimeStatus, schema_id)) {
        free_cstr(s.schema_id);
        s.schema_id = std::ptr::null_mut();
    }
    if has_member(s.data_size, offset_of!(rs::RimeStatus, schema_name)) {
        free_cstr(s.schema_name);
        s.schema_name = std::ptr::null_mut();
    }
    1
}

// ---------------- schema ----------------

unsafe extern "C" fn shim_get_current_schema(
    vsession: rs::RimeSessionId,
    schema_id: *mut c_char,
    buffer_size: usize,
) -> rs::Bool {
    if schema_id.is_null() || buffer_size == 0 {
        return 0;
    }
    let Some(did) = resolve(vsession as usize) else { return 0 };
    match call(Op::GetCurrentSchema { session: did }) {
        Some(OpResult::Str(Some(s))) => {
            let bytes = s.as_bytes();
            let n = bytes.len().min(buffer_size - 1);
            std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, schema_id, n);
            *schema_id.add(n) = 0;
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_get_schema_list(list: *mut rs::RimeSchemaList) -> rs::Bool {
    if list.is_null() {
        return 0;
    }
    match call(Op::GetSchemaList) {
        Some(OpResult::SchemaList(items)) => {
            let out = &mut *list;
            out.size = items.len();
            if items.is_empty() {
                out.list = std::ptr::null_mut();
                return 1;
            }
            let arr = libc::calloc(items.len(), std::mem::size_of::<rs::RimeSchemaListItem>())
                as *mut rs::RimeSchemaListItem;
            for (i, item) in items.iter().enumerate() {
                let slot = &mut *arr.add(i);
                slot.schema_id = malloc_cstr(&item.schema_id);
                slot.name = malloc_cstr(&item.name);
                slot.reserved = std::ptr::null_mut();
            }
            out.list = arr;
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_free_schema_list(list: *mut rs::RimeSchemaList) {
    if list.is_null() {
        return;
    }
    let l = &mut *list;
    if !l.list.is_null() {
        for i in 0..l.size {
            let slot = &mut *l.list.add(i);
            free_cstr(slot.schema_id);
            free_cstr(slot.name);
        }
        libc::free(l.list as *mut c_void);
    }
    l.list = std::ptr::null_mut();
    l.size = 0;
}

unsafe extern "C" fn shim_select_schema(
    vsession: rs::RimeSessionId,
    schema_id: *const c_char,
) -> rs::Bool {
    let Some(did) = resolve(vsession as usize) else { return 0 };
    as_bool(call(Op::SelectSchema { session: did, schema_id: cstr_arg(schema_id) }))
}

// ---------------- config(daemon 侧句柄表代理) ----------------

unsafe fn config_open_impl(kind: ConfigKind, id: *const c_char, config: *mut rs::RimeConfig) -> rs::Bool {
    if config.is_null() {
        return 0;
    }
    match call(Op::ConfigOpen { kind, id: cstr_arg(id) }) {
        Some(OpResult::ConfigHandle(h)) => {
            (*config).ptr = h as usize as *mut c_void;
            STATE.lock().unwrap().config_strings.insert(h, Vec::new());
            1
        }
        _ => 0,
    }
}

unsafe fn config_handle(config: *mut rs::RimeConfig) -> Option<u64> {
    if config.is_null() {
        return None;
    }
    let h = (*config).ptr as usize as u64;
    if h == 0 {
        None
    } else {
        Some(h)
    }
}

unsafe extern "C" fn shim_schema_open(
    schema_id: *const c_char,
    config: *mut rs::RimeConfig,
) -> rs::Bool {
    config_open_impl(ConfigKind::Schema, schema_id, config)
}

unsafe extern "C" fn shim_user_config_open(
    config_id: *const c_char,
    config: *mut rs::RimeConfig,
) -> rs::Bool {
    config_open_impl(ConfigKind::User, config_id, config)
}

unsafe extern "C" fn shim_config_open(
    config_id: *const c_char,
    config: *mut rs::RimeConfig,
) -> rs::Bool {
    // 原 config_open 打开 <id>.yaml(非 user config);daemon 侧走 schema/user 之外的通用打开
    // liberime 不用;为完整性按 user_config 处理会改变语义 —— 如实拒绝。
    let _ = (config_id, config);
    0
}

unsafe extern "C" fn shim_config_close(config: *mut rs::RimeConfig) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    let r = as_bool(call(Op::ConfigClose { handle: h }));
    STATE.lock().unwrap().config_strings.remove(&h);
    (*config).ptr = std::ptr::null_mut();
    r
}

unsafe extern "C" fn shim_config_get_bool(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: *mut rs::Bool,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    if value.is_null() {
        return 0;
    }
    match call(Op::ConfigGetBool { handle: h, key: cstr_arg(key) }) {
        Some(OpResult::CfgBool(Some(v))) => {
            *value = if v { 1 } else { 0 };
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_config_get_int(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: *mut c_int,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    if value.is_null() {
        return 0;
    }
    match call(Op::ConfigGetInt { handle: h, key: cstr_arg(key) }) {
        Some(OpResult::CfgInt(Some(v))) => {
            *value = v;
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_config_get_double(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: *mut f64,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    if value.is_null() {
        return 0;
    }
    match call(Op::ConfigGetDouble { handle: h, key: cstr_arg(key) }) {
        Some(OpResult::CfgDouble(Some(v))) => {
            *value = v;
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_config_get_string(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: *mut c_char,
    buffer_size: usize,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    if value.is_null() || buffer_size == 0 {
        return 0;
    }
    match call(Op::ConfigGetString { handle: h, key: cstr_arg(key) }) {
        Some(OpResult::Str(Some(s))) => {
            let bytes = s.as_bytes();
            let n = bytes.len().min(buffer_size - 1);
            std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, value, n);
            *value.add(n) = 0;
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn shim_config_get_cstring(
    config: *mut rs::RimeConfig,
    key: *const c_char,
) -> *const c_char {
    // 原语义:返回 config 内部字符串指针,close 前有效 → shim 把副本挂在句柄上,close 时释放
    let Some(h) = config_handle(config) else { return std::ptr::null() };
    match call(Op::ConfigGetString { handle: h, key: cstr_arg(key) }) {
        Some(OpResult::Str(Some(s))) => {
            let c = CString::new(s).unwrap_or_default();
            let mut st = STATE.lock().unwrap();
            let v = st.config_strings.entry(h).or_default();
            v.push(c);
            v.last().unwrap().as_ptr()
        }
        _ => std::ptr::null(),
    }
}

unsafe extern "C" fn shim_config_set_bool(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: rs::Bool,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    as_bool(call(Op::ConfigSetBool { handle: h, key: cstr_arg(key), value: value != 0 }))
}

unsafe extern "C" fn shim_config_set_int(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: c_int,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    as_bool(call(Op::ConfigSetInt { handle: h, key: cstr_arg(key), value }))
}

unsafe extern "C" fn shim_config_set_double(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: f64,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    as_bool(call(Op::ConfigSetDouble { handle: h, key: cstr_arg(key), value }))
}

unsafe extern "C" fn shim_config_set_string(
    config: *mut rs::RimeConfig,
    key: *const c_char,
    value: *const c_char,
) -> rs::Bool {
    let Some(h) = config_handle(config) else { return 0 };
    as_bool(call(Op::ConfigSetString {
        handle: h,
        key: cstr_arg(key),
        value: cstr_arg(value),
    }))
}

// ---------------- candidate 迭代器(shim 侧分块快照) ----------------

struct IterState {
    session: u64,
    /// 当前块;text/comment 的 CString 由本结构持有,指针在下一次 next/end 前有效
    chunk: Vec<(CString, Option<CString>)>,
    pos_in_chunk: usize,
    next_global_index: usize,
    exhausted: bool,
}

unsafe extern "C" fn shim_candidate_list_begin(
    vsession: rs::RimeSessionId,
    iterator: *mut rs::RimeCandidateListIterator,
) -> rs::Bool {
    shim_candidate_list_from_index(vsession, iterator, 0)
}

unsafe extern "C" fn shim_candidate_list_from_index(
    vsession: rs::RimeSessionId,
    iterator: *mut rs::RimeCandidateListIterator,
    index: c_int,
) -> rs::Bool {
    if iterator.is_null() {
        return 0;
    }
    let Some(did) = resolve(vsession as usize) else { return 0 };
    let start = index.max(0) as usize;
    let it = &mut *iterator;
    it.ptr = Box::into_raw(Box::new(IterState {
        session: did,
        chunk: Vec::new(),
        pos_in_chunk: 0,
        next_global_index: start,
        exhausted: false,
    })) as *mut c_void;
    it.index = index - 1; // next() 之后指向当前候选,先退一格
    it.candidate.text = std::ptr::null_mut();
    it.candidate.comment = std::ptr::null_mut();
    it.candidate.reserved = std::ptr::null_mut();
    1
}

unsafe extern "C" fn shim_candidate_list_next(
    iterator: *mut rs::RimeCandidateListIterator,
) -> rs::Bool {
    if iterator.is_null() || (*iterator).ptr.is_null() {
        return 0;
    }
    let it = &mut *iterator;
    let state = &mut *(it.ptr as *mut IterState);
    if state.pos_in_chunk >= state.chunk.len() {
        if state.exhausted {
            return 0;
        }
        // 拉下一块
        match call(Op::GetCandidates {
            session: state.session,
            index: state.next_global_index,
            count: CHUNK,
        }) {
            Some(OpResult::Candidates { list, exhausted }) => {
                state.exhausted = exhausted;
                state.chunk = list
                    .into_iter()
                    .map(|c| {
                        (
                            CString::new(c.text).unwrap_or_default(),
                            c.comment.map(|s| CString::new(s).unwrap_or_default()),
                        )
                    })
                    .collect();
                state.pos_in_chunk = 0;
                if state.chunk.is_empty() {
                    return 0;
                }
            }
            _ => return 0,
        }
    }
    let (text, comment) = &state.chunk[state.pos_in_chunk];
    it.candidate.text = text.as_ptr() as *mut c_char;
    it.candidate.comment =
        comment.as_ref().map_or(std::ptr::null_mut(), |c| c.as_ptr() as *mut c_char);
    it.index += 1;
    state.pos_in_chunk += 1;
    state.next_global_index += 1;
    1
}

unsafe extern "C" fn shim_candidate_list_end(iterator: *mut rs::RimeCandidateListIterator) {
    if iterator.is_null() || (*iterator).ptr.is_null() {
        return;
    }
    let it = &mut *iterator;
    drop(Box::from_raw(it.ptr as *mut IterState));
    it.ptr = std::ptr::null_mut();
    it.candidate.text = std::ptr::null_mut();
    it.candidate.comment = std::ptr::null_mut();
}

// ---------------- 杂项 ----------------

unsafe extern "C" fn shim_get_sync_dir() -> *const c_char {
    if let Some(c) = SYNC_DIR.get() {
        return c.as_ptr();
    }
    match call(Op::GetSyncDir) {
        Some(OpResult::Str(Some(s))) => {
            let _ = SYNC_DIR.set(CString::new(s).unwrap_or_default());
            SYNC_DIR.get().map_or(std::ptr::null(), |c| c.as_ptr())
        }
        _ => std::ptr::null(),
    }
}

unsafe extern "C" fn shim_get_version() -> *const c_char {
    // 失败不缓存:initialize 前调用(拿不到 daemon)返回 fallback,之后再调仍可拿到真实版本
    static VERSION: Mutex<Option<CString>> = Mutex::new(None);
    static FALLBACK: &[u8] = b"0.0.0-rime-shim\0";
    let mut g = VERSION.lock().unwrap();
    if let Some(v) = g.as_ref() {
        return v.as_ptr();
    }
    let client = {
        let mut st = STATE.lock().unwrap();
        ensure_client(&mut st)
    };
    if let Some(client) = client {
        if let Ok(r) = client.call_timeout(Op::Hello { proto_ver: PROTO_VER }, CALL_TIMEOUT) {
            if let OpResult::HelloOk { server_rime_ver, .. } = r.result {
                *g = Some(CString::new(server_rime_ver).unwrap_or_default());
                return g.as_ref().unwrap().as_ptr();
            }
        }
    }
    FALLBACK.as_ptr() as *const c_char
}
