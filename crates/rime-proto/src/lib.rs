//! 内部 RPC 协议:Frame/Op/OpResult + postcard 编解码。
//! 演化规则:Op/OpResult 枚举只追加 variant、绝不重排或删除(postcard 按序号编码);
//! 跨版本兼容由 Hello.proto_ver 把关。
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

/// v2:追加 Op::SyncUserData(异步 sync)。Hello 要求精确匹配;
/// 新 client 遇到旧 daemon 会发 Shutdown 接管(Shutdown 的 variant 序号跨版本稳定)。
pub const PROTO_VER: u32 = 2;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Frame {
    Request(Request),
    Response(Response),
    Notify(Notify),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Request {
    pub id: u64,
    pub op: Op,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ConfigKind {
    Schema,
    User,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Op {
    Hello { proto_ver: u32 },
    CreateSession,
    FindSession { session: u64 },
    DestroySession { session: u64 },
    ProcessKey { session: u64, keycode: i32, mask: i32 },
    SimulateKeySequence { session: u64, keys: String },
    CommitComposition { session: u64 },
    ClearComposition { session: u64 },
    GetContext { session: u64 },
    GetCommit { session: u64 },
    GetStatus { session: u64 },
    GetInput { session: u64 },
    SelectCandidateOnCurrentPage { session: u64, index: usize },
    /// 候选分块快照:从全局 index 起最多 count 个(shim 迭代器按块拉取)
    GetCandidates { session: u64, index: usize, count: usize },
    GetSchemaList,
    GetCurrentSchema { session: u64 },
    SelectSchema { session: u64, schema_id: String },
    GetSyncDir,
    ConfigOpen { kind: ConfigKind, id: String },
    ConfigClose { handle: u64 },
    ConfigGetBool { handle: u64, key: String },
    ConfigGetInt { handle: u64, key: String },
    ConfigGetDouble { handle: u64, key: String },
    ConfigGetString { handle: u64, key: String },
    ConfigSetBool { handle: u64, key: String, value: bool },
    ConfigSetInt { handle: u64, key: String, value: i32 },
    ConfigSetDouble { handle: u64, key: String, value: f64 },
    ConfigSetString { handle: u64, key: String, value: String },
    /// 版本接管/手动停止:daemon 停止 accept、drain、退出
    Shutdown,
    /// (v2)异步 sync:daemon 记住请求,等所有 session 空闲(无 composition、短暂无按键)
    /// 后执行 librime 的 sync_user_data(它会销毁全部 session;客户端经 find_session 自愈)。
    /// 立即回 Bool(true) 表示已排队;完成/失败经 deploy 通知广播。
    SyncUserData,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ServerState {
    Ready,
    Maintenancing,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum OpResult {
    HelloOk { server_rime_ver: String, proto_ver: u32, state: ServerState },
    Session(u64),
    Bool(bool),
    Context(Box<ContextData>),
    Commit(Option<CommitData>),
    Status(Box<StatusData>),
    Str(Option<String>),
    Candidates { list: Vec<CandidateData>, exhausted: bool },
    SchemaList(Vec<SchemaItem>),
    ConfigHandle(u64),
    CfgBool(Option<bool>),
    CfgInt(Option<i32>),
    CfgDouble(Option<f64>),
    /// maintenance 中(含首次部署):快速失败,勿排队阻塞
    Busy,
    Error(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub id: u64,
    pub result: OpResult,
    /// 本次调用在 librime 内同步触发的通知(option/schema 类)。
    /// shim 必须在调用者线程、API 返回前依序回调 —— 对齐原库线程契约。
    pub sync_notifications: Vec<Notify>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Notify {
    pub session: u64,
    pub message_type: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CandidateData {
    pub text: String,
    pub comment: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CompositionData {
    pub length: i32,
    pub cursor_pos: i32,
    pub sel_start: i32,
    pub sel_end: i32,
    pub preedit: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct MenuData {
    pub page_size: i32,
    pub page_no: i32,
    pub is_last_page: bool,
    pub highlighted_candidate_index: i32,
    pub num_candidates: i32,
    pub candidates: Vec<CandidateData>,
    pub select_keys: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ContextData {
    pub commit_text_preview: Option<String>,
    pub composition: CompositionData,
    pub menu: MenuData,
    pub select_labels: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CommitData {
    pub text: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StatusData {
    pub schema_id: Option<String>,
    pub schema_name: Option<String>,
    pub is_disabled: bool,
    pub is_composing: bool,
    pub is_ascii_mode: bool,
    pub is_full_shape: bool,
    pub is_simplified: bool,
    pub is_traditional: bool,
    pub is_ascii_punct: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SchemaItem {
    pub schema_id: String,
    pub name: String,
}

/// 每帧 = u32 LE 长度前缀 + postcard 序列化的 Frame
pub fn write_frame<W: Write>(w: &mut W, frame: &Frame) -> io::Result<()> {
    let bytes = postcard::to_allocvec(frame)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(&bytes)?;
    w.flush()
}

pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Frame> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    postcard::from_bytes(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// socket 路径派生(daemon 与 client 必须一致,不能依赖 per-process 的 TMPDIR)。
/// 优先级:$RIME_DAEMON_SOCK_DIR → $XDG_RUNTIME_DIR/rime-daemon(Linux)
///        → ~/Library/Caches/rime-daemon(macOS,本地盘且跨进程稳定)→ /tmp/rime-daemon-$UID
pub mod paths {
    use std::path::{Path, PathBuf};

    pub fn runtime_dir() -> PathBuf {
        if let Ok(d) = std::env::var("RIME_DAEMON_SOCK_DIR") {
            return PathBuf::from(d);
        }
        if let Ok(d) = std::env::var("XDG_RUNTIME_DIR") {
            return Path::new(&d).join("rime-daemon");
        }
        #[cfg(target_os = "macos")]
        if let Ok(home) = std::env::var("HOME") {
            return Path::new(&home).join("Library/Caches/rime-daemon");
        }
        let user = std::env::var("USER").unwrap_or_else(|_| "unknown".into());
        PathBuf::from(format!("/tmp/rime-daemon-{user}"))
    }

    /// FNV-1a 64,对 canonical 化的 user_data_dir 取哈希作 socket 文件名
    pub fn sock_path(user_data_dir: &Path) -> PathBuf {
        let canon = user_data_dir
            .canonicalize()
            .unwrap_or_else(|_| user_data_dir.to_path_buf());
        let mut h: u64 = 0xcbf29ce484222325;
        for b in canon.to_string_lossy().as_bytes() {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        runtime_dir().join(format!("{h:016x}.sock"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = Frame::Request(Request {
            id: 7,
            op: Op::ProcessKey { session: 42, keycode: 0x61, mask: 0 },
        });
        let mut buf = Vec::new();
        write_frame(&mut buf, &f).unwrap();
        let g = read_frame(&mut &buf[..]).unwrap();
        match g {
            Frame::Request(Request { id: 7, op: Op::ProcessKey { session: 42, keycode: 0x61, mask: 0 } }) => {}
            other => panic!("mismatch: {other:?}"),
        }
    }
}
