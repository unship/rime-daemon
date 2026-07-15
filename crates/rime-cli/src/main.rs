//! rime-cli:serve(daemon 本体)/ probe(测试 client)/ shutdown。
//! 手写参数解析,不引 clap(依赖最小化)。
use rime_client::{connect_or_spawn, wait_ready, Client};
use rime_proto::{paths, Op, OpResult, PROTO_VER};
use std::path::PathBuf;
use std::time::Duration;

/// 平台默认 shared_data_dir 候选(按序取第一个存在的;--shared-data-dir 显式覆盖)
const SHARED_DATA_CANDIDATES: &[&str] = &[
    "/Library/Input Methods/Squirrel.app/Contents/SharedSupport", // macOS(鼠须管数据)
    "/usr/share/rime-data",                                       // Linux 发行版
    "/opt/homebrew/share/rime-data",
    "/usr/local/share/rime-data",
];

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("");
    let rc = match cmd {
        "serve" => cmd_serve(&args[1..]),
        "probe" => cmd_probe(&args[1..]),
        "sync" => cmd_sync(&args[1..]),
        "shutdown" => cmd_shutdown(&args[1..]),
        _ => {
            eprintln!(
                "usage: rime-cli serve    --user-data-dir D [--shared-data-dir S]\n\
                        rime-cli probe    --user-data-dir D --keys nihao [--select N] [--all N] [--spawn]\n\
                        rime-cli sync     --user-data-dir D\n\
                        rime-cli shutdown --user-data-dir D"
            );
            2
        }
    };
    std::process::exit(rc);
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn user_dir(args: &[String]) -> PathBuf {
    let d = flag(args, "--user-data-dir").unwrap_or_else(|| {
        eprintln!("--user-data-dir is required");
        std::process::exit(2);
    });
    PathBuf::from(shellexpand_home(&d))
}

fn shellexpand_home(p: &str) -> String {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    p.to_string()
}

fn cmd_serve(args: &[String]) -> i32 {
    let user = user_dir(args);
    let shared = match flag(args, "--shared-data-dir") {
        Some(s) => PathBuf::from(shellexpand_home(&s)),
        None => match SHARED_DATA_CANDIDATES
            .iter()
            .map(PathBuf::from)
            .find(|p| p.exists())
        {
            Some(p) => p,
            None => {
                eprintln!("no rime data dir found; pass --shared-data-dir");
                return 1;
            }
        },
    };
    if !shared.exists() {
        eprintln!("shared data dir not found: {}", shared.display());
        return 1;
    }
    match rime_daemon::serve(rime_daemon::ServeOpts {
        user_data_dir: user,
        shared_data_dir: shared,
    }) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("serve failed: {e}");
            1
        }
    }
}

fn connect(args: &[String], spawn_ok: bool) -> Result<Client, String> {
    let user = user_dir(args);
    let sock = paths::sock_path(&user);
    let client = if spawn_ok && has_flag(args, "--spawn") {
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let user2 = user.clone();
        connect_or_spawn(
            &sock,
            move || {
                std::process::Command::new(&exe)
                    .arg("serve")
                    .arg("--user-data-dir")
                    .arg(&user2)
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()
            },
            Duration::from_secs(10),
        )
        .map_err(|e| format!("connect_or_spawn: {e}"))?
    } else {
        Client::connect(&sock).map_err(|e| format!("connect {}: {e}", sock.display()))?
    };
    let hello = client
        .call(Op::Hello {
            proto_ver: PROTO_VER,
        })
        .map_err(|e| format!("hello: {e}"))?;
    match hello.result {
        OpResult::HelloOk {
            server_rime_ver,
            state,
            ..
        } => {
            eprintln!("[probe] connected: rime {server_rime_ver}, state={state:?}");
        }
        other => return Err(format!("unexpected hello result: {other:?}")),
    }
    Ok(client)
}

fn cmd_probe(args: &[String]) -> i32 {
    let keys = flag(args, "--keys").unwrap_or_else(|| "nihao".into());
    let client = match connect(args, true) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[probe] {e}");
            return 1;
        }
    };
    if wait_ready(&client, Duration::from_secs(120)).is_err() {
        eprintln!("[probe] daemon not ready (maintenance timeout)");
        return 1;
    }
    let sid = match client.call(Op::CreateSession) {
        Ok(r) => match r.result {
            OpResult::Session(s) => s,
            other => {
                eprintln!("[probe] create session: {other:?}");
                return 1;
            }
        },
        Err(e) => {
            eprintln!("[probe] create session: {e}");
            return 1;
        }
    };
    eprintln!("[probe] session={sid}");

    let _ = client.call(Op::SimulateKeySequence {
        session: sid,
        keys: keys.clone(),
    });
    match client.call(Op::GetContext { session: sid }) {
        Ok(r) => match r.result {
            OpResult::Context(ctx) => {
                println!(
                    "preedit: {}",
                    ctx.composition.preedit.as_deref().unwrap_or("")
                );
                for (i, c) in ctx.menu.candidates.iter().enumerate() {
                    println!(
                        "{}. {}{}",
                        i + 1,
                        c.text,
                        c.comment
                            .as_deref()
                            .map(|s| format!(" ({s})"))
                            .unwrap_or_default()
                    );
                }
            }
            other => eprintln!("[probe] get_context: {other:?}"),
        },
        Err(e) => eprintln!("[probe] get_context: {e}"),
    }

    if let Some(n) = flag(args, "--all") {
        let n: usize = n.parse().unwrap_or(20);
        match client.call(Op::GetCandidates {
            session: sid,
            index: 0,
            count: n,
        }) {
            Ok(r) => {
                if let OpResult::Candidates { list, exhausted } = r.result {
                    println!(
                        "-- all candidates (n={} exhausted={exhausted}) --",
                        list.len()
                    );
                    for (i, c) in list.iter().enumerate() {
                        println!("{}. {}", i + 1, c.text);
                    }
                }
            }
            Err(e) => eprintln!("[probe] get_candidates: {e}"),
        }
    }

    if let Some(sel) = flag(args, "--select") {
        let idx: usize = sel.parse().unwrap_or(0);
        let _ = client.call(Op::SelectCandidateOnCurrentPage {
            session: sid,
            index: idx,
        });
        // 选完可能直接上屏,取 commit
        match client.call(Op::GetCommit { session: sid }) {
            Ok(r) => {
                if let OpResult::Commit(Some(c)) = r.result {
                    println!("commit: {}", c.text);
                } else {
                    // 未上屏则手动 commit
                    let _ = client.call(Op::CommitComposition { session: sid });
                    if let Ok(r2) = client.call(Op::GetCommit { session: sid }) {
                        if let OpResult::Commit(Some(c)) = r2.result {
                            println!("commit: {}", c.text);
                        }
                    }
                }
            }
            Err(e) => eprintln!("[probe] get_commit: {e}"),
        }
    }

    let _ = client.call(Op::DestroySession { session: sid });
    0
}

fn cmd_sync(args: &[String]) -> i32 {
    match connect(args, false) {
        Ok(client) => match client.call(Op::SyncUserData) {
            Ok(r) => match r.result {
                OpResult::Bool(true) => {
                    eprintln!("[sync] scheduled; daemon will run it when all sessions are idle");
                    0
                }
                other => {
                    eprintln!("[sync] rejected: {other:?}");
                    1
                }
            },
            Err(e) => {
                eprintln!("[sync] {e}");
                1
            }
        },
        Err(e) => {
            eprintln!("[sync] {e}");
            1
        }
    }
}

fn cmd_shutdown(args: &[String]) -> i32 {
    match connect(args, false) {
        Ok(client) => match client.call(Op::Shutdown) {
            Ok(_) => {
                eprintln!("[shutdown] ok");
                0
            }
            Err(e) => {
                eprintln!("[shutdown] {e}");
                1
            }
        },
        Err(e) => {
            eprintln!("[shutdown] {e}");
            1
        }
    }
}
