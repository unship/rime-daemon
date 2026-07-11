# 设计

范围:让多个 Emacs 实例(liberime/pyim)共享一个 RIME 引擎与一份用户词典。
系统输入法、LSP、引擎重写均为非目标(见 README)。

## 1. 架构与核心不变式

```
emacs #1..N ── liberime-core ── librime.1.dylib(shim,纯 Rust)──unix socket──► rime-cli serve(daemon)
                                                                                  └─ FFI ► 真 librime + userdb
```

- **唯一 librime 宿主**:只有 daemon 链接并调用 C++ librime;shim 与 client 零 librime。
  由此 userdb 单写者、全局单例状态天然收敛在一个进程。
  - 构建层保障:librime 的链接指令放在 `rime-daemon` 自己的 `build.rs`,而不是
    `rime-sys` 的 feature——cargo 整仓构建的 feature 统一会把 feature 传染给 shim
    (实测踩过)。验证:`otool -L` shim 产物只应有系统库。
- **单 worker 线程串行**:daemon 内所有 librime API 调用发生在同一线程。这不是保守,
  是必须:librime `Service::sessions_` 无任何锁。连接 IO(每连接 reader/writer 线程)
  与引擎调用解耦,经 channel 汇聚到 worker。
- **通知线程事实**:librime 的通知回调**不只在调用线程触发**——option/schema/property
  在引擎调用栈内同步触发,deploy/sync 类来自 deployer 线程(`std::async`)。因此回调体
  只投 channel;全局状态只在 worker 线程读写。

## 2. 协议(crates/rime-proto)

- 帧 = `u32` LE 长度前缀 + [postcard](https://docs.rs/postcard) 序列化的 `Frame`。
- **演化规则**:`Op`/`OpResult` 只追加 variant、绝不重排删除(postcard 按序号编码);
  兼容性由 `Hello.proto_ver` 把关,不匹配即拒绝。
- `Response.sync_notifications`:本次调用在 librime 内同步触发的通知,由 shim 在
  **调用者线程、API 返回前**依序回调——对齐原库线程契约(emacs 动态模块 env 不可跨
  线程,这是硬约束)。异步通知(deploy 类)走独立的 `Notify` 帧与 shim 的 pump 线程,
  原库中它们本就来自别的线程。
- `Busy`:daemon 处于 maintenance(含首次部署,可达数十秒)时业务 op 快速失败,
  绝不排队阻塞按键;完成后经 deploy 通知恢复。
- socket:`$XDG_RUNTIME_DIR/rime-daemon/`(Linux)或 `~/Library/Caches/rime-daemon/`
  (macOS)下,文件名 = user_data_dir 规范化路径的 FNV-64;目录 0700、socket 0600
  (键击流是敏感数据)。路径派生必须跨进程稳定,故不用 per-process 的 `$TMPDIR`。

## 3. daemon(crates/rime-daemon)

- 启动次序:拿单例锁 → **先 bind socket**(立即可应答 Hello/Busy)→ worker 线程内
  initialize + `start_maintenance(false)`(有变化才部署)。首个前端不会无解释地卡在
  首次部署上。
- 单例:`<user_data_dir>/.rime-daemon.lock` 上 `flock(LOCK_EX|LOCK_NB)`;持锁者才
  bind。锁与 userdb 必须在本地磁盘(NFS 上 flock 与 LevelDB 均不可靠)。
- session 归属连接:连接断开即销毁其全部 session 与 config 句柄。
- **全局状态模型**:option/schema 全局统一(任一 session 变更即同步到所有 session 并
  广播通知),仅"未上屏输入串"per-session 隔离。同步带白名单:`_` 开头的引擎内部
  option 不参与;apply 造成的连锁通知被抑制(防回环)。新 session 先由 librime 默认
  恢复,再 apply 全局态(以全局态为准)。
- `Shutdown` op:响应后 unlink socket、finalize、退出(供 CLI 与将来的版本接管)。

## 4. shim(crates/rime-shim)

对外导出与原库二进制一致的 `RimeApi` 函数指针表。**"全部转发"不成立**,按处置表分四类:

| 类别 | 例子 | 处置 |
|---|---|---|
| 直接转发 | process_key / get_context / get_commit / get_status / select_candidate_on_current_page / simulate_key_sequence / select_schema / get_schema_list … | RPC 到 daemon;liberime 使用面全保真 |
| 本地虚拟化 | setup / initialize / finalize / set_notification_handler / cleanup_all_sessions | **finalize/cleanup 只作用于本 client**——盲转发会摧毁其他实例的会话 |
| 句柄代理 | schema_open / user_config_open / config_get_* / config_set_* / config_close | daemon 侧句柄表;`config_get_cstring` 的返回指针在 close 前有效(shim 侧按句柄持有副本) |
| 如实拒绝 | sync_user_data(librime 实现会 `CleanupAllSessions`,摧毁所有 client 会话)、register_module、通用 config_open | 返回失败并记日志,绝不假装成功 |

- **未实现槽位保持 NULL**:`RIME_API_AVAILABLE` 语义上等于"此 API 不可用"。注意:
  不检查可用性的调用方会解引用 NULL——接入 liberime 之外的前端前,必须先审计其使用面。
- **虚拟 session id**:shim 对前端发放自己的 id,内部映射 daemon id。daemon 重启后
  映射清空,`find_session` 如实返回 False,liberime 的 `_ensure_session` 自愈重建;
  按键路径上的 `ensure_client` 会自动重连并懒启动新 daemon。
- **`data_size` 精确语义**(自版本化 struct 的回填判定):
  `RIME_STRUCT_HAS_MEMBER ⇔ sizeof(int) + data_size > offsetof(member)`
  ——严格大于、只比 offset、要加 `sizeof(int)`;写成 `offset < data_size` 会差 4 字节,
  边界字段(`commit_text_preview` 等)误判。
- 跨边界内存:shim `malloc` 填充,对应 `free_context`/`free_commit`/`free_status`
  按同样的 data_size 判定释放;`select_labels` 数组带 NULL 哨兵位。
- candidate 迭代器:`candidate_list_from_index/next/end` 在 shim 侧按块(64)拉取快照;
  `iterator.candidate` 的指针在下一次 next/end 前有效,与原库一致。
- 懒启动:找 `rime-cli` 的顺序 = `$RIME_CLI` → shim 自身所在目录(`dladdr`,**绝不能用
  `current_exe`,那是宿主 emacs**)→ `$PATH`。

## 5. 生命周期

- **懒启动**:连不上 → spawn `rime-cli serve --user-data-dir …` → 退避重试(容忍首次
  部署窗口)→ Hello 校验 proto_ver。
- **崩溃恢复**:断连 → 下一次调用重连 + 懒启动;当次调用如实失败,不吞错。未上屏
  输入串不承诺恢复。
- **超时**:client 每次 call 默认 5s 超时;超时/断连/Busy 一律映射为 rime API 的失败
  语义(`False`/空 struct)。
- 尚未实现:空闲退出(daemon 常驻,`rime-cli shutdown` 手动停)、运行时 redeploy/sync
  编排(需要"通知→销毁全部 session→维护→重建→重放"的序列)。

## 6. 测试

- `cargo test`:协议 round-trip。
- `tests/smoke.c`:同一程序分别链接 shim 与真 librime,逐字段对拍(ABI/内存/迭代器/
  config 句柄)。
- 集成清单(手动/脚本):双实例共享学习(A 选词 → B 首选变化)、输入串隔离、kill
  daemon 后一次调用内恢复、单例锁拒绝第二个 daemon、maintenance 期间快速 Busy。
- 运行时判据:`lsof -p <emacs pid> | grep librime` 应只映射 rime-daemon 安装目录下的
  shim 与 liberime-core 模块(brew 安装在 `$(brew --prefix)/opt/rime-daemon/lib/rime-daemon/`);
  出现 `Cellar/librime/…` 即说明仍在直连真 librime。
