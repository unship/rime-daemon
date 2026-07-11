# rime-daemon

Share one [RIME](https://rime.im) engine and one user dictionary across multiple
Emacs instances, by replacing the librime your Emacs loads with an ABI-compatible
shim that talks to a single daemon.

让多个 Emacs 实例(GUI + 各终端)共享**同一个 RIME 引擎、同一份用户词典**。

## 问题

[librime](https://github.com/rime/librime) 的用户词典(userdb)是 LevelDB,
**同一时刻只允许一个进程打开**,而输入法的"学习"是持续写入。多个 Emacs 实例各自经
[liberime](https://github.com/merrickluo/liberime) 内嵌 librime 时只有两个坏选项:
指向同一份 userdb(锁冲突、误判恢复损坏),或各用一份(学习不互通)。

## 方案

一个 daemon 进程独占 librime 与 userdb;Emacs 侧把 liberime 所链接的 librime 换成
**ABI 兼容的替身(shim,纯 Rust,不含任何 librime 代码)**,所有调用经 Unix socket
转发给 daemon:

```
emacs #1 (pyim / liberime) ──┐
emacs #2 (pyim / liberime) ──┼── librime.1.dylib(shim)──unix socket──► rime-daemon ──► librime + userdb
emacs #N ────────────────────┘                                          (唯一写者;插件在此加载)
```

- **学习实时共享**:任一实例学到的词,其他实例下一次输入立即可见
- **输入串隔离**:某实例打到一半的拼音不会串到别的实例;option/schema 状态全局统一
- **崩溃恢复**:daemon 被 kill 后,实例的下一次调用自动重连并拉起新 daemon;学习状态在
  userdb 中持久保留
- **懒启动**与以 user_data_dir 为锚点的**单例**;按键路径带超时,任何故障都不会冻结
  Emacs UI(如实返回失败,而非挂起)
- librime 的 C++ 插件(librime-lua / octagram / predict)由 daemon 侧的真 librime
  加载,效果对所有实例生效

## 状态

实验性(0.1)。macOS 上端到端验证:双 Emacs 实例(pyim + liberime)共享学习、输入串
隔离、崩溃恢复,以及 shim 与真 librime 的 C ABI 逐字段对拍(`tests/smoke.c`)。
Linux 未验证(代码无平台特定依赖,`RIME_INCLUDE_DIR`/`RIME_LIB_DIR` 可指向发行版
librime;`user_data_dir` 必须在本地磁盘,LevelDB 与 flock 均不支持 NFS)。

**非目标**:不替换系统输入法(鼠须管/fcitx5 继续用自带 librime 与独立词典;与它们的
词典互通请用 rime 自带的 sync,最终一致);不重写引擎;不提供 LSP。

## 安装(macOS,Homebrew)

```sh
brew tap unship/rime
brew install rime-daemon        # 最新开发版:brew install --HEAD rime-daemon
```

装好即得:PATH 上的 `rime-cli`、shim、以及**对着 shim 编译好的 liberime Emacs 模块**
(全部在稳定的 `$(brew --prefix)/opt/rime-daemon/` 路径下)。Emacs 侧只需两行配置指向
brew 路径 —— 见 **[docs/emacs-integration.md](docs/emacs-integration.md)**。
formula 维护在 [unship/homebrew-rime](https://github.com/unship/homebrew-rime)。

验证 daemon 链路:

```sh
rime-cli probe --user-data-dir ~/rime-test --keys nihao --spawn
#  preedit: ni hao
#  1. 你好 …
```

从源码开发:`cargo build --release`;组装本地 dist 用 `scripts/make-dist.sh`。

## 文档

- [docs/design.md](docs/design.md) — 架构、线程模型、协议、shim 处置表、生命周期、已知限制
- [docs/emacs-integration.md](docs/emacs-integration.md) — Emacs / Doom Emacs / pyim 接入与回退

## License

[BSD-3-Clause](LICENSE)。`rime_api.h` 的 ABI 契约版权归 RIME 项目(BSD)。
