# Emacs 接入

## 安装(Homebrew)

Homebrew 要求 formula 位于 tap 中:

```sh
brew tap-new "$USER/local" 2>/dev/null   # 一次性:建本地 tap
cp Formula/rime-daemon.rb "$(brew --repository "$USER/local")/Formula/"
brew install --HEAD "$USER/local/rime-daemon"
```

安装产物(`$(brew --prefix)` 在 Apple Silicon 上是 `/opt/homebrew`):

| 路径 | 内容 |
|---|---|
| `$(brew --prefix)/bin/rime-cli` | daemon 本体 + probe/shutdown 子命令(已在 PATH) |
| `$(brew --prefix)/opt/rime-daemon/lib/rime-daemon/librime.1.dylib` | shim(ABI 兼容 librime 的纯 Rust 替身) |
| `$(brew --prefix)/opt/rime-daemon/lib/rime-daemon/liberime-core.dylib` | 对着 shim 编译好的 liberime Emacs 模块 |

shim 按需自动拉起 daemon(`rime-cli` 在 PATH 即可),无需环境变量、无需手动起服务。

## Emacs 配置

elisp 侧照常安装 [pyim](https://github.com/tumashu/pyim) 与
[liberime](https://github.com/emacs-rime/liberime)(MELPA/straight);要做的只是让
liberime 加载本包的模块而不是自编模块。

### 普通 Emacs

```elisp
;; liberime-auto-build 必须为 nil:否则模块文件不可用时会静默自编一个
;; 直连真 librime 的模块 —— 输入照常但共享全部失效,且不会报错。
(setq liberime-auto-build nil
      liberime-module-file
      "/opt/homebrew/opt/rime-daemon/lib/rime-daemon/liberime-core.dylib"
      ;; userdb 所在目录;必须在本地磁盘(LevelDB/flock 不支持 NFS)
      liberime-user-data-dir (expand-file-name "~/.local/share/emacs-rime"))
(require 'liberime)
(setq pyim-default-scheme 'rime)
```

### Doom Emacs(`:input chinese +rime`)

Doom 的 chinese 模块会在 `:init` 里覆盖这些变量,用 advice 保证在引擎启动前生效:

```elisp
(define-advice liberime-load (:before (&rest _) rime-daemon)
  (setq liberime-auto-build nil
        liberime-module-file
        "/opt/homebrew/opt/rime-daemon/lib/rime-daemon/liberime-core.dylib"
        liberime-user-data-dir (expand-file-name "~/.local/share/emacs-rime")))
```

### 切换须知

- 改完配置后**重启所有 Emacs 实例**:旧实例内嵌的真 librime 仍锁着 userdb,不重启则
  daemon 打不开该词典(LevelDB 单写者)。
- 首次经 daemon 使用某个 user dir 时会跑一次增量部署(有变更才做);期间按键快速返回
  失败,部署完成后自动恢复,无需重启。

## 验证

```sh
# daemon 链路(--spawn 会按需拉起 daemon)
rime-cli probe --user-data-dir ~/.local/share/emacs-rime --keys nihao --spawn
```

判定 Emacs 是否真的走了 shim(终极判据):

```sh
lsof -p <emacs 的 pid> | grep librime
# 只应出现 …/rime-daemon/librime.1.dylib 与 liberime-core.dylib;
# 出现 …/Cellar/librime/… 说明还在直连真 librime(检查 liberime-module-file 是否生效)
```

行为判据:多个 Emacs 实例同时输入正常、任一实例学的词在其他实例立即可见、
打字时存在 `rime-cli serve` 进程。daemon 日志在 `<user_data_dir>/log/`。

## 回退

删除上面的 `liberime-module-file` / `liberime-auto-build` 设置,重启 Emacs,即恢复
liberime 自编模块直连 librime 的原始方式。userdb 内容不受影响。

## 已知限制

- `liberime-sync-user-data` 如实返回失败(librime 的 sync 会销毁所有实例的会话,
  跨实例编排尚未实现)。
- daemon 不自动退出;需要时 `rime-cli shutdown --user-data-dir <dir>`。
- 仅覆盖 liberime 的 API 使用面;其他 librime 前端接入前需先审计其 API 面
  (未实现的 API 槽位为 NULL)。
