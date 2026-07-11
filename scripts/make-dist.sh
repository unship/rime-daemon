#!/usr/bin/env bash
# 开发用:组装 dist/(与 Homebrew formula 同配方)。
#   dist/librime.1.dylib    shim(LC_ID = 绝对路径)
#   dist/liberime-core.dylib 从 liberime 源码对着 shim 直接编译(免 retarget)
#   dist/rime-cli
#
# 用法:
#   scripts/make-dist.sh --liberime-src /path/to/liberime(git checkout)
#
# 正式安装请用 Homebrew formula(Formula/rime-daemon.rb),路径更稳定。
set -euo pipefail

if [[ "$(uname)" != "Darwin" ]]; then
  echo "make-dist.sh currently supports macOS only" >&2
  exit 1
fi

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist"
LIBERIME_SRC=""
RIME_INCLUDE="${RIME_INCLUDE_DIR:-$(brew --prefix 2>/dev/null || echo /opt/homebrew)/include}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --liberime-src) LIBERIME_SRC="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$LIBERIME_SRC" || ! -f "$LIBERIME_SRC/src/liberime-core.c" ]]; then
  echo "required: --liberime-src /path/to/liberime (git clone of emacs-rime/liberime)" >&2
  exit 2
fi

cargo build --release --manifest-path "$ROOT/Cargo.toml"

mkdir -p "$DIST"
# 必须先删再拷:macOS 按 vnode 缓存代码签名,原地覆盖已签名二进制会被 SIGKILL
rm -f "$DIST/librime.1.dylib" "$DIST/liberime-core.dylib" "$DIST/rime-cli"

cp "$ROOT/target/release/librime_shim.dylib" "$DIST/librime.1.dylib"
install_name_tool -id "$DIST/librime.1.dylib" "$DIST/librime.1.dylib"

# 选 liberime vendor 的最高版本 emacs-module.h(模块 ABI 向后兼容)
EMACS_MODULE_DIR="$(ls -d "$LIBERIME_SRC"/emacs-module/*/ | sort -t/ -k"$(echo "$LIBERIME_SRC" | awk -F/ '{print NF+2}')" -n | tail -1)"
cc -shared -fPIC -O2 -Wall -DHAVE_RIME_API \
  -I "$LIBERIME_SRC/src" -I "$EMACS_MODULE_DIR" -I "$RIME_INCLUDE" \
  "$LIBERIME_SRC"/src/*.c "$DIST/librime.1.dylib" \
  -o "$DIST/liberime-core.dylib"

cp "$ROOT/target/release/rime-cli" "$DIST/rime-cli"
codesign -f -s - "$DIST/librime.1.dylib" "$DIST/liberime-core.dylib" "$DIST/rime-cli"

echo "== dist assembled =="
echo "module deps (should be the shim + libSystem only):"
otool -L "$DIST/liberime-core.dylib" | sed -n '2,4p'
