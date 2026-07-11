# Homebrew formula. 安装(Homebrew 要求 formula 在 tap 中):
#   brew tap-new "$USER/local"
#   cp Formula/rime-daemon.rb "$(brew --repository "$USER/local")/Formula/"
#   brew install --HEAD "$USER/local/rime-daemon"
# 正式分发建议另建 homebrew-<name> tap 仓库。
class RimeDaemon < Formula
  desc "Share one RIME engine and user dictionary across Emacs instances"
  homepage "https://github.com/unship/rime-daemon"
  url "https://github.com/unship/rime-daemon.git", branch: "main"
  version "0.1.0"
  license "BSD-3-Clause"
  head "https://github.com/unship/rime-daemon.git", branch: "main"

  depends_on "rust" => :build
  depends_on "librime"

  # liberime(GPL-3.0)仅在构建期取源码编出 emacs 模块,产物动态链接本包的 shim
  resource "liberime" do
    url "https://github.com/emacs-rime/liberime.git",
        revision: "482b7854b04169b348f3c943891f0895bd38bc4f"
  end

  def install
    ENV["RIME_INCLUDE_DIR"] = Formula["librime"].opt_include.to_s
    ENV["RIME_LIB_DIR"] = Formula["librime"].opt_lib.to_s
    system "cargo", "build", "--release"

    bin.install "target/release/rime-cli"

    shimdir = lib/"rime-daemon"
    shimdir.mkpath
    shim = shimdir/"librime.1.dylib"
    cp "target/release/librime_shim.dylib", shim
    # LC_ID 用 opt 路径:跨版本升级稳定
    stable_shim = opt_lib/"rime-daemon/librime.1.dylib"
    system "install_name_tool", "-id", stable_shim, shim

    resource("liberime").stage do
      emacs_module = Dir["emacs-module/*"].max_by { |d| File.basename(d).to_i }
      system ENV.cc, "-shared", "-fPIC", "-O2", "-DHAVE_RIME_API",
             "-I", "src", "-I", emacs_module,
             "-I", Formula["librime"].opt_include.to_s,
             *Dir["src/*.c"], shim.to_s,
             "-o", (shimdir/"liberime-core.dylib").to_s
    end

    system "codesign", "-f", "-s", "-",
           shim, shimdir/"liberime-core.dylib", bin/"rime-cli"
  end

  def caveats
    <<~EOS
      Emacs(liberime/pyim)配置:

        (setq liberime-auto-build nil
              liberime-module-file "#{opt_lib}/rime-daemon/liberime-core.dylib")

      rime-cli 已在 PATH 上,shim 会自动懒启动 daemon,无需额外环境变量。
      改完配置后重启所有 Emacs 实例(旧实例内嵌的 librime 仍锁着用户词典)。
    EOS
  end

  test do
    output = shell_output("#{bin}/rime-cli 2>&1", 2)
    assert_match "usage", output
  end
end
