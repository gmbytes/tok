[windows]
set shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

# 显示可用任务
default:
    @just --list

# 检查全部 crate
check:
    cargo check --workspace --all-targets --locked

# 执行 workspace 测试
test:
    cargo test --workspace --locked

# 格式化 Rust 代码
fmt:
    cargo fmt --all

# 检查代码格式
fmt-check:
    cargo fmt --all -- --check

# 检查全部 crate 的 lint
clippy:
    cargo clippy --workspace --all-targets --locked -- -D warnings

# 构建当前平台的游戏示例二进制
build:
    cargo build --release -p demo-game --locked

# 持续运行角色生命周期示例，Ctrl+C 优雅退出；当前不启动游戏网络服务
run config="examples/demo-game/config/local.toml":
    cargo run -p demo-game --locked -- --config "{{config}}"

# 运行一秒后优雅退出，供 CI 验证进程生命周期
smoke:
    cargo run -p demo-game --locked -- --config examples/demo-game/config/local.toml --run-for 1

# 验证指定角色配置
config-check config="examples/demo-game/config/local.toml":
    cargo run -p demo-game --locked -- --config "{{config}}" --check-config

# 检查并在输入变化时生成框架和示例游戏协议
proto:
    cargo check -p tok-proto -p demo-game --locked

# 完成常规开发检查
verify: fmt-check clippy test
