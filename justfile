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

# 持续运行客户端 TCP 和内部 RPC，Ctrl+C 优雅退出
run config="examples/demo-game/config/local.toml":
    cargo run -p demo-game --locked -- --config "{{config}}"

# 运行一秒后优雅退出，供 CI 验证进程生命周期
smoke:
    cargo run -p demo-game --locked -- --config examples/demo-game/config/local.toml --run-for 1

# 验证共享 gRPC 端点上的 player 和 guild 调用，再优雅退出
rpc-smoke:
    cargo run -p demo-game --locked -- --config examples/demo-game/config/local.toml --rpc-smoke

# 通过真实 TCP 验证客户端登录、状态读写与公会调用，再优雅退出
client-smoke:
    cargo run -p demo-game --locked -- --config examples/demo-game/config/local.toml --client-smoke

# Redis 集成测试需要真实 Redis，TOK_TEST_REDIS_URL 默认 redis://127.0.0.1:6379/
redis-test:
    cargo test -p tok-infra --test redis_discovery --locked -- --ignored

# 启动独立游戏和网关进程验证跨进程调用、注销、重启与崩溃过期
redis-smoke:
    cargo test -p demo-game --test redis_discovery --locked -- --ignored two_process

# 验证 Redis session 所有权的竞争、条件操作、过期与 epoch 精度
ownership-test:
    cargo test -p tok-infra --test redis_ownership --locked -- --ignored

# 验证真实 Redis 请求锁的跨进程竞争、过期及条件释放
locks-test:
    cargo test -p tok-infra --test redis_locks --locked -- --ignored

# 验证独立网关进程共享所有权、租约丢失和崩溃过期
ownership-smoke:
    cargo test -p demo-game --test redis_discovery --locked -- --ignored ownership

# 验证本地签名凭证、有效期和维护更换密钥后的失效
resume-token-test:
    cargo test -p tok-gateway player::resume_token --locked

# 验证真实 Redis 与三个独立进程间的跨网关恢复
handoff-smoke:
    cargo test -p demo-game --test redis_discovery --locked -- --ignored handoff

# 验证指定服务配置
config-check config="examples/demo-game/config/local.toml":
    cargo run -p demo-game --locked -- --config "{{config}}" --check-config

# 检查并在输入变化时生成框架和示例游戏协议
proto:
    cargo check -p tok-proto -p demo-game --locked

# 生成框架用户指南和 workspace API 文档，入口 target/doc/tok/index.html
docs:
    cargo doc --workspace --no-deps --locked

# 完成常规开发检查
verify: fmt-check clippy test
