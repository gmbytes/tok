# 项目协作约定

## 提交规范

Git 提交使用 Conventional Commits 格式：

```text
<type>[可选 scope][可选 !]: <中文描述>

[可选中文正文]

[可选页脚]
```

- type 使用标准英文类型，例如 `feat`、`fix`、`docs`、`refactor`、`test`、`build`、`ci`、`chore`、`perf`、`style` 和 `revert`。
- scope 可使用模块或组件名称，例如 `gateway`、`player` 和 `discovery`。
- 提交标题中的描述和正文使用中文，技术名称、标识符和命令保留原文。
- 破坏性变更使用 `!` 或 `BREAKING CHANGE:` 标记，标记后的说明使用中文。页脚中的固定关键字和引用标识保留原文。
- 描述应清楚表达本次提交的实际变更。

示例：

```text
feat(gateway): 添加玩家会话所有权管理
fix(player): 修复定时器重复触发问题
docs: 更新框架设计约定
build: 添加 just 开发命令入口
```

## 文件换行

- 整个工程的文本文件统一使用 LF（`\n`），禁止 CRLF 和单独 CR。
- 编辑、新建及生成工程文本文件时都必须遵守；Git 和编辑器规则分别见 `.gitattributes`、`.editorconfig`，Rust 格式化由 `rustfmt.toml` 固定为 LF。

## 文档维护

- 文档分工见 `docs/README.md`；架构、目标契约、当前实现、实施计划、用户指南与历史验收分别维护，不将待实现接口写成已提供能力。
- 当前方向校正按 `docs/implementation-plan.md` 的 P1–P7 推进，旧阶段编号仅用于历史查询。
- 每批代码变更同步更新实现状态、计划和可运行示例；Rust 用户指南继续走 rustdoc，不另建与 API 脱节的使用文档体系。

## 执行上下文契约

- 框架提供的业务处理链、可组合中间件入口与用户扩展入口统一使用 `tok_execution::Request<T>`，成功结果使用 `tok_execution::Response<R>`，失败保留具体错误类型；协议输入放在 T 中。Request 包含 Context、input 与输入 Extensions，Response 包含 output 与输出 Extensions，不携带 Context。Context 只负责服务、操作、截止点、取消与追踪；Request / Response 不依赖 ContextError，不负责派生执行范围。使用第三方协议或库时，由框架适配器二次封装以接续此契约，不另建只传播部分字段的平行执行上下文。
- 共同执行结果使用 Result<Response<R>, tok_execution::Error<E>>；Stopped 统一表达执行取消与预算耗尽，Inner(E) 保留具体处理错误。Context 构造错误与注册配置错误分别表达，不混入调用失败。协议边界可显式映射为协议错误，普通第三方 Service 通过执行 Layer 适配；已采用共同错误的链再次安装保护时，使用 Error::flatten 保持一层结构，转换具体错误使用 Error::map_inner，不在模块派发错误中重复增加执行停止变体。
- 同一次本地执行在解码后的输入转换、处理链组合、排队和任务切换中保留 Context 与 Request 的输入扩展。Request::map / try_map 和 into_parts / from_parts 用于接续输入。派生 Context 后显式移交适用扩展，保留原截止点、取消关系与追踪，不因重新组装请求而重置预算或仅依赖当前任务的隐式 span。`Context.service` 始终表示本地执行所属服务，出站目标另行表达。
- 根入口、独立异步消费、timer、生命周期通知和补偿任务可以建立新执行范围，但必须明确该范围的起点、预算、停止关系、追踪关联和扩展继承规则。排队本身不是自动切断上下文的理由；独立工作不得隐式继承已经结束的发布者调用期限，也不得借“新范围”绕过原调用预算。
- 输入 Extensions 承载本地可选元信息与能力；输出 Extensions 沿返回路径传递处理结果元信息，不自动复制输入扩展、不作为输入回写通道。结果转换使用 Response::map / try_map 或拆分重组保留输出扩展；丢弃输出扩展必须位于明确的终端/传输边界，失败元信息由具体错误类型承载。多处理者/目标聚合须明确合并顺序和同类型冲突规则。必需业务输入显式传参，固定处理器依赖优先由构造注入。保留上下文不等于复制所有能力到不同服务或 session；范围变化时显式筛选并安装目标本地能力，避免误用源 SessionHandle 或形成引用环。
- 跨 gRPC / NATS 只传播协议明确允许的元数据，不序列化 Extensions、句柄、单调时钟截止点或取消令牌。接收端校验后建立本地 Context；请求式 RPC 的剩余预算与独立事件消费的处理预算分别定义，不承诺远端取消或副作用回滚。
- 此约定约束框架执行接口与适配器，不要求基础容器、发现/存储后端、编解码器、第三方 Tower Service 或普通业务辅助函数全部接收 Request 或返回 Response。execution::Layer、锁中间件和 sync_handler 保持泛型透明传递，不替内层自动包装结果。原生传输接口可以保留，但框架必须提供保持上下文契约的执行适配，且便捷方法不能默默丢弃已有 Context。
- 审查与验收须覆盖真实组合边界：扩展标记保留、排队后的剩余预算、取消与追踪，以及独立范围和跨进程重建。审查与校正记录见 [实现状态](docs/status.md#执行上下文连续性审查2026-10-21) 和 [实施计划](docs/implementation-plan.md#执行上下文连续性校正)。这是本项目的框架契约，不表述为 Tower 或 Rust 强制规则。

## Rust 公开 API 与模块组织

公开 API 按使用者的概念组织，内部模块按实现需要组织，两者通过 `pub use` 解耦。本项目据 Rust 官方指导采用以下规则；这不是 Rust 规定的固定目录模板。

- 模块默认私有。只为拆分实现文件的模块使用 `mod`，不因为有一个文件就公开一个模块。
- 模块代表使用者需要理解、组合或扩展的独立功能时，使用 `pub mod` 提供功能命名空间；不按文件大小或类型数量机械判断。
- crate 的主要使用入口可以通过 `pub use` 导出到根，以便查找和导入；这同时适用于领域类型与通用类型，不采用“只有通用类型可以根导出”的限制。
- 主要类型存在相关辅助 API 时，允许同时保留公开模块路径和根重导出。双路径本身不是设计缺陷，不仅为消除双路径而重构。
- 命名结合公开使用场景判断。根导出的角色名可以保留有辨识作用的前缀；模块中的类型不机械删除前缀，也不为形式统一而更名。使用标准术语或类型名时保持其原有语义，例如 BoxFuture 的输出参数不隐含 Result。
- 各 crate 的 lib.rs 说明主要入口与功能命名空间，指南展示推荐导入方式。隐藏内部模块时，将用户需要的说明移到公开项上，检查 rustdoc 链接与示例。

审查公开路径时回答：使用者是否需要理解这个模块概念、该项是否是主要入口、路径是否暴露了纯实现细节。按使用价值决策，不把项目取舍表述成社区强制规范。

依据：[Rust 官方公开 API 指导](https://doc.rust-lang.org/book/ch14-02-publishing-to-crates-io.html#exporting-a-convenient-public-api)、[标准库 collections 的模块与重导出实践](https://doc.rust-lang.org/std/collections/index.html)、[Rust API Guidelines 命名约定](https://rust-lang.github.io/api-guidelines/naming.html)。
