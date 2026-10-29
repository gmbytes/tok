# Crate 审查确认记录

本表记录使用者已确认的 crate，以及已审查但尚未明确确认的 crate，方便安排后续审查。审查确认针对当时的代码与讨论范围，不代表不存在缺陷；后续修改相关职责或接口时，应重新评估并更新记录。

## 已确认

| Crate | 确认日期 | 确认范围与依据 |
| --- | --- | --- |
| [tok-grpc](../crates/tok-grpc/src/lib.rs) | 2026-10-29 | 已确认通用 gRPC 适配层的职责与此前审查结果，包括客户端连接、服务端监听、上下文传播和 Tower 处理链；游戏协议适配由上层承担。此前修正包含连接缓存、调用停止和追踪，代码审查后的最后一次相关修改为 `4842fd6`。详见[实现状态](status.md#execution--grpc-审查问题修复2026-10-21)。 |
| [tok-core](../crates/tok-core/src/lib.rs) | 2026-10-29 | 已确认模块划分、命名、导出、错误与执行上下文边界；此前与 tok-execution、tok-grpc 一起复审并修复。 |
| [tok-execution](../crates/tok-execution/src/lib.rs) | 2026-10-29 | 已确认请求与响应、扩展、派发、执行停止与追踪等此前审查结果；共同执行驱动与 gRPC 的修复见[实现状态](status.md#execution--grpc-审查问题修复2026-10-21)。 |
| [tok-proto](../crates/tok-proto/src/lib.rs) | 2026-10-29 | 使用者确认列入已确认清单；现有记录说明其承载生成协议，不依赖执行层，见[实现状态](status.md)。现有记录未单独列出针对该 crate 的完整审查范围。 |
| [原 tok-protobuf，现为 tok-message::protobuf](../crates/tok-message/src/protobuf.rs) | 2026-10-29 | 原独立 crate 的类型化处理链、编解码与派发边界曾获确认，最后相关提交为 `becafb0`；现已并入 tok-message，迁移后的整体边界需要重新审查；详见[实现状态](status.md#protobuf-派发键约束校正2026-10-21)。 |
| [tok-observability](../crates/tok-observability/src/lib.rs) | 2026-10-29 | 已确认日志与追踪关联、采样、过滤和 OTLP HTTPS 的多次复审结果，最后相关提交为 `2041fed`。HTTPS 初始化已测试，真实 TLS 握手和证书失败场景尚未覆盖；详见[实现状态](status.md#otlp-https-支持校正2026-10-21)。 |

| [tok-lifecycle](../crates/tok-lifecycle/src/lib.rs) | 2026-10-29 | 已确认生命周期、任务监督、取消和回收的审查结果；审查时未发现确定性缺陷，详见[实现状态](status.md#可观测事件父级与采样边界校正2026-10-21)。 |

## 已审查，待明确确认

以下 crate 有审查记录；当前对话中尚未明确将它们标记为确认。提交修复或测试通过本身不自动视为使用者确认。

| Crate | 最近审查的主要范围 | 记录或依据 |
| --- | --- | --- |
| [tok-session（原 tok-player）](../crates/tok-session/src/lib.rs) | 模块划分、命名、导出和错误 | 使用者曾明确要求审查，随后要求修复并提交；相关提交为 `18ea656`。近期玩家 RPC 已迁入该 crate，迁移后的整体边界尚未最终确认。 |

未列出的 crate 不代表已经或尚未审查；本表只记录有明确依据的确认和上述审查。新增确认时记录日期、范围及对应提交或文档依据，涉及重大后续变更时重新核对。
