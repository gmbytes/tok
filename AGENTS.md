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
