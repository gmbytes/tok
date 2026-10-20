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
