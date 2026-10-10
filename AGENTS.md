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
