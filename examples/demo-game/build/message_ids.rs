//! 校验游戏消息枚举与 protobuf 类型的名称关联，生成静态类型映射。

use heck::{ToShoutySnakeCase, ToUpperCamelCase};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
    io,
};

/// 每个顶层游戏消息必须有一个同名、正数且唯一的 ID，零只用于未指定值。
#[expect(
    clippy::expect_used,
    reason = "writeln 只写入 String，格式参数仅为 String 和 i32，不存在返回 fmt::Error 的写入或自定义格式化路径"
)]
pub fn generate(messages: &[String], values: &[(String, i32)]) -> io::Result<String> {
    let invalid = |reason: String| io::Error::new(io::ErrorKind::InvalidData, reason);
    let mut expected = BTreeMap::new();
    for message in messages {
        let key = format!("MESSAGE_ID_{}", message.to_shouty_snake_case());
        if expected.insert(key.clone(), message).is_some() {
            return Err(invalid(format!("消息名称映射冲突：{key}")));
        }
    }
    let mut numbers = BTreeSet::new();
    let mut unspecified = false;
    let mut output = String::new();
    for (name, number) in values {
        if *number == 0 && name == "MESSAGE_ID_UNSPECIFIED" && !unspecified {
            unspecified = true;
            continue;
        }
        if *number <= 0 || !numbers.insert(*number) {
            return Err(invalid(format!("消息 ID 必须为唯一正数：{name}={number}")));
        }
        let message = expected
            .remove(name)
            .ok_or_else(|| invalid(format!("枚举项没有对应消息：{name}")))?;
        // 与 prost 的 Rust 类型命名规则一致，包括 URL 等缩写。
        let mut message = message.to_upper_camel_case();
        if message == "Self" {
            message.push('_');
        }
        if message.starts_with(|ch: char| ch.is_ascii_digit()) {
            message.insert(0, '_');
        }
        // String 的 fmt::Write 不返回写入错误；这里也没有自定义 Display 实现。
        // 内存分配失败不会通过 fmt::Result 返回，不能用此 Result 将其作为普通 I/O 错误处理。
        writeln!(
            output,
            "impl super::GameMessage for {message} {{ const MESSAGE_ID: u32 = {number}; }}"
        )
        .expect("写入字符串不会失败");
    }
    if !unspecified {
        return Err(invalid(
            "MessageId 必须定义 MESSAGE_ID_UNSPECIFIED=0".into(),
        ));
    }
    if !expected.is_empty() {
        return Err(invalid(format!(
            "消息缺少 ID：{}",
            expected.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    Ok(output)
}
