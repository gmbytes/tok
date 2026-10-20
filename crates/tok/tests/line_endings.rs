//! 仓库文本文件换行约束，覆盖已跟踪文件和未忽略的新文件。
use std::{fs, path::Path, process::Command};

#[test]
fn repository_text_files_use_lf() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ])
        .current_dir(&root)
        .output()
        .expect("需要 Git 枚举工程文件");
    assert!(output.status.success(), "Git 枚举工程文件失败");
    let paths = String::from_utf8(output.stdout).expect("工程路径必须使用 UTF-8");
    let mut invalid = Vec::new();
    for name in paths.split('\0').filter(|name| !name.is_empty()) {
        let path = root.join(name);
        // 删除的文件和子模块目录不属于当前工作区的文本输入。
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path).expect("工程文件必须可读");
        // 二进制文件没有文本换行约定；不检查忽略的构建产物和 Git 元数据。
        if bytes.contains(&0) || std::str::from_utf8(&bytes).is_err() {
            continue;
        }
        if bytes.contains(&b'\r') {
            invalid.push(name);
        }
    }
    assert!(
        invalid.is_empty(),
        "以下文本文件含 CRLF 或 CR，必须转换为 LF：\n{}",
        invalid.join("\n")
    );
}
