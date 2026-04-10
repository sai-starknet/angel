use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has parent")
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn has_allowed_dynamic_marker(lines: &[&str], index: usize) -> bool {
    lines[index].contains("sqlite-dynamic-ok")
        || index
            .checked_sub(1)
            .and_then(|prev| lines.get(prev))
            .is_some_and(|line| line.contains("sqlite-dynamic-ok"))
}

#[test]
fn runtime_sqlite_queries_avoid_uncached_prepare_and_inline_format_sql() {
    let root = workspace_root();
    let files = [
        "crates/torii-ecs-sink/src/grpc_service.rs",
        "crates/introspect-sql-sink/src/sqlite/table.rs",
        "crates/torii-erc20/src/storage.rs",
        "crates/torii-erc721/src/storage.rs",
        "crates/torii-erc1155/src/storage.rs",
    ];
    let forbidden = ["query(&format!", ".prepare("];
    let mut violations = Vec::new();

    for relative_path in files {
        let path = root.join(relative_path);
        let source = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("failed to read {}: {err}", path.display());
        });
        let lines = source.lines().collect::<Vec<_>>();

        for (index, line) in lines.iter().enumerate() {
            for pattern in forbidden {
                if !line.contains(pattern) {
                    continue;
                }
                if pattern == ".prepare(" && line.contains("prepare_cached(") {
                    continue;
                }
                if has_allowed_dynamic_marker(&lines, index) {
                    continue;
                }
                violations.push(format!("{relative_path}:{}: {}", index + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "found runtime SQLite queries that bypass cached prepares or inline format SQL:\n{}",
        violations.join("\n")
    );
}
