use std::path::Path;

use anyhow::Context;
use duckdb::Connection;

pub struct SqlResult {
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
    pub capped: bool,
}

const MAX_SQL_ROWS: usize = 10_000;

pub fn run_sql(file_path: &Path, query: &str) -> anyhow::Result<SqlResult> {
    let conn = Connection::open_in_memory()
        .with_context(|| "failed to open DuckDB in-memory database")?;

    let path_str = file_path
        .to_str()
        .with_context(|| "file path is not valid UTF-8")?;

    let ext = file_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    let table_alias = if ext.eq_ignore_ascii_case("csv") {
        format!("read_csv_auto('{}')", path_str.replace('\'', "''"))
    } else {
        format!("read_parquet('{}')", path_str.replace('\'', "''"))
    };

    conn.execute_batch(&format!("CREATE VIEW data AS SELECT * FROM {table_alias}"))
        .with_context(|| "failed to create view over data file")?;

    let mut stmt = conn
        .prepare(query)
        .with_context(|| "SQL error: failed to prepare query")?;

    let mut result_rows = stmt
        .query([])
        .with_context(|| "SQL error: failed to execute query")?;

    // Get column info from the underlying statement via Rows::as_ref()
    let (col_count, column_names) = match result_rows.as_ref() {
        Some(s) => (s.column_count(), s.column_names()),
        None => (0, Vec::new()),
    };

    let mut rows = Vec::new();
    let mut capped = false;
    while let Some(row) = result_rows
        .next()
        .with_context(|| "failed to read row")?
    {
        let mut cells = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: String = row
                .get::<_, duckdb::types::Value>(i)
                .map(|v| format_duckdb_value(&v))
                .unwrap_or_else(|_| "NULL".to_string());
            cells.push(val);
        }
        rows.push(cells);
        if rows.len() >= MAX_SQL_ROWS {
            capped = true;
            break;
        }
    }

    let row_count = rows.len();

    Ok(SqlResult {
        column_names,
        rows,
        row_count,
        capped,
    })
}

fn format_duckdb_value(v: &duckdb::types::Value) -> String {
    match v {
        duckdb::types::Value::Null => "NULL".to_string(),
        duckdb::types::Value::Boolean(b) => b.to_string(),
        duckdb::types::Value::TinyInt(i) => i.to_string(),
        duckdb::types::Value::SmallInt(i) => i.to_string(),
        duckdb::types::Value::Int(i) => i.to_string(),
        duckdb::types::Value::BigInt(i) => i.to_string(),
        duckdb::types::Value::HugeInt(i) => i.to_string(),
        duckdb::types::Value::UTinyInt(i) => i.to_string(),
        duckdb::types::Value::USmallInt(i) => i.to_string(),
        duckdb::types::Value::UInt(i) => i.to_string(),
        duckdb::types::Value::UBigInt(i) => i.to_string(),
        duckdb::types::Value::Float(f) => f.to_string(),
        duckdb::types::Value::Double(f) => f.to_string(),
        duckdb::types::Value::Text(s) => s.clone(),
        duckdb::types::Value::Blob(b) => format!("<blob {} bytes>", b.len()),
        _ => format!("{:?}", v),
    }
}
