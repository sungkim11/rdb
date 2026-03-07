use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, bail};
use postgres::{Client, NoTls};

use crate::duckdb::SqlResult;

const MAX_SQL_ROWS: usize = 10_000;
const CONNECT_TIMEOUT_SECS: u64 = 5;

pub fn run_pg_sql(conn_str: &str, query: &str) -> anyhow::Result<SqlResult> {
    let mut client = connect(conn_str)?;

    let stmt = client
        .prepare(query)
        .with_context(|| "SQL error: failed to prepare query")?;

    let column_names: Vec<String> = stmt.columns().iter().map(|c| c.name().to_string()).collect();
    let col_count = column_names.len();

    let result_rows = client
        .query(&stmt, &[])
        .with_context(|| "SQL error: failed to execute query")?;

    let mut rows = Vec::new();
    let mut capped = false;
    for row in &result_rows {
        let mut cells = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val = format_pg_cell(row, i);
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

fn format_pg_cell(row: &postgres::Row, idx: usize) -> String {
    use postgres::types::Type;

    let col_type = row.columns()[idx].type_();

    macro_rules! try_get {
        ($t:ty) => {
            match row.try_get::<_, Option<$t>>(idx) {
                Ok(Some(v)) => return v.to_string(),
                Ok(None) => return "NULL".to_string(),
                Err(_) => {}
            }
        };
    }

    match *col_type {
        Type::BOOL => try_get!(bool),
        Type::INT2 => try_get!(i16),
        Type::INT4 => try_get!(i32),
        Type::INT8 => try_get!(i64),
        Type::FLOAT4 => try_get!(f32),
        Type::FLOAT8 => try_get!(f64),
        Type::NUMERIC => try_get!(rust_decimal::Decimal),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => try_get!(String),
        Type::BYTEA => {
            match row.try_get::<_, Option<Vec<u8>>>(idx) {
                Ok(Some(v)) => return format!("<blob {} bytes>", v.len()),
                Ok(None) => return "NULL".to_string(),
                Err(_) => {}
            }
        }
        Type::DATE => try_get!(chrono::NaiveDate),
        Type::TIME => try_get!(chrono::NaiveTime),
        Type::TIMESTAMP => try_get!(chrono::NaiveDateTime),
        Type::TIMESTAMPTZ => try_get!(chrono::DateTime<chrono::Utc>),
        Type::UUID => try_get!(uuid::Uuid),
        Type::JSON | Type::JSONB => {
            match row.try_get::<_, Option<serde_json::Value>>(idx) {
                Ok(Some(v)) => return v.to_string(),
                Ok(None) => return "NULL".to_string(),
                Err(_) => {}
            }
        }
        _ => {}
    }

    // Fallback: try as string
    match row.try_get::<_, Option<String>>(idx) {
        Ok(Some(v)) => v,
        Ok(None) => "NULL".to_string(),
        Err(_) => "?".to_string(),
    }
}

pub fn test_connection(conn_str: &str) -> anyhow::Result<()> {
    let mut client = connect(conn_str)?;
    client
        .simple_query("SELECT 1")
        .with_context(|| "connection test query failed")?;
    Ok(())
}

/// Connect with a timeout so the TUI doesn't freeze on unreachable hosts.
fn connect(conn_str: &str) -> anyhow::Result<Client> {
    let conn = ensure_connect_timeout(conn_str);
    let conn_for_thread = conn.clone();

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = Client::connect(&conn_for_thread, NoTls);
        let _ = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS)) {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(e)) => Err(e).with_context(|| "failed to connect to PostgreSQL"),
        Err(_) => bail!("connection timed out after {CONNECT_TIMEOUT_SECS}s"),
    }
}

/// Fetch database tree: schemas containing tables, each table with columns.
pub fn fetch_db_tree(conn_str: &str) -> anyhow::Result<Vec<PgSchema>> {
    let mut client = connect(conn_str)?;

    let rows = client.query(
        "SELECT t.table_schema, t.table_name, t.table_type, \
                c.column_name, c.data_type, c.ordinal_position, \
                c.is_nullable, c.column_default \
         FROM information_schema.tables t \
         JOIN information_schema.columns c \
           ON c.table_schema = t.table_schema AND c.table_name = t.table_name \
         WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') \
         ORDER BY t.table_schema, t.table_name, c.ordinal_position",
        &[],
    ).with_context(|| "failed to fetch database metadata")?;

    let mut schemas: Vec<PgSchema> = Vec::new();

    for row in &rows {
        let schema_name: String = row.get(0);
        let table_name: String = row.get(1);
        let table_type: String = row.get(2);
        let col_name: String = row.get(3);
        let data_type: String = row.get(4);
        let nullable: String = row.get(6);
        let col_default: Option<String> = row.get(7);

        let schema = match schemas.iter_mut().find(|s| s.name == schema_name) {
            Some(s) => s,
            None => {
                schemas.push(PgSchema {
                    name: schema_name.clone(),
                    tables: Vec::new(),
                    expanded: true,
                });
                schemas.last_mut().unwrap()
            }
        };

        let table = match schema.tables.iter_mut().find(|t| t.name == table_name) {
            Some(t) => t,
            None => {
                schema.tables.push(PgTable {
                    name: table_name.clone(),
                    table_type: table_type.clone(),
                    columns: Vec::new(),
                });
                schema.tables.last_mut().unwrap()
            }
        };

        table.columns.push(PgColumn {
            name: col_name,
            data_type,
            nullable: nullable == "YES",
            default: col_default,
        });
    }

    Ok(schemas)
}

pub struct PgSchema {
    pub name: String,
    pub tables: Vec<PgTable>,
    pub expanded: bool,
}

pub struct PgTable {
    pub name: String,
    pub table_type: String,
    pub columns: Vec<PgColumn>,
}

pub struct PgColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
}

/// Ensure the connection string has a connect_timeout parameter.
fn ensure_connect_timeout(input: &str) -> String {
    if input.contains("connect_timeout") {
        return input.to_string();
    }
    format!("{input} connect_timeout={CONNECT_TIMEOUT_SECS}")
}
