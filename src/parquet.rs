use std::cmp;
use std::fs::File;
use std::path::Path;

use anyhow::Context;
use polars::prelude::*;

pub struct ParquetMeta {
    pub schema_lines: Vec<String>,
    pub total_rows: usize,
    pub total_cols: usize,
}

pub struct ParquetSlice {
    pub row_start: usize,
    pub col_start: usize,
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub fn load_parquet_meta(path: &Path) -> anyhow::Result<ParquetMeta> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = ParquetReader::new(file);

    let total_rows = reader
        .num_rows()
        .with_context(|| format!("failed to read row count from {}", path.display()))?;
    let schema = reader
        .schema()
        .with_context(|| format!("failed to read schema from {}", path.display()))?;

    let mut schema_lines = Vec::with_capacity(schema.len());
    let mut total_cols = 0usize;

    for (name, field) in schema.iter() {
        total_cols += 1;
        schema_lines.push(format!("{}: {:?}", name, field.dtype()));
    }

    Ok(ParquetMeta {
        total_rows,
        total_cols,
        schema_lines,
    })
}

pub fn load_parquet_slice(
    path: &Path,
    row_start: usize,
    row_count: usize,
    projection: &[usize],
    cell_char_limit: usize,
) -> anyhow::Result<ParquetSlice> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

    if projection.is_empty() {
        return Ok(ParquetSlice {
            row_start,
            col_start: 0,
            column_names: Vec::new(),
            rows: Vec::new(),
        });
    }

    let df = ParquetReader::new(file)
        .with_slice(Some((row_start, row_count)))
        .with_projection(Some(projection.to_vec()))
        .finish()
        .with_context(|| format!("failed to read parquet slice from {}", path.display()))?;

    let columns = df.get_columns();
    let mut rows = Vec::with_capacity(df.height());

    for row_idx in 0..df.height() {
        let row = columns
            .iter()
            .map(|series| match series.get(row_idx) {
                Ok(value) => clip_value(&value.to_string(), cell_char_limit),
                Err(_) => "<err>".to_string(),
            })
            .collect::<Vec<_>>();
        rows.push(row);
    }

    Ok(ParquetSlice {
        row_start,
        col_start: *projection.first().unwrap_or(&0),
        column_names: columns
            .iter()
            .map(|series| series.name().to_string())
            .collect(),
        rows,
    })
}

fn clip_value(input: &str, limit: usize) -> String {
    let mut out = String::new();
    for ch in input.chars().take(limit) {
        out.push(ch);
    }
    if input.chars().count() > limit {
        out.push_str("...");
    }
    out
}

pub fn fit_visible_columns(table_width: usize, cell_width: usize) -> usize {
    if table_width == 0 {
        return 0;
    }

    let mut used = 6; // row index column width
    let mut cols = 0;
    while used + cell_width <= table_width {
        used += cell_width;
        cols += 1;
        if used + 3 <= table_width {
            used += 3;
        } else {
            break;
        }
    }

    cmp::max(cols, 1)
}
