use std::cmp;
use std::fs::{self, File};
use std::path::Path;

use anyhow::Context;
use polars::prelude::*;

// ---------------------------------------------------------------------------
// Existing types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// New types: metadata inspection
// ---------------------------------------------------------------------------

pub struct ParquetFileInfo {
    pub file_size_bytes: u64,
    pub num_row_groups: usize,
    pub created_by: String,
    pub row_groups: Vec<RowGroupDetail>,
}

pub struct RowGroupDetail {
    pub index: usize,
    pub num_rows: i64,
    pub total_byte_size: i64,
    pub columns: Vec<ColumnChunkDetail>,
}

pub struct ColumnChunkDetail {
    pub name: String,
    pub compression: String,
    pub compressed_size: i64,
    pub uncompressed_size: i64,
}

// ---------------------------------------------------------------------------
// New types: column statistics
// ---------------------------------------------------------------------------

pub struct ColumnStatistics {
    pub name: String,
    pub dtype: String,
    pub total_count: usize,
    pub null_count: usize,
    pub min_value: String,
    pub max_value: String,
    pub mean_value: String,
}

// ---------------------------------------------------------------------------
// New types: search results
// ---------------------------------------------------------------------------

pub struct SearchResults {
    pub query: String,
    pub column_names: Vec<String>,
    pub matching_rows: Vec<(usize, Vec<String>)>,
    pub capped: bool,
}

// ---------------------------------------------------------------------------
// Existing functions
// ---------------------------------------------------------------------------

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

/// Compute the permutation that sorts the file by a single column.
/// Returns a Vec mapping sorted-position → original-row-index.
pub fn compute_sort_indices(
    path: &Path,
    col_index: usize,
    descending: bool,
) -> anyhow::Result<Vec<usize>> {
    let file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .with_projection(Some(vec![col_index]))
        .finish()
        .with_context(|| format!("failed to read sort column from {}", path.display()))?;

    let col = &df.get_columns()[0];
    let options = SortOptions::default().with_order_descending(descending);
    let sorted_idx = col.as_materialized_series().arg_sort(options);

    Ok(sorted_idx
        .into_no_null_iter()
        .map(|i| i as usize)
        .collect())
}

/// Load specific rows (by original-row-index) with the given column projection.
pub fn load_parquet_rows(
    path: &Path,
    row_indices: &[usize],
    projection: &[usize],
    cell_char_limit: usize,
) -> anyhow::Result<ParquetSlice> {
    let file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

    if projection.is_empty() || row_indices.is_empty() {
        return Ok(ParquetSlice {
            row_start: 0,
            col_start: 0,
            column_names: Vec::new(),
            rows: Vec::new(),
        });
    }

    let df = ParquetReader::new(file)
        .with_projection(Some(projection.to_vec()))
        .finish()
        .with_context(|| format!("failed to read parquet from {}", path.display()))?;

    let idx_vec: Vec<IdxSize> = row_indices.iter().map(|&i| i as IdxSize).collect();
    let idx = IdxCa::new("idx".into(), &idx_vec);
    let sliced = df
        .take(&idx)
        .with_context(|| "failed to select rows for sorted view")?;

    let columns = sliced.get_columns();
    let mut rows = Vec::with_capacity(sliced.height());

    for row_idx in 0..sliced.height() {
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
        row_start: 0,
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

// ---------------------------------------------------------------------------
// Metadata inspection (uses Apache `parquet` crate)
// ---------------------------------------------------------------------------

pub fn load_parquet_file_info(path: &Path) -> anyhow::Result<ParquetFileInfo> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let file_size_bytes = fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len();

    let file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("failed to parse parquet metadata from {}", path.display()))?;
    let meta = reader.metadata();
    let file_meta = meta.file_metadata();

    let created_by = file_meta
        .created_by()
        .unwrap_or("unknown")
        .to_string();

    let mut row_groups = Vec::with_capacity(meta.num_row_groups());
    for i in 0..meta.num_row_groups() {
        let rg = meta.row_group(i);
        let mut columns = Vec::with_capacity(rg.num_columns());
        for j in 0..rg.num_columns() {
            let col = rg.column(j);
            columns.push(ColumnChunkDetail {
                name: col.column_path().to_string(),
                compression: format!("{:?}", col.compression()),
                compressed_size: col.compressed_size(),
                uncompressed_size: col.uncompressed_size(),
            });
        }
        row_groups.push(RowGroupDetail {
            index: i,
            num_rows: rg.num_rows(),
            total_byte_size: rg.total_byte_size(),
            columns,
        });
    }

    Ok(ParquetFileInfo {
        file_size_bytes,
        num_row_groups: meta.num_row_groups(),
        created_by,
        row_groups,
    })
}

// ---------------------------------------------------------------------------
// Column statistics
// ---------------------------------------------------------------------------

pub fn compute_column_statistics(path: &Path) -> anyhow::Result<Vec<ColumnStatistics>> {
    let file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .finish()
        .with_context(|| format!("failed to read {}", path.display()))?;

    let mut stats = Vec::with_capacity(df.width());

    for col in df.get_columns() {
        let name = col.name().to_string();
        let dtype = format!("{:?}", col.dtype());
        let total_count = col.len();
        let null_count = col.null_count();

        let min_value = col
            .min_reduce()
            .map(|s| s.value().to_string())
            .unwrap_or_else(|_| "N/A".to_string());

        let max_value = col
            .max_reduce()
            .map(|s| s.value().to_string())
            .unwrap_or_else(|_| "N/A".to_string());

        let mean_value = if col.dtype().is_numeric() {
            let v = col.mean_reduce().value().to_string();
            if v == "null" { "N/A".to_string() } else { v }
        } else {
            "N/A".to_string()
        };

        stats.push(ColumnStatistics {
            name,
            dtype,
            total_count,
            null_count,
            min_value,
            max_value,
            mean_value,
        });
    }

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Search / filter
// ---------------------------------------------------------------------------

const MAX_SEARCH_RESULTS: usize = 10_000;

pub fn search_parquet_rows(
    path: &Path,
    query: &str,
    cell_char_limit: usize,
) -> anyhow::Result<SearchResults> {
    let file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .finish()
        .with_context(|| format!("failed to read {}", path.display()))?;

    let columns = df.get_columns();
    let column_names: Vec<String> = columns.iter().map(|s| s.name().to_string()).collect();
    let query_lower = query.to_lowercase();
    let total_rows = df.height();

    let mut matching_rows = Vec::new();
    let mut capped = false;

    for row_idx in 0..total_rows {
        let mut matched = false;
        let mut cells = Vec::with_capacity(columns.len());

        for series in columns {
            let cell_str = match series.get(row_idx) {
                Ok(value) => clip_value(&value.to_string(), cell_char_limit),
                Err(_) => "<err>".to_string(),
            };
            if !matched && cell_str.to_lowercase().contains(&query_lower) {
                matched = true;
            }
            cells.push(cell_str);
        }

        if matched {
            matching_rows.push((row_idx, cells));
            if matching_rows.len() >= MAX_SEARCH_RESULTS {
                capped = true;
                break;
            }
        }
    }

    Ok(SearchResults {
        query: query.to_string(),
        column_names,
        matching_rows,
        capped,
    })
}

// ---------------------------------------------------------------------------
// Export / write parquet
// ---------------------------------------------------------------------------

pub fn export_parquet(
    source: &Path,
    dest: &Path,
    row_indices: Option<&[usize]>,
) -> anyhow::Result<usize> {
    let file =
        File::open(source).with_context(|| format!("failed to open {}", source.display()))?;
    let df = ParquetReader::new(file)
        .finish()
        .with_context(|| format!("failed to read {}", source.display()))?;

    let mut out_df = if let Some(indices) = row_indices {
        let idx_vec: Vec<IdxSize> = indices.iter().map(|&i| i as IdxSize).collect();
        let idx = IdxCa::new("idx".into(), &idx_vec);
        df.take(&idx)
            .with_context(|| "failed to select rows for export")?
    } else {
        df
    };

    let rows_written = out_df.height();

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let out_file =
        File::create(dest).with_context(|| format!("failed to create {}", dest.display()))?;
    ParquetWriter::new(out_file)
        .finish(&mut out_df)
        .with_context(|| format!("failed to write parquet to {}", dest.display()))?;

    Ok(rows_written)
}
