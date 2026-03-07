# rdb

`rdb` is a terminal UI for browsing directories and inspecting Parquet and CSV files, built with Rust, Ratatui, and Polars.

## Features

- **File explorer** (left pane) with `redit`-style tree rows:
  - Directories, files, and parent entry (`..`)
  - Expand/collapse directories
  - File type filtering (Parquet, CSV, or all)
- **Data preview** (right pane):
  - Row table with frozen header and horizontal column scrolling
  - Column sorting (ascending/descending, click header or press `o`)
  - Info tabs: Schema, Statistics, Metadata
  - Lazy data loading via row/column slicing (Polars)
- **Parquet & CSV support**:
  - Open and inspect both Parquet and CSV files
  - Import CSV → Parquet (`Ctrl+I`)
  - Export Parquet → CSV (`Ctrl+E`)
  - CLI file open: `rdb <file>`
- **Search**: press `/` to search within loaded data
- **File operations**: rename, copy, move, delete (double-press confirm)
- **Menu bar**: File, View, Tools, Help menus with mouse support
- **Tools**:
  - Palette popup with 5 built-in themes (`Mainframe Green`, `Black & White`, `Amber`, `Ocean Blue`, `Light Paper`)
  - Persistent settings (theme, recent files)
- **Progress popups** for long-running operations
- `redit`-style CRT look and feel (bordered panes, focused titles, top bar, status footer)

## Install & Run

```bash
# Run from source
cargo run

# Open a file directly
cargo run -- path/to/file.parquet
cargo run -- path/to/file.csv
```

## Keys

| Key | Action |
|-----|--------|
| `Up/Down` | File selection (Files pane) or row scroll (Preview pane) |
| `Left/Right` | Column scroll in Preview pane |
| `Ctrl+Left/Right` | Faster column scroll |
| `PageUp/PageDown` | Page row scroll in Preview pane |
| `Enter` | Load selected file or expand/collapse directory |
| `Left/Backspace` | Collapse directory or go to parent (Files pane) |
| `Right` | Expand directory (Files pane) |
| `Tab` | Switch focused pane |
| `o` | Toggle sort on current column (Preview pane) |
| `/` | Search within loaded data |
| `i` | Show Metadata info tab |
| `s` | Show Statistics info tab |
| `1/2/3` | Switch to Schema/Statistics/Metadata tab |
| `Ctrl+I` | Import CSV to Parquet |
| `Ctrl+E` | Export Parquet to CSV |
| `Ctrl+P` or `Alt+T` | Open Palette |
| `F1` | Keybindings help |
| `r` | Rescan explorer tree |
| `n` | Rename selected file |
| `c` | Copy selected file |
| `m` | Move selected file |
| `d` | Delete selected file (press twice within 3s) |
| `q` or `Ctrl+Q` | Quit |

## Mouse

- Click file rows to select; double-click to load
- Click preview pane to focus; click column headers to sort
- Click menu bar items to open menus
- Mouse wheel: scroll file list or data rows
