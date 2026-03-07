# rdb

`rdb` is a terminal UI for browsing directories and inspecting parquet files with Rust, Ratatui, and Polars.

## Features

- Left file explorer pane with `redit`-style tree rows:
  - directories + files
  - parent entry (`..`)
  - expand/collapse directories
- Right parquet inspector pane:
  - schema section
  - row table with frozen header
  - horizontal column scrolling
- Lazy parquet preview windows (row/column slicing via Polars parquet reader)
- File operations from the explorer:
  - rename
  - copy
  - move
  - delete (double-press confirm)
- Tools:
  - Palette popup with 5 built-in themes (`Mainframe Green`, `Black & White`, `Amber`, `Ocean Blue`, `Light Paper`)
- File menu:
  - `Quit` (click `File` then click `Quit`)
- `redit`-style CRT look and feel (green palette, bordered panes, focused titles, top bar, status/message footer)

## Run

```bash
cargo run
```

## Keys

- `Up/Down`: file selection (Files pane) or row scroll (Preview pane)
- `Left/Right`: column scroll in Preview pane
- `Ctrl+Left/Right`: faster column scroll
- `PageUp/PageDown`: page row scroll in Preview pane
- `Enter`: load selected parquet
- `Enter` on directory: expand/collapse directory
- `Left/Backspace` in Files pane: collapse directory or go to parent
- `Right` in Files pane: expand directory
- `Tab`: switch focused pane
- `Ctrl+P` or `Alt+T`: open `Tools | Palette`
- `r`: rescan explorer tree
- `n`: rename selected parquet
- `c`: copy selected parquet
- `m`: move selected parquet
- `d`: delete selected parquet (press twice within 3s)
- `q` or `Ctrl+q`: quit

## Mouse

- Click file rows to select
- Double-click a file row to load parquet
- Click preview pane to focus it
- Mouse wheel in file pane: move file selection
- Mouse wheel in preview pane: scroll rows
