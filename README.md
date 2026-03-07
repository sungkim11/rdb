# rdb

`rdb` is a terminal data explorer built with Rust + Ratatui + Polars.
It provides a multi-pane TUI with a file explorer, Parquet/CSV data preview with sorting and search, info tabs (schema, statistics, metadata), import/export, menu bar, and popup-driven workflows.

## Current Features

- Top menus: `File`, `View`, `Tools`, `Help`
- File explorer pane (left):
  - Tree view for directories/files
  - Expand/collapse directories
  - Double-click support for open/toggle
  - File type filtering (Parquet, CSV, or all)
- Data preview pane (right):
  - Row table with frozen header
  - Horizontal column scrolling
  - Column sorting (ascending/descending)
  - Lazy data loading via row/column slicing (Polars)
- Info tabs:
  - Schema, Statistics, Metadata tabs below preview
  - Prettified popup views
- Search:
  - `Tools | Search` popup for searching within loaded data
- File operations:
  - Rename, copy, move
  - Delete (double-press confirm)
- Import/export:
  - `File | Import from CSV` (CSV to Parquet)
  - `File | Export to CSV` (Parquet to CSV)
  - Progress popups for long-running operations
- Theme palette:
  - `Tools | Palette`
  - 5 themes (`Mainframe Green`, `Black & White`, `Amber`, `Ocean Blue`, `Light Paper`)
  - Keyboard and mouse selection in palette popup
  - Palette is persisted across restarts
- Help popups:
  - `Help | Keybindings`
  - `Help | About rdb`

## Prerequisites

### 1) Rust toolchain (required)

Install Rust from **https://rustup.rs** (recommended and required for build/deploy).

Then verify:

```bash
rustc --version
cargo --version
```

### 2) System build tools

- Linux: install compiler/linker toolchain (for example `build-essential` on Debian/Ubuntu)
- macOS: install Xcode Command Line Tools (`xcode-select --install`)
- Windows: install Visual Studio Build Tools (C++ workload)

## Deployment / Build Instructions

### 1) Clone and enter project

```bash
git clone <your-repo-url> rdb
cd rdb
```

### 2) Build debug binary

```bash
cargo build
```

Binary location:

- Linux/macOS: `target/debug/rdb`
- Windows: `target\debug\rdb.exe`

### 3) Run directly

```bash
cargo run -- path/to/file.parquet
cargo run -- path/to/file.csv
```

If no file is provided, `rdb` starts with the file explorer in the current directory.

### 4) Build release binary (recommended for deployment)

```bash
cargo build --release
```

Release binary:

- Linux/macOS: `target/release/rdb`
- Windows: `target\release\rdb.exe`

### 5) Optional local install (Linux/macOS)

```bash
install -Dm755 target/release/rdb ~/.local/bin/rdb
```

Make sure `~/.local/bin` is in your `PATH`.

### 6) Cross-platform notes

- The produced binary targets the platform you build on by default.
- To build for another target, add Rust target(s) first:

```bash
rustup target add x86_64-pc-windows-gnu
rustup target add x86_64-unknown-linux-gnu
```

Then build with `--target`:

```bash
cargo build --release --target x86_64-pc-windows-gnu
```

## Runtime Data

Settings (palette, recent files) are stored in:

- `$XDG_CONFIG_HOME/rdb/settings.conf` (if `XDG_CONFIG_HOME` is set), or
- `~/.config/rdb/settings.conf`

## Keybindings

- `F1`: Help keybindings popup
- `Tab`: switch focused pane
- `Ctrl-Q`: quit
- `Ctrl-P` / `Alt-T`: open palette
- `Ctrl-I`: import CSV to Parquet
- `Ctrl-E`: export Parquet to CSV
- `/`: search within loaded data
- `o`: toggle sort on current column (Preview pane)
- `i`: show Metadata info tab
- `s`: show Statistics info tab
- `1` / `2` / `3`: switch to Schema / Statistics / Metadata tab
- `r`: rescan explorer tree
- `n`: rename selected file
- `c`: copy selected file
- `m`: move selected file
- `d`: delete selected file (press twice within 3s)

Navigation:

- `Up/Down`: file selection (Files pane) or row scroll (Preview pane)
- `Left/Right`: column scroll (Preview pane) or collapse/expand (Files pane)
- `Ctrl-Left/Right`: faster column scroll
- `PageUp/PageDown`: page row scroll in Preview pane
- `Enter`: load selected file or expand/collapse directory
- `Backspace`: collapse directory or go to parent (Files pane)

Menu navigation:

- `Left/Right`: switch menus
- `Up/Down`: move menu selection
- `Enter`: activate menu action
- `Esc`: close menu/popup

## Mouse Support

- Click top menu labels to open dropdowns
- Click dropdown items to execute actions
- File explorer double-click opens files and expands/collapses directories
- Click column headers to sort
- Palette popup supports mouse click selection and apply

## Quick Start

```bash
cargo run -- data/sample.parquet
```

Then try:

1. `Tools | Palette`
2. `Tools | Search`
3. `File | Export to CSV`
4. `Help | Keybindings`
