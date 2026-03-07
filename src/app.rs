use std::cmp;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::parquet::{
    ColumnStatistics, ParquetFileInfo, ParquetMeta, SearchResults, compute_column_statistics,
    compute_csv_sort_indices, compute_sort_indices, export_csv, fit_visible_columns, import_csv,
    load_csv_meta, load_csv_rows, load_csv_slice, load_parquet_file_info, load_parquet_meta,
    load_parquet_rows, load_parquet_slice, search_csv_rows, search_parquet_rows,
};
use crate::theme::{PaletteTheme, Theme};
use anyhow::Context;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};

const DEFAULT_ROW_WINDOW: usize = 256;
const DEFAULT_COL_WINDOW: usize = 24;
const CELL_CHAR_LIMIT: usize = 48;
const DOUBLE_CLICK_MS: u64 = 500;
const SETTINGS_FILE: &str = "settings.conf";
const APP_NAME: &str = "rdb";

#[derive(Clone, Copy)]
struct Rect {
    x: u16,
    y: u16,
    width: u16,
    height: u16,
}

impl Rect {
    fn contains(self, x: u16, y: u16) -> bool {
        x >= self.x
            && y >= self.y
            && x < self.x.saturating_add(self.width)
            && y < self.y.saturating_add(self.height)
    }
}

#[derive(Clone, Copy)]
struct MouseRegions {
    files_rect: Rect,
    files_inner: Rect,
    preview_rect: Rect,
    rows_inner: Rect,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OpenMenu {
    File,
    View,
    Tools,
    Help,
}

pub struct MenuEntry {
    pub label: &'static str,
    pub shortcut: &'static str,
    pub is_separator: bool,
}

const fn entry(label: &'static str, shortcut: &'static str) -> MenuEntry {
    MenuEntry { label, shortcut, is_separator: false }
}

const fn sep() -> MenuEntry {
    MenuEntry { label: "", shortcut: "", is_separator: true }
}

impl OpenMenu {
    pub fn items(self) -> &'static [MenuEntry] {
        match self {
            OpenMenu::File => &FILE_MENU,
            OpenMenu::View => &VIEW_MENU,
            OpenMenu::Tools => &TOOLS_MENU,
            OpenMenu::Help => &HELP_MENU,
        }
    }
}

static FILE_MENU: [MenuEntry; 4] = [
    entry("Import from CSV", "Ctrl+I"),
    entry("Export to CSV", "Ctrl+E"),
    sep(),
    entry("Quit", "Ctrl+Q"),
];

static VIEW_MENU: [MenuEntry; 0] = [];

static TOOLS_MENU: [MenuEntry; 5] = [
    entry("Search", "/"),
    sep(),
    entry("Refresh", "r"),
    sep(),
    entry("Palette", "Ctrl+P"),
];

static HELP_MENU: [MenuEntry; 2] = [
    entry("Keybindings", "F1"),
    entry("About rdb", ""),
];

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ActivePane {
    Files,
    Preview,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FileActionKind {
    Rename,
    Copy,
    Move,
}

impl FileActionKind {
    pub fn title(self) -> &'static str {
        match self {
            FileActionKind::Rename => " Rename File ",
            FileActionKind::Copy => " Copy File ",
            FileActionKind::Move => " Move File ",
        }
    }

    fn done_verb(self) -> &'static str {
        match self {
            FileActionKind::Rename => "Renamed",
            FileActionKind::Copy => "Copied",
            FileActionKind::Move => "Moved",
        }
    }
}

pub struct FileActionPopup {
    pub kind: FileActionKind,
    pub source: PathBuf,
    pub input: String,
    pub cursor: usize,
    pub rect: Option<(u16, u16, u16, u16)>,
}

#[derive(Clone)]
struct ExplorerEntry {
    rendered_label: String,
    path: PathBuf,
    is_dir: bool,
    expanded: bool,
    parent: Option<PathBuf>,
    is_parent_link: bool,
}

pub struct PalettePopup {
    pub selected: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InfoTab {
    Schema,
    Statistics,
    Metadata,
}

impl InfoTab {
    pub const ALL: [InfoTab; 3] = [InfoTab::Schema, InfoTab::Statistics, InfoTab::Metadata];

    pub fn label(self) -> &'static str {
        match self {
            InfoTab::Schema => "Schema",
            InfoTab::Statistics => "Statistics",
            InfoTab::Metadata => "Metadata",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Scope,
    Input,
    Results,
}

#[derive(Clone)]
pub enum SearchScope {
    Global,
    Column(usize, String), // (col_index, col_name)
}

pub struct SearchState {
    pub mode: SearchMode,
    pub scope: SearchScope,
    pub scope_selected: usize, // selection index in scope popup
    pub scope_scroll: usize,   // scroll offset for scope popup
    pub scope_inner_rect: Option<(u16, u16, u16, u16)>, // (x, y, w, h) of inner area
    pub input: String,
    pub cursor: usize,
    pub results: Option<SearchResults>,
    pub result_offset: usize,
}

pub struct ExportPopup {
    pub source: PathBuf,
    pub input: String,
    pub cursor: usize,
    pub rect: Option<(u16, u16, u16, u16)>,
    pub error: Option<String>,
}

pub struct ImportPopup {
    pub input: String,
    pub cursor: usize,
    pub target: String,
    pub target_cursor: usize,
    pub active_field: ImportField,
    pub rect: Option<(u16, u16, u16, u16)>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ImportField {
    Source,
    Target,
}

#[derive(Clone)]
pub struct SortState {
    pub col_index: usize,
    pub col_name: String,
    pub ascending: bool,
    pub indices: Vec<usize>,
}

#[derive(Clone)]
pub enum InfoLineKind {
    Header,
    Label,
    Value,
    Separator,
    Plain,
}

#[derive(Clone)]
pub struct InfoLine {
    pub kind: InfoLineKind,
    pub text: String,
}

impl InfoLine {
    fn header(text: impl Into<String>) -> Self {
        Self { kind: InfoLineKind::Header, text: text.into() }
    }
    fn label(text: impl Into<String>) -> Self {
        Self { kind: InfoLineKind::Label, text: text.into() }
    }
    fn value(text: impl Into<String>) -> Self {
        Self { kind: InfoLineKind::Value, text: text.into() }
    }
    fn sep() -> Self {
        Self { kind: InfoLineKind::Separator, text: String::new() }
    }
    fn plain(text: impl Into<String>) -> Self {
        Self { kind: InfoLineKind::Plain, text: text.into() }
    }
}

pub struct InfoPopup {
    pub title: String,
    pub lines: Vec<String>,
    pub scroll: usize,
    pub rect: Option<(u16, u16, u16, u16)>,
}

pub struct ProgressPopup {
    pub title: String,
    pub message: String,
    pub started: Instant,
    receiver: mpsc::Receiver<anyhow::Result<String>>,
    pub done_message: Option<String>,
    pub done_at: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LoadedFileType {
    Parquet,
    Csv,
}

pub struct LoadedParquet {
    pub file_type: LoadedFileType,
    pub path: PathBuf,
    pub schema_lines: Vec<String>,
    pub total_rows: usize,
    pub total_cols: usize,
    pub row_offset: usize,
    pub col_offset: usize,
    pub viewport_rows: usize,
    pub viewport_cols: usize,
    pub cache_row_start: usize,
    pub cache_col_start: usize,
    pub cache_columns: Vec<String>,
    pub cache_rows: Vec<Vec<String>>,
    pub stats_lines: Option<Vec<InfoLine>>,
    pub metadata_lines: Option<Vec<InfoLine>>,
    pub sort_state: Option<SortState>,
}

pub struct PreviewRender {
    pub header: Vec<String>,
    pub rows: Vec<(usize, Vec<String>)>,
    pub message: Option<String>,
}

pub struct App {
    pub root: PathBuf,
    pub explorer_root: PathBuf,
    pub files: Vec<PathBuf>,
    file_labels: Vec<String>,
    file_is_dir: Vec<bool>,
    file_is_parent_link: Vec<bool>,
    file_expanded: Vec<bool>,
    file_parent: Vec<Option<PathBuf>>,
    pub selected: usize,
    pub file_scroll: usize,
    explorer_expanded_dirs: HashSet<PathBuf>,
    pub loaded: Option<LoadedParquet>,
    pub active_pane: ActivePane,
    pub status: String,
    pub status_at: Instant,
    pub theme: Theme,
    pub palette_theme: PaletteTheme,
    pub file_action_popup: Option<FileActionPopup>,
    pub palette_popup: Option<PalettePopup>,
    pub open_menu: Option<OpenMenu>,
    pub menu_selected: usize,
    delete_armed: Option<(PathBuf, Instant)>,
    last_file_click: Option<(usize, Instant)>,
    mouse_regions: Option<MouseRegions>,
    top_menu_regions: Option<[(OpenMenu, Rect); 4]>,
    menu_item_regions: Vec<Rect>,
    quit_requested: bool,
    info_tab_regions: Vec<(InfoTab, Rect)>,
    pub info_tab: InfoTab,
    pub info_scroll: usize,
    pub search_state: Option<SearchState>,
    pub export_popup: Option<ExportPopup>,
    pub import_popup: Option<ImportPopup>,
    pub info_popup: Option<InfoPopup>,
    pub progress_popup: Option<ProgressPopup>,
    palette_item_regions: Vec<Rect>,
    header_col_regions: Vec<(usize, Rect)>, // (absolute col_index, rect)
}

impl App {
    pub fn new(root: PathBuf) -> anyhow::Result<Self> {
        let palette_theme = Self::load_palette_theme_setting();
        let mut app = Self {
            root,
            explorer_root: PathBuf::new(),
            files: Vec::new(),
            file_labels: Vec::new(),
            file_is_dir: Vec::new(),
            file_is_parent_link: Vec::new(),
            file_expanded: Vec::new(),
            file_parent: Vec::new(),
            selected: 0,
            file_scroll: 0,
            explorer_expanded_dirs: HashSet::new(),
            loaded: None,
            active_pane: ActivePane::Files,
            status:
                "Tab: pane | Enter: open/toggle | Ctrl+P: Palette | /: search | Ctrl+E: export | Ctrl+Q: quit"
                    .to_string(),
            status_at: Instant::now(),
            theme: palette_theme.theme(),
            palette_theme,
            file_action_popup: None,
            palette_popup: None,
            open_menu: None,
            menu_selected: 0,
            delete_armed: None,
            last_file_click: None,
            mouse_regions: None,
            top_menu_regions: None,
            menu_item_regions: Vec::new(),
            quit_requested: false,
            info_tab_regions: Vec::new(),
            info_tab: InfoTab::Schema,
            info_scroll: 0,
            search_state: None,
            export_popup: None,
            import_popup: None,
            info_popup: None,
            progress_popup: None,
            palette_item_regions: Vec::new(),
            header_col_regions: Vec::new(),
        };
        app.explorer_root = app.root.clone();
        app.explorer_expanded_dirs.insert(app.explorer_root.clone());
        app.rescan_files()?;
        Ok(app)
    }

    /// Open a specific parquet file, expanding ancestor directories so it is
    /// visible in the file nav rooted at the current working directory.
    pub fn open_file(&mut self, path: &Path) -> anyhow::Result<()> {
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };
        let path = path
            .canonicalize()
            .with_context(|| format!("file not found: {}", path.display()))?;

        // Expand every ancestor directory between explorer_root and the file
        // so the file is visible in the tree.
        if let Some(parent) = path.parent() {
            let mut dir = parent.to_path_buf();
            while dir != self.explorer_root && dir.starts_with(&self.explorer_root) {
                self.explorer_expanded_dirs.insert(dir.clone());
                if !dir.pop() {
                    break;
                }
            }
            self.rescan_files()?;
        }

        self.select_path(&path);
        self.load_selected()?;
        self.active_pane = ActivePane::Preview;
        Ok(())
    }

    pub fn selected_file(&self) -> Option<&PathBuf> {
        self.files.get(self.selected)
    }

    fn selected_data_file(&self) -> Option<(PathBuf, LoadedFileType)> {
        let path = self.selected_file()?;
        if self
            .file_is_dir
            .get(self.selected)
            .copied()
            .unwrap_or(false)
        {
            return None;
        }
        let ext = path.extension().and_then(|ext| ext.to_str())?;
        if ext.eq_ignore_ascii_case("parquet") {
            Some((path.clone(), LoadedFileType::Parquet))
        } else if ext.eq_ignore_ascii_case("csv") {
            Some((path.clone(), LoadedFileType::Csv))
        } else {
            None
        }
    }

    pub fn display_path(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .display()
            .to_string()
    }

    pub fn rescan_files(&mut self) -> anyhow::Result<()> {
        let selected_path = self.selected_file().cloned();
        let select_parent_link = self
            .file_is_parent_link
            .get(self.selected)
            .copied()
            .unwrap_or(false);

        self.rebuild_explorer_entries(selected_path, select_parent_link);
        self.delete_armed = None;
        self.last_file_click = None;

        let dir_count = self.file_is_dir.iter().filter(|is_dir| **is_dir).count();
        let file_count = self.file_is_dir.iter().filter(|is_dir| !**is_dir).count();
        self.set_status(format!(
            "Explorer: {dir_count} dirs, {file_count} files in {}",
            self.display_path(&self.explorer_root)
        ));
        Ok(())
    }

    fn rebuild_explorer_entries(
        &mut self,
        selected_path: Option<PathBuf>,
        select_parent_link: bool,
    ) {
        self.explorer_expanded_dirs
            .retain(|path| path.starts_with(&self.explorer_root));
        self.explorer_expanded_dirs
            .insert(self.explorer_root.clone());

        let mut entries = Vec::new();
        if let Some(parent) = self.explorer_root.parent() {
            entries.push(ExplorerEntry {
                rendered_label: "..".to_string(),
                path: parent.to_path_buf(),
                is_dir: true,
                expanded: false,
                parent: None,
                is_parent_link: true,
            });
        }

        let children = Self::read_explorer_children(&self.explorer_root);
        let child_count = children.len();
        for (idx, (path, name, is_dir)) in children.into_iter().enumerate() {
            let is_last = idx + 1 == child_count;
            self.push_explorer_tree_entry(&mut entries, path, name, is_dir, "", is_last, None);
        }

        self.files = entries.iter().map(|entry| entry.path.clone()).collect();
        self.file_labels = entries
            .iter()
            .map(|entry| entry.rendered_label.clone())
            .collect();
        self.file_is_dir = entries.iter().map(|entry| entry.is_dir).collect();
        self.file_is_parent_link = entries.iter().map(|entry| entry.is_parent_link).collect();
        self.file_expanded = entries.iter().map(|entry| entry.expanded).collect();
        self.file_parent = entries.iter().map(|entry| entry.parent.clone()).collect();

        if self.files.is_empty() {
            self.selected = 0;
            self.loaded = None;
            self.file_scroll = 0;
            return;
        }

        self.selected = if select_parent_link {
            self.file_is_parent_link
                .iter()
                .position(|is_parent| *is_parent)
                .unwrap_or(0)
        } else if let Some(path) = selected_path {
            self.files
                .iter()
                .position(|candidate| *candidate == path)
                .unwrap_or_else(|| cmp::min(self.selected, self.files.len() - 1))
        } else {
            cmp::min(self.selected, self.files.len() - 1)
        };
        self.file_scroll = cmp::min(self.file_scroll, self.selected);
    }

    fn read_explorer_children(dir: &Path) -> Vec<(PathBuf, String, bool)> {
        let mut dirs = Vec::new();
        let mut files = Vec::new();

        if let Ok(read_dir) = fs::read_dir(dir) {
            for item in read_dir.flatten() {
                let path = item.path();
                let name = item.file_name().to_string_lossy().to_string();
                if name.starts_with('.') {
                    continue;
                }
                let is_dir = item
                    .file_type()
                    .map_or_else(|_| path.is_dir(), |ft| ft.is_dir());
                if is_dir {
                    dirs.push((path, name, true));
                } else {
                    let dominated = name.to_ascii_lowercase();
                    if dominated.ends_with(".parquet") || dominated.ends_with(".csv") {
                        files.push((path, name, false));
                    }
                }
            }
        }

        dirs.sort_by_key(|(_, name, _)| name.to_ascii_lowercase());
        files.sort_by_key(|(_, name, _)| name.to_ascii_lowercase());
        dirs.extend(files);
        dirs
    }

    fn push_explorer_tree_entry(
        &self,
        out: &mut Vec<ExplorerEntry>,
        path: PathBuf,
        name: String,
        is_dir: bool,
        prefix: &str,
        is_last: bool,
        parent: Option<PathBuf>,
    ) {
        let expanded = is_dir && self.explorer_expanded_dirs.contains(&path);
        let (branch, marker) = if is_dir {
            (
                if is_last { "`- " } else { "|- " },
                if expanded { "[-] " } else { "[+] " },
            )
        } else {
            ("", "       ")
        };

        out.push(ExplorerEntry {
            rendered_label: format!("{prefix}{branch}{marker}{name}"),
            path: path.clone(),
            is_dir,
            expanded,
            parent: parent.clone(),
            is_parent_link: false,
        });

        if is_dir && expanded {
            let children = Self::read_explorer_children(&path);
            let child_count = children.len();
            let child_prefix = format!("{prefix}{}", if is_last { "   " } else { "|  " });
            for (idx, (child_path, child_name, child_is_dir)) in children.into_iter().enumerate() {
                let child_is_last = idx + 1 == child_count;
                self.push_explorer_tree_entry(
                    out,
                    child_path,
                    child_name,
                    child_is_dir,
                    &child_prefix,
                    child_is_last,
                    Some(path.clone()),
                );
            }
        }
    }

    pub fn move_selection(&mut self, delta: isize) {
        if self.files.is_empty() {
            return;
        }

        let next = self.selected as isize + delta;
        let bounded = next.clamp(0, (self.files.len() - 1) as isize) as usize;
        self.selected = bounded;
        self.delete_armed = None;
        self.last_file_click = None;
    }

    pub fn ensure_selected_visible(&mut self, visible_rows: usize) {
        if visible_rows == 0 {
            return;
        }
        if self.selected < self.file_scroll {
            self.file_scroll = self.selected;
            return;
        }

        let window_end = self.file_scroll + visible_rows;
        if self.selected >= window_end {
            self.file_scroll = self.selected + 1 - visible_rows;
        }
    }

    pub fn visible_file_indices(&self, visible_rows: usize) -> (usize, usize) {
        let start = self.file_scroll;
        let end = cmp::min(start + visible_rows, self.files.len());
        (start, end)
    }

    pub fn file_label(&self, idx: usize) -> Option<&str> {
        self.file_labels.get(idx).map(String::as_str)
    }

    pub fn file_is_dir(&self, idx: usize) -> bool {
        self.file_is_dir.get(idx).copied().unwrap_or(false)
    }

    pub fn file_is_parent_link(&self, idx: usize) -> bool {
        self.file_is_parent_link.get(idx).copied().unwrap_or(false)
    }

    pub fn toggle_focus(&mut self) {
        self.active_pane = match self.active_pane {
            ActivePane::Files => ActivePane::Preview,
            ActivePane::Preview => ActivePane::Files,
        };
    }

    pub fn clear_mouse_regions(&mut self) {
        self.mouse_regions = None;
    }

    pub fn clear_top_menu_regions(&mut self) {
        self.top_menu_regions = None;
    }

    pub fn clear_menu_item_regions(&mut self) {
        self.menu_item_regions.clear();
    }

    pub fn set_palette_item_regions(&mut self, regions: Vec<(u16, u16, u16, u16)>) {
        self.palette_item_regions = regions
            .into_iter()
            .map(|(x, y, w, h)| Rect { x, y, width: w, height: h })
            .collect();
    }

    fn palette_item_at(&self, x: u16, y: u16) -> Option<usize> {
        self.palette_item_regions
            .iter()
            .position(|rect| rect.contains(x, y))
    }

    pub fn set_menu_item_regions(&mut self, regions: Vec<(u16, u16, u16, u16)>) {
        self.menu_item_regions = regions
            .into_iter()
            .map(|(x, y, w, h)| Rect { x, y, width: w, height: h })
            .collect();
    }

    pub fn update_top_menu_regions(
        &mut self,
        file: (u16, u16, u16, u16),
        view: (u16, u16, u16, u16),
        tools: (u16, u16, u16, u16),
        help: (u16, u16, u16, u16),
    ) {
        self.top_menu_regions = Some([
            (
                OpenMenu::File,
                Rect {
                    x: file.0,
                    y: file.1,
                    width: file.2,
                    height: file.3,
                },
            ),
            (
                OpenMenu::View,
                Rect {
                    x: view.0,
                    y: view.1,
                    width: view.2,
                    height: view.3,
                },
            ),
            (
                OpenMenu::Tools,
                Rect {
                    x: tools.0,
                    y: tools.1,
                    width: tools.2,
                    height: tools.3,
                },
            ),
            (
                OpenMenu::Help,
                Rect {
                    x: help.0,
                    y: help.1,
                    width: help.2,
                    height: help.3,
                },
            ),
        ]);
    }

    pub fn update_mouse_regions(
        &mut self,
        files_rect: (u16, u16, u16, u16),
        files_inner: (u16, u16, u16, u16),
        preview_rect: (u16, u16, u16, u16),
        rows_inner: (u16, u16, u16, u16),
    ) {
        self.mouse_regions = Some(MouseRegions {
            files_rect: Rect {
                x: files_rect.0,
                y: files_rect.1,
                width: files_rect.2,
                height: files_rect.3,
            },
            files_inner: Rect {
                x: files_inner.0,
                y: files_inner.1,
                width: files_inner.2,
                height: files_inner.3,
            },
            preview_rect: Rect {
                x: preview_rect.0,
                y: preview_rect.1,
                width: preview_rect.2,
                height: preview_rect.3,
            },
            rows_inner: Rect {
                x: rows_inner.0,
                y: rows_inner.1,
                width: rows_inner.2,
                height: rows_inner.3,
            },
        });
    }

    pub fn handle_menu_key(&mut self, key: KeyEvent) -> anyhow::Result<bool> {
        let Some(menu) = self.open_menu else {
            return Ok(false);
        };

        let items = menu.items();
        match key.code {
            KeyCode::Esc => {
                self.open_menu = None;
                self.set_status("Menu closed");
            }
            KeyCode::Up => {
                self.menu_move(-1, items);
            }
            KeyCode::Down => {
                self.menu_move(1, items);
            }
            KeyCode::Left => {
                let next = match menu {
                    OpenMenu::File => OpenMenu::Help,
                    OpenMenu::View => OpenMenu::File,
                    OpenMenu::Tools => OpenMenu::View,
                    OpenMenu::Help => OpenMenu::Tools,
                };
                self.open_menu = Some(next);
                self.menu_selected = 0;
            }
            KeyCode::Right => {
                let next = match menu {
                    OpenMenu::File => OpenMenu::View,
                    OpenMenu::View => OpenMenu::Tools,
                    OpenMenu::Tools => OpenMenu::Help,
                    OpenMenu::Help => OpenMenu::File,
                };
                self.open_menu = Some(next);
                self.menu_selected = 0;
            }
            KeyCode::Enter => {
                let label = items.get(self.menu_selected).map(|e| e.label);
                self.open_menu = None;
                if let Some(label) = label {
                    self.execute_menu_item(label)?;
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn menu_move(&mut self, dir: isize, items: &[MenuEntry]) {
        if items.is_empty() {
            return;
        }
        let len = items.len() as isize;
        let mut pos = self.menu_selected as isize + dir;
        // wrap and skip separators
        for _ in 0..len {
            pos = pos.rem_euclid(len);
            if !items[pos as usize].is_separator {
                break;
            }
            pos += dir;
        }
        self.menu_selected = pos.rem_euclid(len) as usize;
    }

    fn execute_menu_item(&mut self, label: &str) -> anyhow::Result<()> {
        match label {
            "Rename" => self.open_file_action(FileActionKind::Rename),
            "Copy" => self.open_file_action(FileActionKind::Copy),
            "Move" => self.open_file_action(FileActionKind::Move),
            "Delete" => self.arm_or_delete_selected()?,
            "Import from CSV" => self.open_import_popup(),
            "Export to CSV" => self.open_export_popup(),
            "Quit" => self.request_quit(),
            "Metadata" => self.switch_info_tab(InfoTab::Metadata)?,
            "Statistics" => self.switch_info_tab(InfoTab::Statistics)?,
            "Search" => self.open_search(),
            "Palette" => self.open_palette_popup(),
            "Refresh" => self.rescan_files()?,
            "Keybindings" => self.open_keybindings_popup(),
            "About rdb" => self.open_about_popup(),
            _ => {}
        }
        Ok(())
    }

    pub fn consume_quit_requested(&mut self) -> bool {
        if self.quit_requested {
            self.quit_requested = false;
            return true;
        }
        false
    }

    pub fn load_selected(&mut self) -> anyhow::Result<()> {
        let Some(path) = self.selected_file().cloned() else {
            self.set_status("No entry selected");
            return Ok(());
        };

        if self
            .file_is_parent_link
            .get(self.selected)
            .copied()
            .unwrap_or(false)
        {
            self.open_explorer_parent();
            return Ok(());
        }

        if self
            .file_is_dir
            .get(self.selected)
            .copied()
            .unwrap_or(false)
        {
            if self
                .file_expanded
                .get(self.selected)
                .copied()
                .unwrap_or(false)
            {
                self.explorer_expanded_dirs.remove(&path);
            } else {
                self.explorer_expanded_dirs.insert(path.clone());
            }
            self.rebuild_explorer_entries(Some(path), false);
            return Ok(());
        }

        let ext = path.extension().and_then(|ext| ext.to_str());
        let file_type = if ext.is_some_and(|e| e.eq_ignore_ascii_case("parquet")) {
            LoadedFileType::Parquet
        } else if ext.is_some_and(|e| e.eq_ignore_ascii_case("csv")) {
            LoadedFileType::Csv
        } else {
            self.set_status(format!("Unsupported file: {}", self.display_path(&path)));
            return Ok(());
        };

        let meta = match file_type {
            LoadedFileType::Parquet => load_parquet_meta(&path)
                .with_context(|| format!("unable to load {}", path.display()))?,
            LoadedFileType::Csv => load_csv_meta(&path)
                .with_context(|| format!("unable to load {}", path.display()))?,
        };
        self.loaded = Some(LoadedParquet::from_meta(path, meta, file_type));
        self.info_tab = InfoTab::Schema;
        self.info_scroll = 0;
        self.ensure_preview_cache(true)?;

        // Pre-build statistics and metadata so tab switching is instant (parquet only)
        if file_type == LoadedFileType::Parquet {
            self.prebuild_info_tabs();
        }

        if let Some(loaded) = &self.loaded {
            self.set_status(format!("Loaded {}", self.display_path(&loaded.path)));
        }
        Ok(())
    }

    pub fn expand_selected_directory(&mut self) {
        let Some(path) = self.selected_file().cloned() else {
            return;
        };
        if self.file_is_parent_link(self.selected) {
            return;
        }
        if !self.file_is_dir(self.selected) {
            return;
        }
        if !self
            .file_expanded
            .get(self.selected)
            .copied()
            .unwrap_or(false)
        {
            self.explorer_expanded_dirs.insert(path.clone());
            self.rebuild_explorer_entries(Some(path), false);
        }
    }

    pub fn collapse_selected_directory_or_parent(&mut self) {
        let Some(path) = self.selected_file().cloned() else {
            return;
        };
        if self.file_is_parent_link(self.selected) {
            self.open_explorer_parent();
            return;
        }
        if self.file_is_dir(self.selected)
            && self
                .file_expanded
                .get(self.selected)
                .copied()
                .unwrap_or(false)
        {
            self.explorer_expanded_dirs.remove(&path);
            self.rebuild_explorer_entries(Some(path), false);
            return;
        }
        if let Some(parent) = self.file_parent.get(self.selected).cloned().flatten() {
            self.rebuild_explorer_entries(Some(parent), false);
        }
    }

    pub fn set_preview_viewport(
        &mut self,
        table_height: usize,
        table_width: usize,
    ) -> anyhow::Result<()> {
        let Some(loaded) = self.loaded.as_mut() else {
            return Ok(());
        };

        let rows = cmp::max(table_height, 1);
        let cols = fit_visible_columns(table_width, CELL_CHAR_LIMIT / 2);

        if loaded.viewport_rows != rows || loaded.viewport_cols != cols {
            loaded.viewport_rows = rows;
            loaded.viewport_cols = cols;
            self.ensure_preview_cache(false)?;
        }
        Ok(())
    }

    pub fn scroll_preview_rows(&mut self, delta: isize) -> anyhow::Result<()> {
        let Some(loaded) = self.loaded.as_mut() else {
            return Ok(());
        };

        if loaded.total_rows == 0 {
            loaded.row_offset = 0;
            return Ok(());
        }

        let next = loaded.row_offset as isize + delta;
        loaded.row_offset = next.clamp(0, loaded.total_rows.saturating_sub(1) as isize) as usize;
        self.ensure_preview_cache(false)
    }

    pub fn page_preview_rows(&mut self, pages: isize) -> anyhow::Result<()> {
        let page_size = self
            .loaded
            .as_ref()
            .map(|loaded| cmp::max(loaded.viewport_rows.saturating_sub(1), 1))
            .unwrap_or(20);
        self.scroll_preview_rows(pages * page_size as isize)
    }

    pub fn scroll_preview_cols(&mut self, delta: isize) -> anyhow::Result<()> {
        let Some(loaded) = self.loaded.as_mut() else {
            return Ok(());
        };

        if loaded.total_cols == 0 {
            loaded.col_offset = 0;
            return Ok(());
        }

        let next = loaded.col_offset as isize + delta;
        loaded.col_offset = next.clamp(0, loaded.total_cols.saturating_sub(1) as isize) as usize;
        self.ensure_preview_cache(false)
    }

    pub fn open_file_action(&mut self, kind: FileActionKind) {
        let Some((source, _)) = self.selected_data_file() else {
            self.set_status("Select a data file first");
            return;
        };

        let source_rel = self.display_path(&source);
        let default_target = match kind {
            FileActionKind::Rename => source_rel,
            FileActionKind::Copy => format!("{source_rel}.copy"),
            FileActionKind::Move => {
                let name = source
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("file.parquet");
                format!("moved/{name}")
            }
        };

        self.file_action_popup = Some(FileActionPopup {
            kind,
            source,
            cursor: default_target.chars().count(),
            input: default_target,
            rect: None,
        });
    }

    pub fn handle_file_action_key(&mut self, key: KeyEvent) -> anyhow::Result<bool> {
        let Some(popup) = self.file_action_popup.as_mut() else {
            return Ok(false);
        };

        match key.code {
            KeyCode::Esc => {
                self.file_action_popup = None;
                self.set_status("Action canceled");
            }
            KeyCode::Enter => {
                self.execute_file_action()?;
            }
            KeyCode::Backspace => {
                if popup.cursor > 0 {
                    let from = char_to_byte_index(&popup.input, popup.cursor - 1);
                    let to = char_to_byte_index(&popup.input, popup.cursor);
                    popup.input.replace_range(from..to, "");
                    popup.cursor -= 1;
                }
            }
            KeyCode::Delete => {
                if popup.cursor < popup.input.chars().count() {
                    let from = char_to_byte_index(&popup.input, popup.cursor);
                    let to = char_to_byte_index(&popup.input, popup.cursor + 1);
                    popup.input.replace_range(from..to, "");
                }
            }
            KeyCode::Left => {
                popup.cursor = popup.cursor.saturating_sub(1);
            }
            KeyCode::Right => {
                popup.cursor = cmp::min(popup.cursor + 1, popup.input.chars().count());
            }
            KeyCode::Home => {
                popup.cursor = 0;
            }
            KeyCode::End => {
                popup.cursor = popup.input.chars().count();
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                let byte_idx = char_to_byte_index(&popup.input, popup.cursor);
                popup.input.insert(byte_idx, c);
                popup.cursor += 1;
            }
            _ => return Ok(false),
        }

        Ok(true)
    }

    pub fn open_palette_popup(&mut self) {
        self.open_menu = None;
        self.palette_popup = Some(PalettePopup {
            selected: self.palette_theme.index(),
        });
    }

    pub fn handle_palette_key(&mut self, key: KeyEvent) -> bool {
        let Some(popup) = self.palette_popup.as_mut() else {
            return false;
        };

        match key.code {
            KeyCode::Esc => {
                self.palette_popup = None;
                self.set_status("Palette canceled");
            }
            KeyCode::Up => {
                popup.selected = popup.selected.saturating_sub(1);
            }
            KeyCode::Down => {
                popup.selected = cmp::min(popup.selected + 1, PaletteTheme::ALL.len() - 1);
            }
            KeyCode::Home => {
                popup.selected = 0;
            }
            KeyCode::End => {
                popup.selected = PaletteTheme::ALL.len() - 1;
            }
            KeyCode::Char(c) if ('1'..='9').contains(&c) => {
                let idx = c as usize - '1' as usize;
                if idx < PaletteTheme::ALL.len() {
                    popup.selected = idx;
                }
            }
            KeyCode::Enter => {
                let idx = popup.selected;
                let theme = PaletteTheme::from_index(idx);
                self.apply_palette(theme);
                self.palette_popup = None;
            }
            _ => return false,
        }

        true
    }

    pub fn handle_mouse_event(
        &mut self,
        mouse: MouseEvent,
        terminal_width: u16,
        terminal_height: u16,
    ) -> anyhow::Result<()> {
        // Handle import popup — click outside closes it
        if let Some(popup) = &self.import_popup {
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                let inside = popup.rect.map_or(false, |(x, y, w, h)| {
                    mouse.column >= x
                        && mouse.row >= y
                        && mouse.column < x.saturating_add(w)
                        && mouse.row < y.saturating_add(h)
                });
                if !inside {
                    self.import_popup = None;
                    self.set_status("Import cancelled");
                }
            }
            return Ok(());
        }

        // Handle export popup — click outside closes it
        if let Some(popup) = &self.export_popup {
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                let inside = popup.rect.map_or(false, |(x, y, w, h)| {
                    mouse.column >= x
                        && mouse.row >= y
                        && mouse.column < x.saturating_add(w)
                        && mouse.row < y.saturating_add(h)
                });
                if !inside {
                    self.export_popup = None;
                    self.set_status("Export cancelled");
                }
            }
            return Ok(());
        }

        // Handle file action popup — click outside closes it
        if let Some(popup) = &self.file_action_popup {
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                let inside = popup.rect.map_or(false, |(x, y, w, h)| {
                    mouse.column >= x
                        && mouse.row >= y
                        && mouse.column < x.saturating_add(w)
                        && mouse.row < y.saturating_add(h)
                });
                if !inside {
                    self.file_action_popup = None;
                }
            }
            return Ok(());
        }

        // Handle search scope popup mouse clicks
        if let Some(state) = &self.search_state {
            if matches!(state.mode, SearchMode::Scope) {
                if let Some((ix, iy, iw, ih)) = state.scope_inner_rect {
                    let inside = mouse.column >= ix
                        && mouse.row >= iy
                        && mouse.column < ix.saturating_add(iw)
                        && mouse.row < iy.saturating_add(ih);
                    if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                        if inside {
                            let row = (mouse.row - iy) as usize;
                            let idx = state.scope_scroll + row;
                            let options = self.search_scope_options();
                            if let Some((scope, _)) = options.get(idx) {
                                let scope = scope.clone();
                                if let Some(state) = self.search_state.as_mut() {
                                    state.scope = scope;
                                    state.scope_selected = idx;
                                    state.mode = SearchMode::Input;
                                    state.input.clear();
                                    state.cursor = 0;
                                }
                            }
                        } else {
                            self.search_state = None;
                            self.set_status("Search cancelled");
                        }
                    } else if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        if inside {
                            let row = (mouse.row - iy) as usize;
                            let idx = state.scope_scroll + row;
                            if let Some(state) = self.search_state.as_mut() {
                                state.scope_selected = idx;
                            }
                        }
                    }
                }
                return Ok(());
            }
        }

        // Handle info popup (Keybindings / About) — click outside closes it
        if let Some(popup) = &self.info_popup {
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                let inside = popup.rect.map_or(false, |(x, y, w, h)| {
                    mouse.column >= x
                        && mouse.row >= y
                        && mouse.column < x.saturating_add(w)
                        && mouse.row < y.saturating_add(h)
                });
                if !inside {
                    self.info_popup = None;
                }
            }
            return Ok(());
        }

        // Handle palette popup mouse clicks
        if self.palette_popup.is_some() {
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                if let Some(idx) = self.palette_item_at(mouse.column, mouse.row) {
                    if idx < PaletteTheme::ALL.len() {
                        let theme = PaletteTheme::from_index(idx);
                        self.apply_palette(theme);
                        self.palette_popup = None;
                    }
                } else {
                    // Click outside palette closes it
                    self.palette_popup = None;
                }
            } else if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                // Highlight on mouse down
                if let Some(idx) = self.palette_item_at(mouse.column, mouse.row) {
                    if idx < PaletteTheme::ALL.len() {
                        if let Some(popup) = self.palette_popup.as_mut() {
                            popup.selected = idx;
                        }
                    }
                }
            }
            return Ok(());
        }

        if matches!(
            mouse.kind,
            MouseEventKind::Down(MouseButton::Left) | MouseEventKind::Up(MouseButton::Left)
        ) {
            // Check clicks on menu dropdown items
            if self.open_menu.is_some() {
                if let Some(idx) = self.menu_item_at(mouse.column, mouse.row) {
                    if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                        let menu = self.open_menu.unwrap();
                        let items = menu.items();
                        if let Some(entry) = items.get(idx) {
                            if !entry.is_separator {
                                let label = entry.label;
                                self.open_menu = None;
                                self.execute_menu_item(label)?;
                            }
                        }
                    }
                    return Ok(());
                }
            }

            // Check clicks on top menu bar labels
            if let Some(target) = self.top_menu_target_at(mouse.column, mouse.row) {
                if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                    self.apply_top_menu_action(target);
                }
                return Ok(());
            }

            // Click outside open menu closes it
            if self.open_menu.is_some() && matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                self.open_menu = None;
            }

            // Info tab clicks
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                if let Some(tab) = self.info_tab_at(mouse.column, mouse.row) {
                    self.switch_info_tab(tab)?;
                    return Ok(());
                }
            }

            // Column header clicks (sort)
            if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                if let Some(col_index) = self.header_col_at(mouse.column, mouse.row) {
                    self.toggle_sort_column(col_index)?;
                    return Ok(());
                }
            }
        }

        let Some((files_rect, files_inner, preview_rect, rows_rect)) = self
            .mouse_regions
            .map(|regions| {
                (
                    regions.files_rect,
                    regions.files_inner,
                    regions.preview_rect,
                    regions.rows_inner,
                )
            })
            .or_else(|| layout_rects(terminal_width, terminal_height))
        else {
            return Ok(());
        };

        let x = mouse.column;
        let y = mouse.row;

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) | MouseEventKind::Drag(MouseButton::Left) => {
                if files_inner.contains(x, y) {
                    self.active_pane = ActivePane::Files;
                    self.select_file_at_mouse_row(y, files_inner, false);
                } else if preview_rect.contains(x, y) || rows_rect.contains(x, y) {
                    self.active_pane = ActivePane::Preview;
                } else if files_rect.contains(x, y) {
                    self.active_pane = ActivePane::Files;
                }
            }
            MouseEventKind::Up(MouseButton::Left) => {
                if files_inner.contains(x, y) {
                    self.active_pane = ActivePane::Files;
                    self.select_file_at_mouse_row(y, files_inner, true);
                } else if preview_rect.contains(x, y) || rows_rect.contains(x, y) {
                    self.active_pane = ActivePane::Preview;
                } else if files_rect.contains(x, y) {
                    self.active_pane = ActivePane::Files;
                }
            }
            MouseEventKind::ScrollUp => {
                if files_rect.contains(x, y) {
                    self.active_pane = ActivePane::Files;
                    self.move_selection(-1);
                } else if preview_rect.contains(x, y) {
                    self.active_pane = ActivePane::Preview;
                    self.scroll_preview_rows(-3)?;
                }
            }
            MouseEventKind::ScrollDown => {
                if files_rect.contains(x, y) {
                    self.active_pane = ActivePane::Files;
                    self.move_selection(1);
                } else if preview_rect.contains(x, y) {
                    self.active_pane = ActivePane::Preview;
                    self.scroll_preview_rows(3)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    pub fn arm_or_delete_selected(&mut self) -> anyhow::Result<()> {
        let Some((path, _)) = self.selected_data_file() else {
            self.set_status("Select a data file first");
            return Ok(());
        };

        if let Some((armed_path, armed_at)) = &self.delete_armed {
            if *armed_path == path && armed_at.elapsed() < Duration::from_secs(3) {
                fs::remove_file(&path)
                    .with_context(|| format!("failed to delete {}", path.display()))?;
                if self
                    .loaded
                    .as_ref()
                    .is_some_and(|loaded| loaded.path == path)
                {
                    self.loaded = None;
                }
                self.delete_armed = None;
                self.rescan_files()?;
                self.set_status(format!("Deleted {}", self.display_path(&path)));
                return Ok(());
            }
        }

        self.delete_armed = Some((path.clone(), Instant::now()));
        self.set_status(format!(
            "Press d again within 3s to delete {}",
            self.display_path(&path)
        ));
        Ok(())
    }

    pub fn build_preview_render(&self, visible_rows: usize, visible_cols: usize) -> PreviewRender {
        let Some(loaded) = &self.loaded else {
            return PreviewRender {
                header: Vec::new(),
                rows: Vec::new(),
                message: Some("No parquet loaded".to_string()),
            };
        };

        if loaded.total_cols == 0 {
            return PreviewRender {
                header: Vec::new(),
                rows: Vec::new(),
                message: Some("Parquet has zero columns".to_string()),
            };
        }

        let max_visible_cols = cmp::min(
            visible_cols,
            loaded.total_cols.saturating_sub(loaded.col_offset),
        );
        let col_rel = loaded.col_offset.saturating_sub(loaded.cache_col_start);

        if col_rel >= loaded.cache_columns.len() {
            return PreviewRender {
                header: Vec::new(),
                rows: Vec::new(),
                message: Some("Loading columns...".to_string()),
            };
        }

        let usable_cols = cmp::min(max_visible_cols, loaded.cache_columns.len() - col_rel);
        let header = loaded.cache_columns[col_rel..col_rel + usable_cols].to_vec();

        let mut rows = Vec::new();
        for global_row in
            loaded.row_offset..cmp::min(loaded.row_offset + visible_rows, loaded.total_rows)
        {
            if global_row < loaded.cache_row_start {
                break;
            }
            let row_rel = global_row - loaded.cache_row_start;
            let Some(cached_row) = loaded.cache_rows.get(row_rel) else {
                break;
            };
            if col_rel >= cached_row.len() {
                break;
            }

            let end = cmp::min(col_rel + usable_cols, cached_row.len());
            let cells = cached_row[col_rel..end].to_vec();
            rows.push((global_row, cells));
        }

        PreviewRender {
            header,
            rows,
            message: None,
        }
    }

    // ------------------------------------------------------------------
    // Sort
    // ------------------------------------------------------------------

    pub fn toggle_sort_column(&mut self, col_index: usize) -> anyhow::Result<()> {
        let Some(loaded) = &self.loaded else {
            self.set_status("Load a file first");
            return Ok(());
        };
        if col_index >= loaded.total_cols {
            return Ok(());
        }

        let path = loaded.path.clone();

        // Cycle: none → ascending → descending → none
        if let Some(sort) = &loaded.sort_state {
            if sort.col_index == col_index {
                if sort.ascending {
                    // Switch to descending by reversing the cached ascending indices
                    let mut indices = sort.indices.clone();
                    indices.reverse();
                    let col_name = sort.col_name.clone();
                    if let Some(loaded) = self.loaded.as_mut() {
                        loaded.sort_state = Some(SortState {
                            col_index,
                            col_name: col_name.clone(),
                            ascending: false,
                            indices,
                        });
                    }
                    self.set_status(format!("Sorted by {col_name} (descending)"));
                } else {
                    // Clear sort
                    if let Some(loaded) = self.loaded.as_mut() {
                        loaded.sort_state = None;
                    }
                    self.set_status("Sort cleared");
                }
                self.ensure_preview_cache(true)?;
                return Ok(());
            }
        }

        // New column or no sort — ascending
        let col_name = loaded
            .schema_lines
            .get(col_index)
            .map(|line| line.split(':').next().unwrap_or("?").to_string())
            .unwrap_or_else(|| format!("col{col_index}"));
        let file_type = loaded.file_type;

        self.set_status(format!("Sorting by {col_name}..."));
        let indices = match file_type {
            LoadedFileType::Parquet => compute_sort_indices(&path, col_index, false)?,
            LoadedFileType::Csv => compute_csv_sort_indices(&path, col_index, false)?,
        };
        if let Some(loaded) = self.loaded.as_mut() {
            loaded.sort_state = Some(SortState {
                col_index,
                col_name: col_name.clone(),
                ascending: true,
                indices,
            });
        }
        self.set_status(format!("Sorted by {col_name} (ascending)"));
        self.ensure_preview_cache(true)?;
        Ok(())
    }

    /// Toggle sort on the leftmost visible column (keyboard shortcut).
    pub fn toggle_sort_current_column(&mut self) -> anyhow::Result<()> {
        let col_index = self
            .loaded
            .as_ref()
            .map(|l| l.col_offset)
            .unwrap_or(0);
        self.toggle_sort_column(col_index)
    }

    pub fn set_header_col_regions(&mut self, regions: Vec<(usize, u16, u16, u16, u16)>) {
        self.header_col_regions = regions
            .into_iter()
            .map(|(col, x, y, w, h)| (col, Rect { x, y, width: w, height: h }))
            .collect();
    }

    fn header_col_at(&self, x: u16, y: u16) -> Option<usize> {
        self.header_col_regions
            .iter()
            .find_map(|(col, rect)| rect.contains(x, y).then_some(*col))
    }

    pub fn current_status_line(&self, width: usize) -> String {
        let selected = self
            .selected_file()
            .map(|path| self.display_path(path))
            .unwrap_or_else(|| "(no file)".to_string());

        let preview_stats = self.loaded.as_ref().map_or_else(
            || "not loaded".to_string(),
            |loaded| {
                format!(
                    "rows: {}, cols: {}, row: {}, col: {}",
                    loaded.total_rows,
                    loaded.total_cols,
                    loaded.row_offset + 1,
                    loaded.col_offset + 1,
                )
            },
        );

        let focus = match self.active_pane {
            ActivePane::Files => "Files",
            ActivePane::Preview => "Preview",
        };

        pad_or_clip(
            &format!(" {selected} | {preview_stats} | Focus: {focus} "),
            width,
        )
    }

    pub fn current_message_line(&self, width: usize) -> String {
        if self.status_at.elapsed() > Duration::from_secs(5) {
            return self.preview_window_legend(width);
        }
        pad_or_clip(&self.status, width)
    }

    pub fn set_status(&mut self, text: impl Into<String>) {
        self.status = text.into();
        self.status_at = Instant::now();
    }

    pub fn apply_result(&mut self, result: anyhow::Result<()>) {
        if let Err(err) = result {
            self.set_status(format!("Error: {err:#}"));
        }
    }

    // ------------------------------------------------------------------
    // Info panel tabs (Schema / Statistics / Metadata)
    // ------------------------------------------------------------------

    pub fn switch_info_tab(&mut self, tab: InfoTab) -> anyhow::Result<()> {
        if self.info_tab == tab {
            return Ok(());
        }
        self.info_tab = tab;
        self.info_scroll = 0;
        self.ensure_info_tab_data()?;
        Ok(())
    }

    /// Pre-compute statistics and metadata so tab switching is instant.
    /// Errors are silently ignored — tabs fall back to lazy loading on click.
    fn prebuild_info_tabs(&mut self) {
        let saved_tab = self.info_tab;
        for tab in [InfoTab::Statistics, InfoTab::Metadata] {
            self.info_tab = tab;
            let _ = self.ensure_info_tab_data();
        }
        self.info_tab = saved_tab;
    }

    pub fn ensure_info_tab_data(&mut self) -> anyhow::Result<()> {
        let Some(loaded) = &self.loaded else {
            return Ok(());
        };
        if loaded.file_type == LoadedFileType::Csv {
            return Ok(()); // CSV files don't have info tabs
        }
        let path = loaded.path.clone();

        match self.info_tab {
            InfoTab::Schema => {} // always available
            InfoTab::Statistics => {
                if loaded.stats_lines.is_none() {
                    let stats = compute_column_statistics(&path)?;
                    let lines = Self::format_stats_lines(&stats);
                    if let Some(loaded) = self.loaded.as_mut() {
                        loaded.stats_lines = Some(lines);
                    }
                }
            }
            InfoTab::Metadata => {
                if loaded.metadata_lines.is_none() {
                    let info = load_parquet_file_info(&path)?;
                    let lines = Self::format_metadata_lines(&info);
                    if let Some(loaded) = self.loaded.as_mut() {
                        loaded.metadata_lines = Some(lines);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn set_info_tab_regions(&mut self, regions: Vec<(InfoTab, u16, u16, u16, u16)>) {
        self.info_tab_regions = regions
            .into_iter()
            .map(|(tab, x, y, w, h)| (tab, Rect { x, y, width: w, height: h }))
            .collect();
    }

    fn info_tab_at(&self, x: u16, y: u16) -> Option<InfoTab> {
        self.info_tab_regions
            .iter()
            .find_map(|(tab, rect)| rect.contains(x, y).then_some(*tab))
    }

    pub fn scroll_info_panel(&mut self, delta: isize) {
        let new = self.info_scroll as isize + delta;
        self.info_scroll = new.max(0) as usize;
    }

    pub fn info_lines_for_render(&self) -> Vec<InfoLine> {
        let Some(loaded) = &self.loaded else {
            return vec![InfoLine::plain("Press Enter on a parquet file to load")];
        };
        match self.info_tab {
            InfoTab::Schema => Self::format_schema_lines(loaded),
            InfoTab::Statistics => loaded
                .stats_lines
                .clone()
                .unwrap_or_else(|| vec![InfoLine::plain("Press 's' or click Statistics tab to load")]),
            InfoTab::Metadata => loaded
                .metadata_lines
                .clone()
                .unwrap_or_else(|| vec![InfoLine::plain("Press 'i' or click Metadata tab to load")]),
        }
    }

    fn format_schema_lines(loaded: &LoadedParquet) -> Vec<InfoLine> {
        let mut lines = Vec::new();
        lines.push(InfoLine::header(format!(
            " {:>4}  {:30} {}",
            "#", "Column Name", "Data Type"
        )));
        lines.push(InfoLine::sep());
        for (idx, raw) in loaded.schema_lines.iter().enumerate() {
            let (name, dtype) = raw
                .split_once(": ")
                .unwrap_or((raw.as_str(), ""));
            lines.push(InfoLine::value(format!(
                " {:>4}  {:30} {}",
                idx + 1,
                truncate_str(name, 30),
                dtype,
            )));
        }
        lines.push(InfoLine::sep());
        lines.push(InfoLine::label(format!(
            " {} columns, {} rows",
            loaded.total_cols, loaded.total_rows
        )));
        lines
    }

    fn format_stats_lines(stats: &[ColumnStatistics]) -> Vec<InfoLine> {
        let mut lines = Vec::new();
        lines.push(InfoLine::header(format!(
            " {:20} {:12} {:>8} {:>8} {:>14} {:>14} {:>14}",
            "Column", "Type", "Total", "Nulls", "Min", "Max", "Mean"
        )));
        lines.push(InfoLine::sep());
        for stat in stats {
            lines.push(InfoLine::value(format!(
                " {:20} {:12} {:>8} {:>8} {:>14} {:>14} {:>14}",
                truncate_str(&stat.name, 20),
                truncate_str(&stat.dtype, 12),
                stat.total_count,
                stat.null_count,
                truncate_str(&stat.min_value, 14),
                truncate_str(&stat.max_value, 14),
                truncate_str(&stat.mean_value, 14),
            )));
        }
        lines
    }

    fn format_metadata_lines(info: &ParquetFileInfo) -> Vec<InfoLine> {
        let mut lines = Vec::new();
        lines.push(InfoLine::header(" File Information"));
        lines.push(InfoLine::sep());
        lines.push(InfoLine::label(format!(
            "  File size    {}",
            Self::format_bytes(info.file_size_bytes)
        )));
        lines.push(InfoLine::label(format!(
            "  Created by   {}",
            info.created_by
        )));
        lines.push(InfoLine::label(format!(
            "  Row groups   {}",
            info.num_row_groups
        )));
        lines.push(InfoLine::sep());

        for rg in &info.row_groups {
            lines.push(InfoLine::header(format!(
                " Row Group {}",
                rg.index
            )));
            lines.push(InfoLine::label(format!(
                "  Rows  {}    Size  {}",
                Self::format_number(rg.num_rows as u64),
                Self::format_bytes(rg.total_byte_size as u64),
            )));
            lines.push(InfoLine::sep());
            lines.push(InfoLine::value(format!(
                "  {:24} {:12} {:>12} {:>12} {:>6}",
                "Column", "Codec", "Compressed", "Raw", "Ratio"
            )));
            lines.push(InfoLine::sep());
            for col in &rg.columns {
                let ratio = if col.uncompressed_size > 0 {
                    col.compressed_size as f64 / col.uncompressed_size as f64
                } else {
                    0.0
                };
                lines.push(InfoLine::plain(format!(
                    "  {:24} {:12} {:>12} {:>12} {:>5.1}%",
                    truncate_str(&col.name, 24),
                    truncate_str(&col.compression, 12),
                    Self::format_bytes(col.compressed_size as u64),
                    Self::format_bytes(col.uncompressed_size as u64),
                    ratio * 100.0,
                )));
            }
            lines.push(InfoLine::sep());
        }
        lines
    }

    fn format_bytes(bytes: u64) -> String {
        if bytes >= 1_073_741_824 {
            format!("{:.2} GB", bytes as f64 / 1_073_741_824.0)
        } else if bytes >= 1_048_576 {
            format!("{:.2} MB", bytes as f64 / 1_048_576.0)
        } else if bytes >= 1024 {
            format!("{:.1} KB", bytes as f64 / 1024.0)
        } else {
            format!("{} B", bytes)
        }
    }

    fn format_number(n: u64) -> String {
        let s = n.to_string();
        let mut result = String::new();
        for (i, c) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(c);
        }
        result.chars().rev().collect()
    }

    // ------------------------------------------------------------------
    // Search / filter
    // ------------------------------------------------------------------

    pub fn open_search(&mut self) {
        if self.loaded.is_none() {
            self.set_status("Load a file first");
            return;
        }
        self.search_state = Some(SearchState {
            mode: SearchMode::Scope,
            scope: SearchScope::Global,
            scope_selected: 0,
            scope_scroll: 0,
            scope_inner_rect: None,
            input: String::new(),
            cursor: 0,
            results: None,
            result_offset: 0,
        });
    }

    /// Build the list of search scope options: "All columns" + each column name.
    pub fn search_scope_options(&self) -> Vec<(SearchScope, String)> {
        let mut options = vec![(SearchScope::Global, "All columns".to_string())];
        if let Some(loaded) = &self.loaded {
            for (idx, line) in loaded.schema_lines.iter().enumerate() {
                let col_name = line.split(':').next().unwrap_or("?").to_string();
                options.push((
                    SearchScope::Column(idx, col_name.clone()),
                    format!("Column: {col_name}"),
                ));
            }
        }
        options
    }

    pub fn handle_search_key(&mut self, key: KeyEvent) -> anyhow::Result<bool> {
        let Some(state) = self.search_state.as_mut() else {
            return Ok(false);
        };

        match state.mode {
            SearchMode::Scope => {
                let option_count = self.search_scope_options().len();
                match key.code {
                    KeyCode::Esc => {
                        self.search_state = None;
                        self.set_status("Search cancelled");
                    }
                    KeyCode::Up => {
                        if let Some(state) = self.search_state.as_mut() {
                            state.scope_selected = state.scope_selected.saturating_sub(1);
                        }
                    }
                    KeyCode::Down => {
                        if let Some(state) = self.search_state.as_mut() {
                            state.scope_selected =
                                cmp::min(state.scope_selected + 1, option_count.saturating_sub(1));
                        }
                    }
                    KeyCode::Enter => {
                        let options = self.search_scope_options();
                        if let Some(state) = self.search_state.as_mut() {
                            if let Some((scope, _)) = options.get(state.scope_selected) {
                                state.scope = scope.clone();
                                state.mode = SearchMode::Input;
                                state.input.clear();
                                state.cursor = 0;
                            }
                        }
                    }
                    _ => return Ok(false),
                }
                return Ok(true);
            }
            SearchMode::Input => match key.code {
                KeyCode::Esc => {
                    // Go back to scope selection
                    if let Some(state) = self.search_state.as_mut() {
                        state.mode = SearchMode::Scope;
                        state.scope_selected = 0;
                        state.scope_scroll = 0;
                        state.scope_inner_rect = None;
                        state.input.clear();
                        state.cursor = 0;
                    }
                }
                KeyCode::Enter => {
                    if state.input.trim().is_empty() {
                        self.search_state = None;
                        self.set_status("Search cleared");
                        return Ok(true);
                    }
                    let query = state.input.clone();
                    let scope = state.scope.clone();
                    self.execute_search(&query, &scope)?;
                }
                KeyCode::Backspace => {
                    if state.cursor > 0 {
                        let from = char_to_byte_index(&state.input, state.cursor - 1);
                        let to = char_to_byte_index(&state.input, state.cursor);
                        state.input.replace_range(from..to, "");
                        state.cursor -= 1;
                    }
                }
                KeyCode::Left => {
                    state.cursor = state.cursor.saturating_sub(1);
                }
                KeyCode::Right => {
                    state.cursor = cmp::min(state.cursor + 1, state.input.chars().count());
                }
                KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                    let byte_idx = char_to_byte_index(&state.input, state.cursor);
                    state.input.insert(byte_idx, c);
                    state.cursor += 1;
                }
                _ => return Ok(false),
            },
            SearchMode::Results => match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    self.search_state = None;
                    self.set_status("Search cleared");
                }
                KeyCode::Char('/') => {
                    if let Some(state) = self.search_state.as_mut() {
                        state.mode = SearchMode::Scope;
                        state.scope_selected = 0;
                        state.scope_scroll = 0;
                        state.scope_inner_rect = None;
                        state.input.clear();
                        state.cursor = 0;
                    }
                }
                KeyCode::Up => {
                    state.result_offset = state.result_offset.saturating_sub(1);
                }
                KeyCode::Down => {
                    state.result_offset = state.result_offset.saturating_add(1);
                }
                KeyCode::PageUp => {
                    state.result_offset = state.result_offset.saturating_sub(20);
                }
                KeyCode::PageDown => {
                    state.result_offset = state.result_offset.saturating_add(20);
                }
                KeyCode::Home => {
                    state.result_offset = 0;
                }
                KeyCode::Left => {
                    // handled later if we add col scrolling for search results
                }
                KeyCode::Right => {}
                KeyCode::Char('e')
                    if key.modifiers.contains(KeyModifiers::CONTROL) =>
                {
                    self.open_export_from_search();
                    return Ok(true);
                }
                _ => return Ok(false),
            },
        }
        Ok(true)
    }

    fn execute_search(&mut self, query: &str, scope: &SearchScope) -> anyhow::Result<()> {
        let Some(loaded) = &self.loaded else {
            return Ok(());
        };
        let path = loaded.path.clone();
        let file_type = loaded.file_type;
        let scope_label = match scope {
            SearchScope::Global => "all columns".to_string(),
            SearchScope::Column(_, name) => format!("column '{name}'"),
        };
        let col_index = match scope {
            SearchScope::Global => None,
            SearchScope::Column(idx, _) => Some(*idx),
        };
        self.set_status(format!("Searching {scope_label} for '{query}'..."));
        let results = match file_type {
            LoadedFileType::Parquet => search_parquet_rows(&path, query, col_index, CELL_CHAR_LIMIT)?,
            LoadedFileType::Csv => search_csv_rows(&path, query, col_index, CELL_CHAR_LIMIT)?,
        };
        let count = results.matching_rows.len();
        let capped = results.capped;
        let msg = if capped {
            format!("Found {count}+ matches for '{query}' in {scope_label} (capped)")
        } else {
            format!("Found {count} matches for '{query}' in {scope_label}")
        };
        if let Some(state) = self.search_state.as_mut() {
            state.mode = SearchMode::Results;
            state.results = Some(results);
            state.result_offset = 0;
        }
        self.set_status(msg);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Export / write parquet
    // ------------------------------------------------------------------

    pub fn open_export_popup(&mut self) {
        let Some(loaded) = &self.loaded else {
            self.set_status("Load a file first");
            return;
        };
        if loaded.file_type == LoadedFileType::Csv {
            self.set_status("Export to CSV is only available for Parquet files");
            return;
        }
        let source = loaded.path.clone();
        let source_rel = self.display_path(&loaded.path);
        // Replace .parquet extension with .csv
        let default = if source_rel.ends_with(".parquet") {
            format!("{}.csv", &source_rel[..source_rel.len() - 8])
        } else {
            format!("{source_rel}.csv")
        };
        self.export_popup = Some(ExportPopup {
            source,
            cursor: default.chars().count(),
            input: default,
            rect: None,
            error: None,
        });
    }

    fn open_export_from_search(&mut self) {
        let Some(loaded) = &self.loaded else {
            return;
        };
        let source = loaded.path.clone();
        let source_rel = self.display_path(&loaded.path);
        let default = if source_rel.ends_with(".parquet") {
            format!("{}.filtered.csv", &source_rel[..source_rel.len() - 8])
        } else {
            format!("{source_rel}.filtered.csv")
        };
        self.export_popup = Some(ExportPopup {
            source,
            cursor: default.chars().count(),
            input: default,
            rect: None,
            error: None,
        });
    }

    // ------------------------------------------------------------------
    // Import from CSV
    // ------------------------------------------------------------------

    pub fn open_import_popup(&mut self) {
        let default_source = String::new();
        self.import_popup = Some(ImportPopup {
            input: default_source,
            cursor: 0,
            target: String::new(),
            target_cursor: 0,
            active_field: ImportField::Source,
            rect: None,
            error: None,
        });
    }

    pub fn handle_import_key(&mut self, key: KeyEvent) -> anyhow::Result<bool> {
        let Some(popup) = self.import_popup.as_mut() else {
            return Ok(false);
        };

        match key.code {
            KeyCode::Esc => {
                self.import_popup = None;
                self.set_status("Import cancelled");
            }
            KeyCode::Tab => {
                // Toggle between source and target fields
                popup.active_field = match popup.active_field {
                    ImportField::Source => ImportField::Target,
                    ImportField::Target => ImportField::Source,
                };
            }
            KeyCode::Enter => {
                self.execute_import()?;
            }
            KeyCode::Backspace => {
                let (input, cursor) = match popup.active_field {
                    ImportField::Source => (&mut popup.input, &mut popup.cursor),
                    ImportField::Target => (&mut popup.target, &mut popup.target_cursor),
                };
                if *cursor > 0 {
                    let from = char_to_byte_index(input, *cursor - 1);
                    let to = char_to_byte_index(input, *cursor);
                    input.replace_range(from..to, "");
                    *cursor -= 1;
                    // Auto-fill target when editing source
                    if popup.active_field == ImportField::Source {
                        let src = &popup.input;
                        popup.target = if src.ends_with(".csv") {
                            format!("{}.parquet", &src[..src.len() - 4])
                        } else {
                            format!("{src}.parquet")
                        };
                        popup.target_cursor = popup.target.chars().count();
                    }
                }
            }
            KeyCode::Delete => {
                let (input, cursor) = match popup.active_field {
                    ImportField::Source => (&mut popup.input, &mut popup.cursor),
                    ImportField::Target => (&mut popup.target, &mut popup.target_cursor),
                };
                if *cursor < input.chars().count() {
                    let from = char_to_byte_index(input, *cursor);
                    let to = char_to_byte_index(input, *cursor + 1);
                    input.replace_range(from..to, "");
                }
            }
            KeyCode::Left => {
                let cursor = match popup.active_field {
                    ImportField::Source => &mut popup.cursor,
                    ImportField::Target => &mut popup.target_cursor,
                };
                *cursor = cursor.saturating_sub(1);
            }
            KeyCode::Right => {
                let (input, cursor) = match popup.active_field {
                    ImportField::Source => (&popup.input, &mut popup.cursor),
                    ImportField::Target => (&popup.target, &mut popup.target_cursor),
                };
                *cursor = cmp::min(*cursor + 1, input.chars().count());
            }
            KeyCode::Home => {
                let cursor = match popup.active_field {
                    ImportField::Source => &mut popup.cursor,
                    ImportField::Target => &mut popup.target_cursor,
                };
                *cursor = 0;
            }
            KeyCode::End => {
                let (input, cursor) = match popup.active_field {
                    ImportField::Source => (&popup.input, &mut popup.cursor),
                    ImportField::Target => (&popup.target, &mut popup.target_cursor),
                };
                *cursor = input.chars().count();
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                popup.error = None;
                let (input, cursor) = match popup.active_field {
                    ImportField::Source => (&mut popup.input, &mut popup.cursor),
                    ImportField::Target => (&mut popup.target, &mut popup.target_cursor),
                };
                let byte_idx = char_to_byte_index(input, *cursor);
                input.insert(byte_idx, c);
                *cursor += 1;
                // Auto-fill target when editing source
                if popup.active_field == ImportField::Source {
                    let src = &popup.input;
                    popup.target = if src.ends_with(".csv") {
                        format!("{}.parquet", &src[..src.len() - 4])
                    } else {
                        format!("{src}.parquet")
                    };
                    popup.target_cursor = popup.target.chars().count();
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn execute_import(&mut self) -> anyhow::Result<()> {
        let Some(popup) = &self.import_popup else {
            return Ok(());
        };

        // Resolve and validate paths before taking the popup so errors don't lose it
        let source = match self.resolve_target_path(&popup.input) {
            Ok(p) => p,
            Err(e) => {
                if let Some(p) = self.import_popup.as_mut() {
                    p.error = Some(format!("Source: {e}"));
                }
                return Ok(());
            }
        };
        if !source.exists() {
            if let Some(p) = self.import_popup.as_mut() {
                p.error = Some(format!("Source file not found: {}", source.display()));
            }
            return Ok(());
        }
        let dest = match self.resolve_target_path(&popup.target) {
            Ok(p) => p,
            Err(e) => {
                if let Some(p) = self.import_popup.as_mut() {
                    p.error = Some(format!("Target: {e}"));
                }
                return Ok(());
            }
        };

        // Now take the popup
        self.import_popup = None;
        let source_display = self.display_path(&source);
        let dest_display = self.display_path(&dest);

        let (tx, rx) = mpsc::channel();
        let src_disp = source_display.clone();
        let dst_disp = dest_display.clone();
        thread::spawn(move || {
            let result = import_csv(&source, &dest)
                .map(|rows| format!("Imported {rows} rows from {src_disp} to {dst_disp}"));
            let _ = tx.send(result);
        });

        self.progress_popup = Some(ProgressPopup {
            title: "Importing from CSV".to_string(),
            message: format!("Reading {source_display}..."),
            started: Instant::now(),
            receiver: rx,
            done_message: None,
            done_at: None,
        });

        Ok(())
    }

    // ------------------------------------------------------------------
    // Info popup (Keybindings / About)
    // ------------------------------------------------------------------

    pub fn open_keybindings_popup(&mut self) {
        self.info_popup = Some(InfoPopup {
            title: "Keybindings".to_string(),
            lines: vec![
                "Navigation".to_string(),
                "  Tab            Switch pane (Files / Preview)".to_string(),
                "  Up/Down        Move selection / scroll rows".to_string(),
                "  Left/Right     Collapse/expand dir, scroll columns".to_string(),
                "  Ctrl+Left/Right  Scroll columns by 5".to_string(),
                "  PageUp/PageDown  Page through rows".to_string(),
                "  Shift+Up/Down  Scroll info panel".to_string(),
                "  Enter          Open/toggle selected entry".to_string(),
                "  Backspace      Collapse directory or go to parent".to_string(),
                String::new(),
                "Info Tabs".to_string(),
                "  1 / click      Schema tab".to_string(),
                "  2 / s / click  Statistics tab".to_string(),
                "  3 / i / click  Metadata tab".to_string(),
                String::new(),
                "File Operations".to_string(),
                "  Ctrl+I         Import from CSV".to_string(),
                "  Ctrl+E         Export to CSV".to_string(),
                String::new(),
                "Sort".to_string(),
                "  o              Sort by leftmost column (asc/desc/none)".to_string(),
                "  Click header   Sort by clicked column".to_string(),
                String::new(),
                "Search & Tools".to_string(),
                "  /              Search in parquet data".to_string(),
                "  r              Refresh file list".to_string(),
                "  Ctrl+P         Open palette".to_string(),
                String::new(),
                "General".to_string(),
                "  F1             This help".to_string(),
                "  Ctrl+Q         Quit".to_string(),
                "  Esc            Close popup/menu".to_string(),
            ],
            scroll: 0,
            rect: None,
        });
    }

    pub fn open_about_popup(&mut self) {
        self.info_popup = Some(InfoPopup {
            title: "About rdb".to_string(),
            lines: vec![
                "rdb v0.1".to_string(),
                String::new(),
                "A terminal-based Parquet file explorer and viewer.".to_string(),
                String::new(),
                "Built with Ratatui, Polars, and Apache Parquet.".to_string(),
                String::new(),
                "Features:".to_string(),
                "  - Browse and inspect parquet files".to_string(),
                "  - View schema, statistics, and metadata".to_string(),
                "  - Search/filter rows across all columns".to_string(),
                "  - Export filtered or full data".to_string(),
                "  - File operations (rename, copy, move, delete)".to_string(),
                "  - Multiple color themes".to_string(),
            ],
            scroll: 0,
            rect: None,
        });
    }

    pub fn handle_info_popup_key(&mut self, key: KeyEvent) -> bool {
        let Some(popup) = self.info_popup.as_mut() else {
            return false;
        };

        match key.code {
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('q') | KeyCode::Char(' ') => {
                self.info_popup = None;
            }
            KeyCode::Up => {
                popup.scroll = popup.scroll.saturating_sub(1);
            }
            KeyCode::Down => {
                popup.scroll = popup.scroll.saturating_add(1);
            }
            KeyCode::PageUp => {
                popup.scroll = popup.scroll.saturating_sub(10);
            }
            KeyCode::PageDown => {
                popup.scroll = popup.scroll.saturating_add(10);
            }
            KeyCode::Home => {
                popup.scroll = 0;
            }
            _ => return false,
        }
        true
    }

    pub fn handle_export_key(&mut self, key: KeyEvent) -> anyhow::Result<bool> {
        let Some(popup) = self.export_popup.as_mut() else {
            return Ok(false);
        };

        match key.code {
            KeyCode::Esc => {
                self.export_popup = None;
                self.set_status("Export cancelled");
            }
            KeyCode::Enter => {
                self.execute_export()?;
            }
            KeyCode::Backspace => {
                if popup.cursor > 0 {
                    let from = char_to_byte_index(&popup.input, popup.cursor - 1);
                    let to = char_to_byte_index(&popup.input, popup.cursor);
                    popup.input.replace_range(from..to, "");
                    popup.cursor -= 1;
                }
            }
            KeyCode::Delete => {
                if popup.cursor < popup.input.chars().count() {
                    let from = char_to_byte_index(&popup.input, popup.cursor);
                    let to = char_to_byte_index(&popup.input, popup.cursor + 1);
                    popup.input.replace_range(from..to, "");
                }
            }
            KeyCode::Left => {
                popup.cursor = popup.cursor.saturating_sub(1);
            }
            KeyCode::Right => {
                popup.cursor = cmp::min(popup.cursor + 1, popup.input.chars().count());
            }
            KeyCode::Home => {
                popup.cursor = 0;
            }
            KeyCode::End => {
                popup.cursor = popup.input.chars().count();
            }
            KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                popup.error = None;
                let byte_idx = char_to_byte_index(&popup.input, popup.cursor);
                popup.input.insert(byte_idx, c);
                popup.cursor += 1;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn execute_export(&mut self) -> anyhow::Result<()> {
        let Some(popup) = &self.export_popup else {
            return Ok(());
        };

        let source = popup.source.clone();
        let dest = match self.resolve_target_path(&popup.input) {
            Ok(p) => p,
            Err(e) => {
                if let Some(p) = self.export_popup.as_mut() {
                    p.error = Some(format!("Target: {e}"));
                }
                return Ok(());
            }
        };
        let dest_display = self.display_path(&dest);

        // Now take the popup
        self.export_popup = None;

        // If search filter is active, export only matching rows
        let row_indices: Option<Vec<usize>> = self
            .search_state
            .as_ref()
            .and_then(|s| s.results.as_ref())
            .map(|r| r.matching_rows.iter().map(|(idx, _)| *idx).collect());

        let (tx, rx) = mpsc::channel();
        let dest_disp = dest_display.clone();
        thread::spawn(move || {
            let result = export_csv(&source, &dest, row_indices.as_deref())
                .map(|rows| format!("Exported {rows} rows to {dest_disp}"));
            let _ = tx.send(result);
        });

        self.progress_popup = Some(ProgressPopup {
            title: "Exporting to CSV".to_string(),
            message: format!("Writing to {dest_display}..."),
            started: Instant::now(),
            receiver: rx,
            done_message: None,
            done_at: None,
        });

        Ok(())
    }

    // ------------------------------------------------------------------
    // Progress popup
    // ------------------------------------------------------------------

    const PROGRESS_DONE_DISPLAY_MS: u64 = 2000;
    const PROGRESS_ERROR_DISPLAY_MS: u64 = 4000;

    pub fn poll_progress(&mut self) {
        let Some(progress) = &self.progress_popup else {
            return;
        };

        // Already showing completion message — check if it's time to close
        if let Some(done_at) = progress.done_at {
            let is_error = progress
                .done_message
                .as_ref()
                .is_some_and(|m| m.starts_with("Error"));
            let display_ms = if is_error {
                Self::PROGRESS_ERROR_DISPLAY_MS
            } else {
                Self::PROGRESS_DONE_DISPLAY_MS
            };
            if done_at.elapsed() >= Duration::from_millis(display_ms) {
                self.progress_popup = None;
                let _ = self.rescan_files();
            }
            return;
        }

        // Check if the background task is done
        match progress.receiver.try_recv() {
            Ok(Ok(msg)) => {
                self.set_status(msg.clone());
                if let Some(p) = self.progress_popup.as_mut() {
                    p.done_message = Some(msg);
                    p.done_at = Some(Instant::now());
                }
            }
            Ok(Err(err)) => {
                let msg = format!("Error: {err}");
                self.set_status(msg.clone());
                if let Some(p) = self.progress_popup.as_mut() {
                    p.done_message = Some(msg);
                    p.done_at = Some(Instant::now());
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                let msg = "Error: operation failed unexpectedly".to_string();
                self.set_status(msg.clone());
                if let Some(p) = self.progress_popup.as_mut() {
                    p.done_message = Some(msg);
                    p.done_at = Some(Instant::now());
                }
            }
            Err(mpsc::TryRecvError::Empty) => {} // still working
        }
    }

    pub fn progress_spinner(&self) -> &str {
        let Some(progress) = &self.progress_popup else {
            return "";
        };
        if progress.done_at.is_some() {
            return "✓";
        }
        const FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
        let idx = (progress.started.elapsed().as_millis() / 100) as usize % FRAMES.len();
        FRAMES[idx]
    }

    fn apply_palette(&mut self, palette_theme: PaletteTheme) {
        self.palette_theme = palette_theme;
        self.theme = palette_theme.theme();
        match Self::save_palette_theme_setting(palette_theme) {
            Ok(()) => {
                self.set_status(format!("Palette changed to {}.", palette_theme.name()));
            }
            Err(err) => {
                self.set_status(format!(
                    "Palette changed to {} (settings not saved: {err}).",
                    palette_theme.name()
                ));
            }
        }
    }

    fn load_palette_theme_setting() -> PaletteTheme {
        let Some(path) = Self::settings_path() else {
            return PaletteTheme::MainframeGreen;
        };
        let Ok(contents) = fs::read_to_string(path) else {
            return PaletteTheme::MainframeGreen;
        };
        contents
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once('=')?;
                if key.trim() != "palette" {
                    return None;
                }
                PaletteTheme::from_settings_value(value.trim())
            })
            .unwrap_or(PaletteTheme::MainframeGreen)
    }

    fn save_palette_theme_setting(theme: PaletteTheme) -> std::io::Result<()> {
        let Some(path) = Self::settings_path() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, format!("palette={}\n", theme.settings_value()))
    }

    fn settings_path() -> Option<PathBuf> {
        if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(path).join(APP_NAME).join(SETTINGS_FILE));
        }
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(".config").join(APP_NAME).join(SETTINGS_FILE))
    }

    fn open_explorer_parent(&mut self) {
        if let Some(parent) = self.explorer_root.parent() {
            self.explorer_root = parent.to_path_buf();
            self.rebuild_explorer_entries(None, true);
            self.set_status(format!(
                "Explorer: {}",
                self.display_path(&self.explorer_root)
            ));
        }
    }

    fn request_quit(&mut self) {
        self.quit_requested = true;
        self.set_status("Quit requested");
    }

    fn top_menu_target_at(&self, x: u16, y: u16) -> Option<OpenMenu> {
        self.top_menu_regions.and_then(|regions| {
            regions
                .iter()
                .find_map(|(target, rect)| rect.contains(x, y).then_some(*target))
        })
    }

    fn menu_item_at(&self, x: u16, y: u16) -> Option<usize> {
        self.menu_item_regions
            .iter()
            .position(|rect| rect.contains(x, y))
    }

    fn apply_top_menu_action(&mut self, target: OpenMenu) {
        if self.open_menu == Some(target) {
            self.open_menu = None;
        } else {
            self.open_menu = Some(target);
            self.menu_selected = 0;
        }
    }

    fn execute_file_action(&mut self) -> anyhow::Result<()> {
        let Some(popup) = self.file_action_popup.take() else {
            return Ok(());
        };

        let target = self.resolve_target_path(&popup.input)?;

        if popup.kind != FileActionKind::Copy && target == popup.source {
            self.set_status("Source and destination are identical");
            return Ok(());
        }

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }

        match popup.kind {
            FileActionKind::Rename | FileActionKind::Move => {
                fs::rename(&popup.source, &target).with_context(|| {
                    format!(
                        "failed to move {} to {}",
                        popup.source.display(),
                        target.display()
                    )
                })?;
            }
            FileActionKind::Copy => {
                fs::copy(&popup.source, &target).with_context(|| {
                    format!(
                        "failed to copy {} to {}",
                        popup.source.display(),
                        target.display()
                    )
                })?;
            }
        }

        if popup.kind != FileActionKind::Copy
            && self
                .loaded
                .as_ref()
                .is_some_and(|loaded| loaded.path == popup.source)
        {
            self.loaded = None;
        }

        self.rescan_files()?;
        self.select_path(&target);
        self.set_status(format!(
            "{} {}",
            popup.kind.done_verb(),
            self.display_path(&target)
        ));
        Ok(())
    }

    fn resolve_target_path(&self, text: &str) -> anyhow::Result<PathBuf> {
        let trimmed = text.trim();
        anyhow::ensure!(!trimmed.is_empty(), "target path cannot be empty");

        let path = PathBuf::from(trimmed);
        if path.is_absolute() {
            Ok(path)
        } else {
            Ok(self.root.join(path))
        }
    }

    fn select_path(&mut self, path: &Path) {
        if let Some(idx) = self.files.iter().position(|candidate| candidate == path) {
            self.selected = idx;
        }
    }

    fn select_file_at_mouse_row(&mut self, y: u16, files_inner: Rect, track_double_click: bool) {
        if files_inner.height == 0 || self.files.is_empty() || y < files_inner.y {
            return;
        }

        let row = usize::from(y.saturating_sub(files_inner.y));
        let idx = self.file_scroll + row;
        if idx >= self.files.len() {
            return;
        }

        self.selected = idx;
        self.delete_armed = None;

        if track_double_click {
            let now = Instant::now();
            if let Some((last_idx, clicked_at)) = self.last_file_click {
                if last_idx == idx
                    && now.duration_since(clicked_at) <= Duration::from_millis(DOUBLE_CLICK_MS)
                {
                    let result = self.load_selected();
                    self.apply_result(result);
                }
            }
            self.last_file_click = Some((idx, now));
        }
    }

    fn preview_window_legend(&self, width: usize) -> String {
        let text = self.loaded.as_ref().map_or_else(
            || " No parquet loaded ".to_string(),
            |loaded| {
                if loaded.total_rows == 0 || loaded.total_cols == 0 {
                    return format!(
                        " Window rows 0/{} | cols 0/{} ",
                        loaded.total_rows, loaded.total_cols
                    );
                }

                let row_start = loaded.row_offset + 1;
                let row_end = cmp::min(loaded.row_offset + loaded.viewport_rows, loaded.total_rows);
                let col_start = loaded.col_offset + 1;
                let col_end = cmp::min(loaded.col_offset + loaded.viewport_cols, loaded.total_cols);
                format!(
                    " Window rows {row_start}-{row_end}/{} | cols {col_start}-{col_end}/{} ",
                    loaded.total_rows, loaded.total_cols
                )
            },
        );

        pad_or_clip(&text, width)
    }

    fn ensure_preview_cache(&mut self, force: bool) -> anyhow::Result<()> {
        let Some(loaded) = self.loaded.as_ref() else {
            return Ok(());
        };

        if loaded.total_cols == 0 || loaded.total_rows == 0 {
            return Ok(());
        }

        if !force && self.preview_is_cached() {
            return Ok(());
        }

        let (path, file_type, row_start, row_count, projection, sorted_row_indices) = {
            let loaded = self.loaded.as_mut().expect("loaded exists");
            let row_window = cmp::max(DEFAULT_ROW_WINDOW, loaded.viewport_rows.saturating_mul(3));
            let col_window = cmp::max(DEFAULT_COL_WINDOW, loaded.viewport_cols.saturating_add(6));

            loaded.row_offset = cmp::min(loaded.row_offset, loaded.total_rows.saturating_sub(1));
            loaded.col_offset = cmp::min(loaded.col_offset, loaded.total_cols.saturating_sub(1));

            let row_start = loaded
                .row_offset
                .saturating_sub(loaded.viewport_rows.saturating_div(2));
            let row_count = cmp::min(row_window, loaded.total_rows.saturating_sub(row_start));

            let col_start = loaded.col_offset.saturating_sub(2);
            let col_count = cmp::min(col_window, loaded.total_cols.saturating_sub(col_start));
            let projection = (col_start..col_start + col_count).collect::<Vec<_>>();

            let sorted_row_indices = loaded.sort_state.as_ref().map(|sort| {
                let end = cmp::min(row_start + row_count, sort.indices.len());
                sort.indices[row_start..end].to_vec()
            });

            (loaded.path.clone(), loaded.file_type, row_start, row_count, projection, sorted_row_indices)
        };

        let slice = if let Some(row_indices) = sorted_row_indices {
            let mut s = match file_type {
                LoadedFileType::Parquet => load_parquet_rows(&path, &row_indices, &projection, CELL_CHAR_LIMIT)
                    .with_context(|| format!("unable to read sorted preview for {}", path.display()))?,
                LoadedFileType::Csv => load_csv_rows(&path, &row_indices, &projection, CELL_CHAR_LIMIT)
                    .with_context(|| format!("unable to read sorted CSV preview for {}", path.display()))?,
            };
            s.row_start = row_start;
            s
        } else {
            match file_type {
                LoadedFileType::Parquet => load_parquet_slice(&path, row_start, row_count, &projection, CELL_CHAR_LIMIT)
                    .with_context(|| format!("unable to read preview window for {}", path.display()))?,
                LoadedFileType::Csv => load_csv_slice(&path, row_start, row_count, &projection, CELL_CHAR_LIMIT)
                    .with_context(|| format!("unable to read CSV preview for {}", path.display()))?,
            }
        };

        if let Some(loaded) = self.loaded.as_mut() {
            loaded.cache_row_start = slice.row_start;
            loaded.cache_col_start = slice.col_start;
            loaded.cache_columns = slice.column_names;
            loaded.cache_rows = slice.rows;
        }

        Ok(())
    }

    fn preview_is_cached(&self) -> bool {
        let Some(loaded) = &self.loaded else {
            return true;
        };

        let view_rows = cmp::max(loaded.viewport_rows, 1);
        let view_cols = cmp::max(loaded.viewport_cols, 1);

        let row_end = loaded.cache_row_start + loaded.cache_rows.len();
        let col_end = loaded.cache_col_start + loaded.cache_columns.len();

        loaded.row_offset >= loaded.cache_row_start
            && loaded.row_offset + view_rows <= row_end
            && loaded.col_offset >= loaded.cache_col_start
            && loaded.col_offset + view_cols <= col_end
    }
}

impl LoadedParquet {
    fn from_meta(path: PathBuf, meta: ParquetMeta, file_type: LoadedFileType) -> Self {
        Self {
            file_type,
            path,
            schema_lines: meta.schema_lines,
            total_rows: meta.total_rows,
            total_cols: meta.total_cols,
            row_offset: 0,
            col_offset: 0,
            viewport_rows: 20,
            viewport_cols: 4,
            cache_row_start: 0,
            cache_col_start: 0,
            cache_columns: Vec::new(),
            cache_rows: Vec::new(),
            stats_lines: None,
            metadata_lines: None,
            sort_state: None,
        }
    }
}

fn char_to_byte_index(value: &str, char_idx: usize) -> usize {
    value
        .char_indices()
        .nth(char_idx)
        .map(|(idx, _)| idx)
        .unwrap_or(value.len())
}

fn truncate_str(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

fn pad_or_clip(text: &str, width: usize) -> String {
    let clipped = text.chars().take(width).collect::<String>();
    let used = clipped.chars().count();
    if used >= width {
        return clipped;
    }
    let mut out = clipped;
    out.push_str(&" ".repeat(width - used));
    out
}

fn layout_rects(terminal_width: u16, terminal_height: u16) -> Option<(Rect, Rect, Rect, Rect)> {
    if terminal_width < 40 || terminal_height < 12 {
        return None;
    }

    let body = Rect {
        x: 0,
        y: 1,
        width: terminal_width,
        height: terminal_height.saturating_sub(3),
    };
    let body_inner = shrink_rect(body)?;

    let files_width = body_inner.width.min(36);
    let preview_width = body_inner.width.saturating_sub(files_width);

    let files_rect = Rect {
        x: body_inner.x,
        y: body_inner.y,
        width: files_width,
        height: body_inner.height,
    };
    let preview_rect = Rect {
        x: body_inner.x.saturating_add(files_width),
        y: body_inner.y,
        width: preview_width,
        height: body_inner.height,
    };

    let files_inner = shrink_rect(files_rect)?;
    let preview_inner = shrink_rect(preview_rect)?;

    let schema_height = preview_inner.height.saturating_mul(34) / 100;
    let rows_height = preview_inner.height.saturating_sub(schema_height);
    let rows_rect = Rect {
        x: preview_inner.x,
        y: preview_inner.y.saturating_add(schema_height),
        width: preview_inner.width,
        height: rows_height,
    };
    let rows_inner = shrink_rect(rows_rect)?;

    Some((files_rect, files_inner, preview_rect, rows_inner))
}

fn shrink_rect(rect: Rect) -> Option<Rect> {
    if rect.width < 2 || rect.height < 2 {
        return None;
    }
    Some(Rect {
        x: rect.x.saturating_add(1),
        y: rect.y.saturating_add(1),
        width: rect.width.saturating_sub(2),
        height: rect.height.saturating_sub(2),
    })
}
