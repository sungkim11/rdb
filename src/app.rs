use std::cmp;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::parquet::{ParquetMeta, fit_visible_columns, load_parquet_meta, load_parquet_slice};
use crate::theme::{PaletteTheme, Theme};
use anyhow::Context;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};

const DEFAULT_ROW_WINDOW: usize = 256;
const DEFAULT_COL_WINDOW: usize = 24;
const CELL_CHAR_LIMIT: usize = 48;
const DOUBLE_CLICK_MS: u64 = 500;

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

#[derive(Clone, Copy)]
enum TopMenuTarget {
    File,
    View,
    Tools,
    Help,
}

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

pub struct LoadedParquet {
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
    pub file_menu_open: bool,
    delete_armed: Option<(PathBuf, Instant)>,
    last_file_click: Option<(usize, Instant)>,
    mouse_regions: Option<MouseRegions>,
    top_menu_regions: Option<[(TopMenuTarget, Rect); 4]>,
    file_menu_quit_region: Option<Rect>,
    quit_requested: bool,
}

impl App {
    pub fn new(root: PathBuf) -> anyhow::Result<Self> {
        let palette_theme = PaletteTheme::MainframeGreen;
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
                "Tab: pane | Enter: open/toggle | Ctrl+P: Tools|Palette | n/c/m/d: parquet ops | q: quit"
                    .to_string(),
            status_at: Instant::now(),
            theme: palette_theme.theme(),
            palette_theme,
            file_action_popup: None,
            palette_popup: None,
            file_menu_open: false,
            delete_armed: None,
            last_file_click: None,
            mouse_regions: None,
            top_menu_regions: None,
            file_menu_quit_region: None,
            quit_requested: false,
        };
        app.explorer_root = app.root.clone();
        app.explorer_expanded_dirs.insert(app.explorer_root.clone());
        app.rescan_files()?;
        Ok(app)
    }

    pub fn selected_file(&self) -> Option<&PathBuf> {
        self.files.get(self.selected)
    }

    fn selected_parquet_file(&self) -> Option<PathBuf> {
        let path = self.selected_file()?;
        if self
            .file_is_dir
            .get(self.selected)
            .copied()
            .unwrap_or(false)
        {
            return None;
        }
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            return Some(path.clone());
        }
        None
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
                    files.push((path, name, false));
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

    pub fn clear_file_menu_regions(&mut self) {
        self.file_menu_quit_region = None;
    }

    pub fn update_file_menu_quit_region(&mut self, region: (u16, u16, u16, u16)) {
        self.file_menu_quit_region = Some(Rect {
            x: region.0,
            y: region.1,
            width: region.2,
            height: region.3,
        });
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
                TopMenuTarget::File,
                Rect {
                    x: file.0,
                    y: file.1,
                    width: file.2,
                    height: file.3,
                },
            ),
            (
                TopMenuTarget::View,
                Rect {
                    x: view.0,
                    y: view.1,
                    width: view.2,
                    height: view.3,
                },
            ),
            (
                TopMenuTarget::Tools,
                Rect {
                    x: tools.0,
                    y: tools.1,
                    width: tools.2,
                    height: tools.3,
                },
            ),
            (
                TopMenuTarget::Help,
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

    pub fn handle_file_menu_key(&mut self, key: KeyEvent) -> bool {
        if !self.file_menu_open {
            return false;
        }

        match key.code {
            KeyCode::Esc => {
                self.file_menu_open = false;
                self.set_status("File menu closed");
            }
            KeyCode::Enter | KeyCode::Char('q') => {
                self.file_menu_open = false;
                self.request_quit();
            }
            _ => return false,
        }

        true
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

        if !path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            self.set_status(format!("Not a parquet file: {}", self.display_path(&path)));
            return Ok(());
        }

        let meta = load_parquet_meta(&path)
            .with_context(|| format!("unable to load {}", path.display()))?;
        self.loaded = Some(LoadedParquet::from_meta(path, meta));
        self.ensure_preview_cache(true)?;

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
        let Some(source) = self.selected_parquet_file() else {
            self.set_status("Select a parquet file first");
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
        self.file_menu_open = false;
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
        if self.file_action_popup.is_some() || self.palette_popup.is_some() {
            return Ok(());
        }

        if matches!(
            mouse.kind,
            MouseEventKind::Down(MouseButton::Left) | MouseEventKind::Up(MouseButton::Left)
        ) {
            if let Some(quit_region) = self.file_menu_quit_region {
                if quit_region.contains(mouse.column, mouse.row) {
                    if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                        self.file_menu_open = false;
                        self.request_quit();
                    }
                    return Ok(());
                }
            }
        }

        if matches!(
            mouse.kind,
            MouseEventKind::Down(MouseButton::Left) | MouseEventKind::Up(MouseButton::Left)
        ) {
            if let Some(target) = self.top_menu_target_at(mouse.column, mouse.row) {
                if matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                    self.apply_top_menu_action(target);
                }
                return Ok(());
            }

            if self.file_menu_open && matches!(mouse.kind, MouseEventKind::Up(MouseButton::Left)) {
                self.file_menu_open = false;
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
        let Some(path) = self.selected_parquet_file() else {
            self.set_status("Select a parquet file first");
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

    pub fn schema_lines_for_render(&self) -> Vec<String> {
        self.loaded.as_ref().map_or_else(
            || vec!["Press Enter on a parquet file to load schema".to_string()],
            |loaded| loaded.schema_lines.clone(),
        )
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

    fn apply_palette(&mut self, palette_theme: PaletteTheme) {
        self.palette_theme = palette_theme;
        self.theme = palette_theme.theme();
        self.set_status(format!("Palette applied: {}", palette_theme.name()));
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

    fn top_menu_target_at(&self, x: u16, y: u16) -> Option<TopMenuTarget> {
        self.top_menu_regions.and_then(|regions| {
            regions
                .iter()
                .find_map(|(target, rect)| rect.contains(x, y).then_some(*target))
        })
    }

    fn apply_top_menu_action(&mut self, target: TopMenuTarget) {
        match target {
            TopMenuTarget::File => {
                self.active_pane = ActivePane::Files;
                self.file_menu_open = !self.file_menu_open;
                if self.file_menu_open {
                    self.set_status("File menu: Enter or click Quit");
                } else {
                    self.set_status(
                        "Files: Enter open/toggle | Left collapse | Right expand | n/c/m/d parquet ops",
                    );
                }
            }
            TopMenuTarget::View => {
                self.file_menu_open = false;
                self.active_pane = ActivePane::Preview;
                self.set_status("View: preview focused (Left/Right cols, Up/Down rows)");
            }
            TopMenuTarget::Tools => {
                self.file_menu_open = false;
                self.open_palette_popup();
            }
            TopMenuTarget::Help => {
                self.file_menu_open = false;
                self.set_status(
                    "Help: Tab pane | Enter load | Ctrl+P palette | n/c/m/d file ops | q quit",
                );
            }
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

        let (path, row_start, row_count, projection) = {
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

            (loaded.path.clone(), row_start, row_count, projection)
        };

        let slice =
            load_parquet_slice(&path, row_start, row_count, &projection, CELL_CHAR_LIMIT)
                .with_context(|| format!("unable to read preview window for {}", path.display()))?;

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
    fn from_meta(path: PathBuf, meta: ParquetMeta) -> Self {
        Self {
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
