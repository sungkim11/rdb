use std::cmp;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};

use crate::app::{ActivePane, App, InfoLineKind, InfoTab, OpenMenu, SearchMode};
use crate::theme::PaletteTheme;

pub fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    if area.width < 40 || area.height < 12 {
        app.clear_mouse_regions();
        app.clear_top_menu_regions();
        app.clear_menu_item_regions();
        // Fill with theme bg first
        frame.render_widget(
            Paragraph::new("Terminal too small (need at least 40x12)").style(
                Style::default()
                    .fg(app.theme.fg)
                    .bg(app.theme.bg)
                    .add_modifier(Modifier::BOLD),
            ),
            area,
        );
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(5),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);

    // Fill entire area with theme background
    let bg_fill = Paragraph::new("").style(Style::default().bg(app.theme.bg));
    frame.render_widget(bg_fill, area);

    draw_top_bar(frame, chunks[0], app);
    draw_body(frame, chunks[1], app);
    draw_status(frame, chunks[2], app);
    draw_message(frame, chunks[3], app);

    if app.open_menu.is_some() {
        draw_menu_popup(frame, area, app);
    } else {
        app.clear_menu_item_regions();
    }

    if app.palette_popup.is_some() {
        draw_palette_popup(frame, area, app);
    }

    if app.file_action_popup.is_some() {
        draw_file_action_popup(frame, area, app);
    }

    // Overlays
    if app.search_state.is_some() {
        draw_search_overlay(frame, area, app);
    }

    if app.export_popup.is_some() {
        draw_export_popup(frame, area, app);
    }

    if app.info_popup.is_some() {
        draw_info_popup(frame, area, app);
    }
}

fn draw_top_bar(frame: &mut Frame, area: Rect, app: &mut App) {
    let base = Style::default().fg(app.theme.bar_fg).bg(app.theme.bar_bg);
    let active = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg)
        .add_modifier(Modifier::BOLD);

    let mut spans = vec![Span::styled(" rdb ", active)];
    spans.push(Span::styled("  ", base));
    let menu_style = |menu: OpenMenu| -> Style {
        if app.open_menu == Some(menu) { active } else { base }
    };
    spans.push(Span::styled("File", menu_style(OpenMenu::File)));
    spans.push(Span::styled("  ", base));
    spans.push(Span::styled("View", menu_style(OpenMenu::View)));
    spans.push(Span::styled("  ", base));
    spans.push(Span::styled("Tools", menu_style(OpenMenu::Tools)));
    spans.push(Span::styled("  ", base));
    spans.push(Span::styled("Help", menu_style(OpenMenu::Help)));
    let width = usize::from(area.width);
    let used = spans
        .iter()
        .map(|s| s.content.chars().count())
        .sum::<usize>();
    if used < width {
        spans.push(Span::styled(" ".repeat(width - used), base));
    }

    let base_x = area.x;
    let base_y = area.y;
    let file_x = base_x.saturating_add(7);
    let view_x = file_x.saturating_add(6);
    let tools_x = view_x.saturating_add(6);
    let help_x = tools_x.saturating_add(7);

    // These match the rendered top-bar labels exactly for click hit-testing.
    app.update_top_menu_regions(
        (file_x, base_y, 4, 1),
        (view_x, base_y, 4, 1),
        (tools_x, base_y, 5, 1),
        (help_x, base_y, 4, 1),
    );

    frame.render_widget(Paragraph::new(Line::from(spans)).style(base), area);
}

fn draw_body(frame: &mut Frame, area: Rect, app: &mut App) {
    // Fill body area with theme bg first
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.bg)),
        area,
    );
    let body_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(body_block.clone(), area);
    let inner = body_block.inner(area);

    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(36), Constraint::Min(20)])
        .split(inner);

    let files_inner = draw_files_pane(frame, panes[0], app);
    let rows_inner = draw_preview_pane(frame, panes[1], app);

    app.update_mouse_regions(
        (panes[0].x, panes[0].y, panes[0].width, panes[0].height),
        (
            files_inner.x,
            files_inner.y,
            files_inner.width,
            files_inner.height,
        ),
        (panes[1].x, panes[1].y, panes[1].width, panes[1].height),
        (
            rows_inner.x,
            rows_inner.y,
            rows_inner.width,
            rows_inner.height,
        ),
    );
}

fn draw_files_pane(frame: &mut Frame, area: Rect, app: &mut App) -> Rect {
    let title = if app.active_pane == ActivePane::Files {
        " Files [focused] "
    } else {
        " Files "
    };

    // Fill pane area with theme bg
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.bg)),
        area,
    );
    let block = Block::default()
        .title(title)
        .title_style(Style::default().fg(app.theme.fg).bg(app.theme.bg))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(block.clone(), area);

    let inner = block.inner(area);
    let visible_rows = usize::from(inner.height);
    app.ensure_selected_visible(visible_rows);
    let (start, end) = app.visible_file_indices(visible_rows);

    let mut items = Vec::new();
    for idx in start..end {
        let label = app.file_label(idx).unwrap_or("(unknown)");
        let line = format!(" {}", label);
        let style = if app.file_is_parent_link(idx) {
            Style::default().fg(app.theme.dim_fg).bg(app.theme.bg)
        } else if app.file_is_dir(idx) {
            Style::default().fg(app.theme.menu_fg).bg(app.theme.bg)
        } else {
            Style::default().fg(app.theme.fg).bg(app.theme.bg)
        };
        items.push(ListItem::new(Line::styled(line, style)));
    }

    if items.is_empty() {
        items.push(ListItem::new(Line::styled(
            " (empty)",
            Style::default().fg(app.theme.dim_fg).bg(app.theme.bg),
        )));
    }

    let active_selected_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg)
        .add_modifier(Modifier::BOLD);
    let inactive_selected_style = Style::default().fg(app.theme.fg).bg(app.theme.line_bg);

    let list = List::new(items)
        .style(Style::default().bg(app.theme.bg))
        .highlight_style(if app.active_pane == ActivePane::Files {
            active_selected_style
        } else {
            inactive_selected_style
        });

    let mut state = ListState::default();
    if !app.files.is_empty() {
        state.select(Some(app.selected.saturating_sub(start)));
    }

    frame.render_stateful_widget(list, inner, &mut state);
    inner
}

fn draw_preview_pane(frame: &mut Frame, area: Rect, app: &mut App) -> Rect {
    let title = if app.active_pane == ActivePane::Preview {
        " Parquet Preview [focused] "
    } else {
        " Parquet Preview "
    };

    // Fill pane area with theme bg
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.bg)),
        area,
    );
    let block = Block::default()
        .title(title)
        .title_style(Style::default().fg(app.theme.fg).bg(app.theme.bg))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(block.clone(), area);
    let inner = block.inner(area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
        .split(inner);

    draw_info_tabs(frame, sections[0], app);

    let rows_block = Block::default()
        .title(" Rows (Left/Right: cols, Up/Down: rows) ")
        .title_style(Style::default().fg(app.theme.fg).bg(app.theme.bg))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(rows_block.clone(), sections[1]);
    let rows_inner = rows_block.inner(sections[1]);
    // Fill rows inner with menu_bg for consistent background
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.menu_bg)),
        rows_inner,
    );

    let total_height = usize::from(rows_inner.height);
    let row_body_height = total_height.saturating_sub(2);
    let viewport_result = app.set_preview_viewport(row_body_height, usize::from(rows_inner.width));
    app.apply_result(viewport_result);

    let preview = app.build_preview_render(row_body_height, 64);
    let mut lines = Vec::new();

    if let Some(msg) = preview.message {
        lines.push(Line::styled(
            clip_or_pad(&msg, usize::from(rows_inner.width)),
            Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg),
        ));
        while lines.len() < total_height {
            lines.push(Line::styled(
                " ".repeat(usize::from(rows_inner.width)),
                Style::default().bg(app.theme.menu_bg),
            ));
        }
        frame.render_widget(Paragraph::new(lines), rows_inner);
        app.set_header_col_regions(Vec::new());
        return rows_inner;
    }

    let table_width = usize::from(rows_inner.width);
    let visible_cols = cmp::max(preview.header.len(), 1);
    let cell_width = compute_cell_width(table_width, visible_cols);

    // Build header with sort indicators
    let sort_state = app.loaded.as_ref().and_then(|l| l.sort_state.as_ref());
    let col_offset = app.loaded.as_ref().map(|l| l.col_offset).unwrap_or(0);

    let header_cells: Vec<String> = preview
        .header
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let abs_col = col_offset + i;
            let indicator = sort_state
                .filter(|s| s.col_index == abs_col)
                .map(|s| if s.ascending { " \u{25B2}" } else { " \u{25BC}" })
                .unwrap_or("");
            let available = cell_width.saturating_sub(indicator.chars().count());
            let clipped_name = clip_or_pad(name, available);
            format!("{clipped_name}{indicator}")
        })
        .collect();

    let header_line = format_table_row(Some("#".to_string()), header_cells);
    lines.push(Line::styled(
        clip_or_pad(&header_line, table_width),
        Style::default()
            .fg(app.theme.active_fg)
            .bg(app.theme.active_bg)
            .add_modifier(Modifier::BOLD),
    ));

    // Store header column hit regions for mouse click sorting
    let header_y = rows_inner.y;
    let mut col_regions = Vec::new();
    let mut cx = rows_inner.x + 6 + 3; // skip row-index column + separator
    for (i, _) in preview.header.iter().enumerate() {
        let w = cell_width as u16;
        col_regions.push((col_offset + i, cx, header_y, w, 1u16));
        cx += w + 3; // cell + separator
    }
    app.set_header_col_regions(col_regions);

    let divider = "-".repeat(table_width);
    lines.push(Line::styled(
        divider,
        Style::default()
            .fg(app.theme.panel_border)
            .bg(app.theme.menu_bg),
    ));

    for (row_idx, cells) in preview.rows.iter().take(row_body_height) {
        let row_line = format_table_row(
            Some((row_idx + 1).to_string()),
            cells
                .iter()
                .map(|cell| clip_or_pad(cell, cell_width))
                .collect::<Vec<_>>(),
        );
        lines.push(Line::styled(
            clip_or_pad(&row_line, table_width),
            Style::default().fg(app.theme.fg).bg(app.theme.menu_bg),
        ));
    }

    while lines.len() < total_height {
        lines.push(Line::styled(
            " ".repeat(table_width),
            Style::default().bg(app.theme.menu_bg),
        ));
    }

    frame.render_widget(Paragraph::new(lines), rows_inner);
    rows_inner
}

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    let line = app.current_status_line(usize::from(area.width));
    frame.render_widget(
        Paragraph::new(line).style(Style::default().fg(app.theme.bar_fg).bg(app.theme.bar_bg)),
        area,
    );
}

fn draw_message(frame: &mut Frame, area: Rect, app: &App) {
    let line = app.current_message_line(usize::from(area.width));
    frame.render_widget(
        Paragraph::new(line).style(
            Style::default()
                .fg(app.theme.menu_fg)
                .bg(app.theme.message_bg),
        ),
        area,
    );
}

fn draw_menu_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    let Some(menu) = app.open_menu else {
        app.clear_menu_item_regions();
        return;
    };
    if area.height < 4 || area.width < 18 {
        app.clear_menu_item_regions();
        return;
    }

    let items = menu.items();

    // Compute dropdown dimensions
    let content_width = items
        .iter()
        .filter(|e| !e.is_separator)
        .map(|e| e.label.len() + 2 + e.shortcut.len() + 2)
        .max()
        .unwrap_or(10);
    let width = cmp::max(content_width as u16 + 4, 22).min(area.width.saturating_sub(2));
    let height = (items.len() as u16 + 2).min(area.height.saturating_sub(2));

    // Position dropdown under the correct top-bar label
    let menu_x = match menu {
        OpenMenu::File => area.x.saturating_add(6),
        OpenMenu::View => area.x.saturating_add(12),
        OpenMenu::Tools => area.x.saturating_add(18),
        OpenMenu::Help => area.x.saturating_add(25),
    };
    let x = menu_x.min(area.x + area.width.saturating_sub(width));
    let y = area.y.saturating_add(1);
    let rect = Rect::new(x, y, width, height);

    let block = Block::default().borders(Borders::ALL).border_style(
        Style::default()
            .fg(app.theme.panel_border)
            .bg(app.theme.menu_bg),
    );
    frame.render_widget(Clear, rect);
    frame.render_widget(block.clone(), rect);
    let inner = block.inner(rect);
    let inner_width = usize::from(inner.width);

    let normal_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let selected_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg);
    let sep_style = Style::default()
        .fg(app.theme.panel_border)
        .bg(app.theme.menu_bg);

    let mut lines = Vec::new();
    let mut item_regions = Vec::new();

    for (idx, entry) in items.iter().enumerate() {
        let row_y = inner.y.saturating_add(idx as u16);
        if entry.is_separator {
            lines.push(Line::styled(
                clip_or_pad(&"-".repeat(inner_width), inner_width),
                sep_style,
            ));
        } else {
            let style = if idx == app.menu_selected {
                selected_style
            } else {
                normal_style
            };
            let shortcut_pad = if entry.shortcut.is_empty() {
                String::new()
            } else {
                let label_len = entry.label.len() + 2;
                let gap = inner_width.saturating_sub(label_len + entry.shortcut.len() + 1);
                format!("{}{}", " ".repeat(gap), entry.shortcut)
            };
            let text = format!(" {}{}", entry.label, shortcut_pad);
            lines.push(Line::styled(clip_or_pad(&text, inner_width), style));
        }
        item_regions.push((inner.x, row_y, inner.width, 1));
    }

    frame.render_widget(Paragraph::new(lines), inner);
    app.set_menu_item_regions(item_regions);
}

fn draw_palette_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    let Some(popup) = app.palette_popup.as_ref() else {
        return;
    };
    if area.width < 30 || area.height < 10 {
        return;
    }

    let entries = PaletteTheme::ALL
        .iter()
        .enumerate()
        .map(|(idx, theme)| format!(" {}. {}", idx + 1, theme.name()))
        .collect::<Vec<_>>();
    let content_width = entries
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0);

    let width = cmp::max(
        30,
        cmp::min((content_width + 6) as u16, area.width.saturating_sub(4)),
    );
    let height = cmp::max(
        8,
        cmp::min((entries.len() + 4) as u16, area.height.saturating_sub(2)),
    );
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(width)) / 2,
        area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );
    let inner_width = usize::from(width.saturating_sub(2));
    let inner_height = usize::from(height.saturating_sub(2));

    let normal_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let selected_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg);
    let hint_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);

    let mut lines = entries
        .iter()
        .enumerate()
        .take(inner_height)
        .map(|(idx, line)| {
            let style = if idx == popup.selected {
                selected_style
            } else {
                normal_style
            };
            Line::styled(clip_or_pad(line, inner_width), style)
        })
        .collect::<Vec<_>>();

    if lines.len() < inner_height {
        lines.push(Line::styled(
            clip_or_pad(" Enter: apply  Esc: cancel  1-5: quick select", inner_width),
            hint_style,
        ));
    }
    while lines.len() < inner_height {
        lines.push(Line::styled(
            " ".repeat(inner_width),
            Style::default().bg(app.theme.menu_bg),
        ));
    }

    let block = Block::default()
        .title(" Palette ")
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(app.theme.panel_border)
                .bg(app.theme.menu_bg),
        );
    frame.render_widget(Clear, rect);
    frame.render_widget(block.clone(), rect);
    let inner = block.inner(rect);
    frame.render_widget(Paragraph::new(lines), inner);

    // Store click regions for each palette entry
    let item_regions: Vec<(u16, u16, u16, u16)> = (0..entries.len())
        .map(|idx| (inner.x, inner.y + idx as u16, inner.width, 1))
        .collect();
    app.set_palette_item_regions(item_regions);
}

fn draw_file_action_popup(frame: &mut Frame, area: Rect, app: &App) {
    let Some(popup) = app.file_action_popup.as_ref() else {
        return;
    };

    if area.width < 30 || area.height < 7 {
        return;
    }

    let width = cmp::min(76, area.width.saturating_sub(4));
    let height = 7;
    let rect = Rect::new(
        (area.width.saturating_sub(width)) / 2,
        (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    let inner_width = usize::from(width.saturating_sub(2));

    let label = format!(" Source: {}", app.display_path(&popup.source));
    let input = popup.input.clone();

    let lines = vec![
        Line::styled(
            clip_or_pad(&label, inner_width),
            Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
        ),
        Line::styled(
            clip_or_pad(" Target:", inner_width),
            Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
        ),
        Line::styled(
            clip_or_pad(&input, inner_width),
            Style::default().fg(app.theme.fg).bg(app.theme.line_bg),
        ),
        Line::styled(
            clip_or_pad(" Enter: apply  Esc: cancel", inner_width),
            Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg),
        ),
    ];

    let block = Block::default()
        .title(popup.kind.title())
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(app.theme.panel_border)
                .bg(app.theme.menu_bg),
        );

    frame.render_widget(Clear, rect);
    frame.render_widget(block.clone(), rect);
    frame.render_widget(Paragraph::new(lines), block.inner(rect));
}

// ---------------------------------------------------------------------------
// Info panel with tabs (Schema / Statistics / Metadata)
// ---------------------------------------------------------------------------

fn draw_info_tabs(frame: &mut Frame, area: Rect, app: &mut App) {
    // Draw tab bar + content inside the given area
    let tab_bar_height = 1u16;
    if area.height < tab_bar_height + 2 {
        return;
    }

    // Fill info area with menu_bg so content background is consistent
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.menu_bg)),
        area,
    );
    let outer_block = Block::default()
        .borders(Borders::LEFT | Borders::RIGHT | Borders::BOTTOM)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.menu_bg));

    // Split area: tab bar (1 line) + content
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(tab_bar_height), Constraint::Min(1)])
        .split(area);

    // --- Tab bar ---
    let tab_area = chunks[0];
    let tab_base = Style::default().fg(app.theme.bar_fg).bg(app.theme.bar_bg);
    let tab_active = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg)
        .add_modifier(Modifier::BOLD);

    let mut spans = Vec::new();
    let mut tab_regions = Vec::new();
    let mut cursor_x = tab_area.x;
    for (idx, tab) in InfoTab::ALL.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::styled(" ", tab_base));
            cursor_x += 1;
        }
        let style = if app.info_tab == *tab {
            tab_active
        } else {
            tab_base
        };
        let label = format!(" {}.{} ", idx + 1, tab.label());
        let label_len = label.chars().count() as u16;
        tab_regions.push((*tab, cursor_x, tab_area.y, label_len, 1));
        spans.push(Span::styled(label, style));
        cursor_x += label_len;
    }
    let tab_used: usize = spans.iter().map(|s| s.content.chars().count()).sum();
    let tab_width = usize::from(tab_area.width);
    if tab_used < tab_width {
        spans.push(Span::styled(" ".repeat(tab_width - tab_used), tab_base));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), tab_area);
    app.set_info_tab_regions(tab_regions);

    // --- Content area ---
    let content_area = chunks[1];
    frame.render_widget(outer_block.clone(), content_area);
    let content_inner = outer_block.inner(content_area);
    let inner_width = usize::from(content_inner.width);
    let inner_height = usize::from(content_inner.height);

    let info_lines = app.info_lines_for_render();
    let scroll = cmp::min(app.info_scroll, info_lines.len().saturating_sub(inner_height));

    let header_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg)
        .add_modifier(Modifier::BOLD);
    let label_style = Style::default()
        .fg(app.theme.fg)
        .bg(app.theme.menu_bg)
        .add_modifier(Modifier::BOLD);
    let value_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let sep_style = Style::default().fg(app.theme.panel_border).bg(app.theme.menu_bg);
    let plain_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);

    let mut lines: Vec<Line> = info_lines
        .iter()
        .skip(scroll)
        .take(inner_height)
        .map(|info_line| {
            let (style, text) = match info_line.kind {
                InfoLineKind::Header => (header_style, info_line.text.clone()),
                InfoLineKind::Label => (label_style, info_line.text.clone()),
                InfoLineKind::Value => (value_style, info_line.text.clone()),
                InfoLineKind::Separator => (sep_style, "\u{2500}".repeat(inner_width)),
                InfoLineKind::Plain => (plain_style, info_line.text.clone()),
            };
            Line::styled(clip_or_pad(&text, inner_width), style)
        })
        .collect();

    while lines.len() < inner_height {
        lines.push(Line::styled(
            " ".repeat(inner_width),
            Style::default().bg(app.theme.menu_bg),
        ));
    }

    frame.render_widget(Paragraph::new(lines), content_inner);
}

// ---------------------------------------------------------------------------
// Search overlay
// ---------------------------------------------------------------------------

fn draw_search_overlay(frame: &mut Frame, area: Rect, app: &App) {
    let Some(state) = app.search_state.as_ref() else {
        return;
    };

    match state.mode {
        SearchMode::Input => {
            // Draw search input bar at the bottom of the screen
            if area.height < 3 {
                return;
            }
            let bar_rect = Rect::new(area.x, area.y + area.height.saturating_sub(2), area.width, 1);
            let prompt = format!(" Search: {}", state.input);
            let style = Style::default()
                .fg(app.theme.active_fg)
                .bg(app.theme.active_bg);
            frame.render_widget(Clear, bar_rect);
            frame.render_widget(
                Paragraph::new(clip_or_pad(&prompt, usize::from(area.width))).style(style),
                bar_rect,
            );
        }
        SearchMode::Results => {
            let Some(results) = state.results.as_ref() else {
                return;
            };
            if area.width < 40 || area.height < 12 {
                return;
            }

            let width = cmp::min(area.width.saturating_sub(4), 120);
            let height = cmp::min(area.height.saturating_sub(2), 40);
            let rect = Rect::new(
                area.x + (area.width.saturating_sub(width)) / 2,
                area.y + (area.height.saturating_sub(height)) / 2,
                width,
                height,
            );

            let title = format!(
                " Search: '{}' - {} matches{} (Esc: close, /: new search, e: export) ",
                results.query,
                results.matching_rows.len(),
                if results.capped { "+" } else { "" },
            );

            let block = Block::default()
                .title(title.as_str())
                .borders(Borders::ALL)
                .border_style(
                    Style::default()
                        .fg(app.theme.panel_border)
                        .bg(app.theme.menu_bg),
                );

            frame.render_widget(Clear, rect);
            frame.render_widget(block.clone(), rect);
            let inner = block.inner(rect);
            let inner_width = usize::from(inner.width);
            let inner_height = usize::from(inner.height);

            let header_style = Style::default()
                .fg(app.theme.active_fg)
                .bg(app.theme.active_bg)
                .add_modifier(Modifier::BOLD);
            let normal_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);

            // Build header
            let col_names = &results.column_names;
            let visible_cols = cmp::min(col_names.len(), 6);
            let cell_w = if visible_cols > 0 {
                cmp::max(
                    8,
                    cmp::min(
                        (inner_width.saturating_sub(8)) / cmp::max(visible_cols, 1),
                        20,
                    ),
                )
            } else {
                8
            };

            let header_cells: Vec<String> = col_names
                .iter()
                .take(visible_cols)
                .map(|n| clip_or_pad(n, cell_w))
                .collect();
            let header_text = format!(
                " {:>5} | {}",
                "Row#",
                header_cells.join(" | ")
            );

            let max_results = results.matching_rows.len();
            let scroll = cmp::min(state.result_offset, max_results.saturating_sub(inner_height.saturating_sub(2)));

            let mut lines = Vec::new();
            lines.push(Line::styled(
                clip_or_pad(&header_text, inner_width),
                header_style,
            ));
            lines.push(Line::styled(
                "-".repeat(inner_width),
                Style::default()
                    .fg(app.theme.panel_border)
                    .bg(app.theme.menu_bg),
            ));

            let body_height = inner_height.saturating_sub(2);
            for (orig_row, cells) in results
                .matching_rows
                .iter()
                .skip(scroll)
                .take(body_height)
            {
                let row_cells: Vec<String> = cells
                    .iter()
                    .take(visible_cols)
                    .map(|c| clip_or_pad(c, cell_w))
                    .collect();
                let row_text = format!(
                    " {:>5} | {}",
                    orig_row + 1,
                    row_cells.join(" | ")
                );
                lines.push(Line::styled(
                    clip_or_pad(&row_text, inner_width),
                    normal_style,
                ));
            }

            while lines.len() < inner_height {
                lines.push(Line::styled(
                    " ".repeat(inner_width),
                    Style::default().bg(app.theme.menu_bg),
                ));
            }

            frame.render_widget(Paragraph::new(lines), inner);
        }
    }
}

// ---------------------------------------------------------------------------
// Export popup
// ---------------------------------------------------------------------------

fn draw_export_popup(frame: &mut Frame, area: Rect, app: &App) {
    let Some(popup) = app.export_popup.as_ref() else {
        return;
    };

    if area.width < 30 || area.height < 7 {
        return;
    }

    let width = cmp::min(76, area.width.saturating_sub(4));
    let height = 7;
    let rect = Rect::new(
        (area.width.saturating_sub(width)) / 2,
        (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    let inner_width = usize::from(width.saturating_sub(2));

    let filter_note = if app
        .search_state
        .as_ref()
        .and_then(|s| s.results.as_ref())
        .is_some()
    {
        " (will export filtered rows only)"
    } else {
        " (will export all rows)"
    };

    let lines = vec![
        Line::styled(
            clip_or_pad(&format!(" Export to:{filter_note}"), inner_width),
            Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
        ),
        Line::styled(
            clip_or_pad(" Path:", inner_width),
            Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
        ),
        Line::styled(
            clip_or_pad(&popup.input, inner_width),
            Style::default().fg(app.theme.fg).bg(app.theme.line_bg),
        ),
        Line::styled(
            clip_or_pad(" Enter: export  Esc: cancel", inner_width),
            Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg),
        ),
    ];

    let block = Block::default()
        .title(" Export Parquet ")
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(app.theme.panel_border)
                .bg(app.theme.menu_bg),
        );

    frame.render_widget(Clear, rect);
    frame.render_widget(block.clone(), rect);
    frame.render_widget(Paragraph::new(lines), block.inner(rect));
}

// ---------------------------------------------------------------------------
// Info popup (Keybindings / About)
// ---------------------------------------------------------------------------

fn draw_info_popup(frame: &mut Frame, area: Rect, app: &App) {
    let Some(popup) = app.info_popup.as_ref() else {
        return;
    };

    if area.width < 30 || area.height < 10 {
        return;
    }

    let content_width = popup
        .lines
        .iter()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(20);

    let width = cmp::min(cmp::max(content_width as u16 + 6, 40), area.width.saturating_sub(4));
    let height = cmp::min(popup.lines.len() as u16 + 4, area.height.saturating_sub(2));
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(width)) / 2,
        area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    let title = format!(" {} ", popup.title);
    let block = Block::default()
        .title(title.as_str())
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(app.theme.panel_border)
                .bg(app.theme.menu_bg),
        );

    frame.render_widget(Clear, rect);
    frame.render_widget(block.clone(), rect);
    let inner = block.inner(rect);
    let inner_width = usize::from(inner.width);
    let inner_height = usize::from(inner.height);

    let normal_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let hint_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);

    let body_height = inner_height.saturating_sub(1);
    let max_scroll = popup.lines.len().saturating_sub(body_height);
    let scroll = cmp::min(popup.scroll, max_scroll);

    let mut lines: Vec<Line> = popup
        .lines
        .iter()
        .skip(scroll)
        .take(body_height)
        .map(|line| {
            Line::styled(clip_or_pad(&format!(" {line}"), inner_width), normal_style)
        })
        .collect();

    while lines.len() < body_height {
        lines.push(Line::styled(
            " ".repeat(inner_width),
            Style::default().bg(app.theme.menu_bg),
        ));
    }

    lines.push(Line::styled(
        clip_or_pad(" Esc/Enter: close", inner_width),
        hint_style,
    ));

    frame.render_widget(Paragraph::new(lines), inner);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn compute_cell_width(total_width: usize, visible_cols: usize) -> usize {
    if visible_cols == 0 || total_width <= 12 {
        return 8;
    }

    let separators = visible_cols.saturating_sub(1) * 3;
    let available = total_width.saturating_sub(6 + 3 + separators);
    cmp::max(cmp::min(available / visible_cols, 24), 8)
}

fn format_table_row(index: Option<String>, cells: Vec<String>) -> String {
    let index_text = index.unwrap_or_default();
    if cells.is_empty() {
        return format!("{:>5}", index_text);
    }
    format!("{:>5} | {}", index_text, cells.join(" | "))
}

fn clip_or_pad(text: &str, width: usize) -> String {
    let mut out = text.chars().take(width).collect::<String>();
    let used = out.chars().count();
    if used < width {
        out.push_str(&" ".repeat(width - used));
    }
    out
}
