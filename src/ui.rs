use std::cmp;

use unicode_width::UnicodeWidthChar;
use unicode_width::UnicodeWidthStr;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};

use crate::app::{ActivePane, App, ImportField, InfoLineKind, InfoTab, LoadedFileType, OpenMenu, SearchMode, SearchScope};
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

    if app.import_popup.is_some() {
        draw_import_popup(frame, area, app);
    }

    if app.sql_popup.is_some() {
        draw_sql_popup(frame, area, app);
    }

    if app.info_popup.is_some() {
        draw_info_popup(frame, area, app);
    }

    if app.progress_popup.is_some() {
        draw_progress_popup(frame, area, app);
    }
}

fn draw_top_bar(frame: &mut Frame, area: Rect, app: &mut App) {
    let base = Style::default().fg(app.theme.bar_fg).bg(app.theme.bar_bg);
    let active = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg)
        .add_modifier(Modifier::BOLD);

    let mut spans = vec![Span::styled(" rdb v0.1 ", active)];
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
    let is_csv = app
        .loaded
        .as_ref()
        .is_some_and(|l| l.file_type == LoadedFileType::Csv);

    let title = if app.active_pane == ActivePane::Preview {
        if is_csv { " CSV Preview [focused] " } else { " Parquet Preview [focused] " }
    } else if is_csv {
        " CSV Preview "
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

    let rows_area = if is_csv {
        // CSV: no info tabs, full area for rows
        inner
    } else {
        let sections = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
            .split(inner);

        draw_info_tabs(frame, sections[0], app);
        sections[1]
    };

    let rows_block = Block::default()
        .title(" Rows (Left/Right: cols, Up/Down: rows) ")
        .title_style(Style::default().fg(app.theme.fg).bg(app.theme.bg))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(rows_block.clone(), rows_area);
    let rows_inner = rows_block.inner(rows_area);
    // Fill rows inner with menu_bg for consistent background
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.menu_bg)),
        rows_inner,
    );

    let total_height = usize::from(rows_inner.height);
    let row_body_height = total_height.saturating_sub(3); // padding + header + divider
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

    // Padding line above column headers with matching separators
    let pad_cells: Vec<String> = (0..header_cells.len())
        .map(|_| " ".repeat(cell_width))
        .collect();
    let pad_line = format_table_row(Some(" ".repeat(1)), pad_cells);
    lines.push(Line::styled(
        clip_or_pad(&pad_line, table_width),
        Style::default()
            .fg(app.theme.active_fg)
            .bg(app.theme.active_bg)
            .add_modifier(Modifier::BOLD),
    ));

    let header_line = format_table_row(Some("#".to_string()), header_cells);
    lines.push(Line::styled(
        clip_or_pad(&header_line, table_width),
        Style::default()
            .fg(app.theme.active_fg)
            .bg(app.theme.active_bg)
            .add_modifier(Modifier::BOLD),
    ));

    // Store header column hit regions for mouse click sorting
    // Cover both the padding line and the header line (height=2)
    let pad_y = rows_inner.y;
    let mut col_regions = Vec::new();
    let mut cx = rows_inner.x + 6 + 3; // skip row-index column + separator
    for (i, _) in preview.header.iter().enumerate() {
        let w = cell_width as u16;
        col_regions.push((col_offset + i, cx, pad_y, w, 2u16));
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

fn draw_file_action_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.file_action_popup.is_none() {
        return;
    }

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

    // Store rect for mouse click-outside detection
    if let Some(popup) = app.file_action_popup.as_mut() {
        popup.rect = Some((rect.x, rect.y, rect.width, rect.height));
    }

    let popup = app.file_action_popup.as_ref().unwrap();
    let inner_width = usize::from(width.saturating_sub(2));

    let label = format!(" Source: {}", app.display_path(&popup.source));
    let input = &popup.input;
    let cursor = popup.cursor;

    // Build input line with a visible cursor (underscore block)
    let input_style = Style::default().fg(app.theme.fg).bg(app.theme.line_bg);
    let cursor_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg);

    let before: String = input.chars().take(cursor).collect();
    let cursor_char = input.chars().nth(cursor).unwrap_or(' ');
    let after: String = input.chars().skip(cursor + 1).collect();
    let after_pad_len = inner_width
        .saturating_sub(1) // leading space
        .saturating_sub(UnicodeWidthStr::width(before.as_str()))
        .saturating_sub(UnicodeWidthChar::width(cursor_char).unwrap_or(1))
        .saturating_sub(UnicodeWidthStr::width(after.as_str()));
    let after_padded = format!("{after}{}", " ".repeat(after_pad_len));

    let input_line = Line::from(vec![
        Span::styled(" ", input_style),
        Span::styled(before, input_style),
        Span::styled(cursor_char.to_string(), cursor_style),
        Span::styled(after_padded, input_style),
    ]);

    let lines = vec![
        Line::styled(
            clip_or_pad(&label, inner_width),
            Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
        ),
        Line::styled(
            clip_or_pad(" Target:", inner_width),
            Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
        ),
        input_line,
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
    // Tab bar (padding + labels) lives inside the bordered area.
    let tab_bar_height = 2u16; // 1 padding + 1 labels
    if area.height < tab_bar_height + 3 {
        return;
    }

    // Fill info area with menu_bg so content background is consistent
    frame.render_widget(
        Paragraph::new("").style(Style::default().bg(app.theme.menu_bg)),
        area,
    );
    let outer_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.menu_bg));
    frame.render_widget(outer_block.clone(), area);
    let inner = outer_block.inner(area);

    // Split inner: tab bar (2 lines) + content body
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(tab_bar_height), Constraint::Min(1)])
        .split(inner);

    // --- Tab bar (padding line + label line) ---
    let tab_area = chunks[0];
    let tab_base = Style::default().fg(app.theme.bar_fg).bg(app.theme.bar_bg);
    let tab_active = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg)
        .add_modifier(Modifier::BOLD);

    // The actual tab labels sit on the second line
    let label_area = Rect {
        y: tab_area.y + 1,
        height: 1,
        ..tab_area
    };

    let mut spans = Vec::new();
    let mut pad_spans = Vec::new();
    let mut tab_regions = Vec::new();
    let mut cursor_x = label_area.x;
    for (idx, tab) in InfoTab::ALL.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::styled(" ", tab_base));
            pad_spans.push(Span::styled(" ", tab_base));
            cursor_x += 1;
        }
        let style = if app.info_tab == *tab {
            tab_active
        } else {
            tab_base
        };
        let label = format!(" {}.{} ", idx + 1, tab.label());
        let label_len = label.chars().count() as u16;
        // Padding line mirrors the style of each tab label (no bold for padding)
        let pad_style = if app.info_tab == *tab {
            Style::default().fg(app.theme.active_fg).bg(app.theme.active_bg)
        } else {
            tab_base
        };
        pad_spans.push(Span::styled(" ".repeat(label_len as usize), pad_style));
        // Make click region cover both the padding line and the label line
        tab_regions.push((*tab, cursor_x, tab_area.y, label_len, 2));
        spans.push(Span::styled(label, style));
        cursor_x += label_len;
    }
    let tab_used: usize = spans.iter().map(|s| s.content.chars().count()).sum();
    let tab_width = usize::from(label_area.width);
    if tab_used < tab_width {
        spans.push(Span::styled(" ".repeat(tab_width - tab_used), tab_base));
        pad_spans.push(Span::styled(" ".repeat(tab_width - tab_used), tab_base));
    }
    // Render padding line with styles matching the tab labels below
    let pad_area = Rect { height: 1, ..tab_area };
    frame.render_widget(Paragraph::new(Line::from(pad_spans)), pad_area);
    frame.render_widget(Paragraph::new(Line::from(spans)), label_area);
    app.set_info_tab_regions(tab_regions);

    // --- Content area ---
    let content_inner = chunks[1];
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

fn draw_search_overlay(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.search_state.is_none() {
        return;
    }

    let mode = app.search_state.as_ref().unwrap().mode.clone();

    match mode {
        SearchMode::Scope => {
            let options = app.search_scope_options();
            if area.width < 30 || area.height < 8 {
                return;
            }

            let content_width = options
                .iter()
                .map(|(_, label): &(SearchScope, String)| label.chars().count() + 4)
                .max()
                .unwrap_or(20);
            let width = cmp::min(
                cmp::max(content_width as u16 + 4, 30),
                area.width.saturating_sub(4),
            );
            let height = cmp::min(options.len() as u16 + 4, area.height.saturating_sub(2));
            let rect = Rect::new(
                area.x + (area.width.saturating_sub(width)) / 2,
                area.y + (area.height.saturating_sub(height)) / 2,
                width,
                height,
            );

            let block = Block::default()
                .title(" Search Scope (Enter: select, Esc: cancel) ")
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

            let state = app.search_state.as_ref().unwrap();
            let scope_selected = state.scope_selected;

            // Compute scroll so selected item is visible
            let visible_start = if scope_selected >= state.scope_scroll + inner_height {
                scope_selected - inner_height + 1
            } else if scope_selected < state.scope_scroll {
                scope_selected
            } else {
                state.scope_scroll
            };

            // Store scroll and inner rect for mouse handling
            if let Some(state) = app.search_state.as_mut() {
                state.scope_scroll = visible_start;
                state.scope_inner_rect = Some((inner.x, inner.y, inner.width, inner.height));
            }

            let normal_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
            let selected_style = Style::default()
                .fg(app.theme.active_fg)
                .bg(app.theme.active_bg);

            let mut lines: Vec<Line> = options
                .iter()
                .enumerate()
                .skip(visible_start)
                .take(inner_height)
                .map(|(idx, (_, label))| {
                    let style = if idx == scope_selected {
                        selected_style
                    } else {
                        normal_style
                    };
                    Line::styled(clip_or_pad(&format!(" {label}"), inner_width), style)
                })
                .collect();

            while lines.len() < inner_height {
                lines.push(Line::styled(
                    " ".repeat(inner_width),
                    Style::default().bg(app.theme.menu_bg),
                ));
            }

            frame.render_widget(Paragraph::new(lines), inner);
        }
        SearchMode::Input => {
            let state = app.search_state.as_ref().unwrap();
            if area.width < 30 || area.height < 7 {
                return;
            }
            let scope_label = match &state.scope {
                SearchScope::Global => "All columns".to_string(),
                SearchScope::Column(_, name) => format!("Column: {name}"),
            };

            let width = cmp::min(76, area.width.saturating_sub(4));
            let height = 7;
            let rect = Rect::new(
                area.x + (area.width.saturating_sub(width)) / 2,
                area.y + (area.height.saturating_sub(height)) / 2,
                width,
                height,
            );

            let block = Block::default()
                .title(" Search ")
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

            let label_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
            let input_style = Style::default().fg(app.theme.fg).bg(app.theme.line_bg);
            let hint_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);

            let lines = vec![
                Line::styled(
                    clip_or_pad(&format!(" Scope: {scope_label}"), inner_width),
                    label_style,
                ),
                Line::styled(
                    clip_or_pad(" Query:", inner_width),
                    label_style,
                ),
                Line::styled(
                    clip_or_pad(&format!(" {}", state.input), inner_width),
                    input_style,
                ),
                Line::styled(
                    clip_or_pad(" Enter: search  Esc: back to scope", inner_width),
                    hint_style,
                ),
            ];
            frame.render_widget(Paragraph::new(lines), inner);
        }
        SearchMode::Results => {
            let state = app.search_state.as_ref().unwrap();
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
                " Search: '{}' - {} matches{} (Esc: close, /: new search, Ctrl+E: export) ",
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

fn draw_export_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.export_popup.is_none() {
        return;
    }

    let has_error = app.export_popup.as_ref().is_some_and(|p| p.error.is_some());
    let base_height: u16 = if has_error { 9 } else { 8 };

    if area.width < 30 || area.height < base_height {
        return;
    }

    let width = cmp::min(76, area.width.saturating_sub(4));
    let height = base_height;
    let rect = Rect::new(
        (area.width.saturating_sub(width)) / 2,
        (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    // Store rect for mouse click-outside detection
    if let Some(popup) = app.export_popup.as_mut() {
        popup.rect = Some((rect.x, rect.y, rect.width, rect.height));
    }

    let popup = app.export_popup.as_ref().unwrap();
    let inner_width = usize::from(width.saturating_sub(2));

    let source_label = format!(" Source: {}", app.display_path(&popup.source));

    let filter_note = if app
        .search_state
        .as_ref()
        .and_then(|s| s.results.as_ref())
        .is_some()
    {
        " (filtered rows only)"
    } else {
        " (all rows)"
    };

    let label_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let input_style = Style::default().fg(app.theme.fg).bg(app.theme.line_bg);
    let cursor_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg);
    let hint_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);

    let input = &popup.input;
    let cursor = popup.cursor;
    let before: String = input.chars().take(cursor).collect();
    let cursor_char = input.chars().nth(cursor).unwrap_or(' ');
    let after: String = input.chars().skip(cursor + 1).collect();
    let after_pad_len = inner_width
        .saturating_sub(1)
        .saturating_sub(UnicodeWidthStr::width(before.as_str()))
        .saturating_sub(UnicodeWidthChar::width(cursor_char).unwrap_or(1))
        .saturating_sub(UnicodeWidthStr::width(after.as_str()));
    let after_padded = format!("{after}{}", " ".repeat(after_pad_len));

    let input_line = Line::from(vec![
        Span::styled(" ", input_style),
        Span::styled(before, input_style),
        Span::styled(cursor_char.to_string(), cursor_style),
        Span::styled(after_padded, input_style),
    ]);

    let error_style = Style::default().fg(app.theme.active_fg).bg(app.theme.menu_bg);

    let mut lines = vec![
        Line::styled(
            clip_or_pad(&source_label, inner_width),
            label_style,
        ),
        Line::styled(
            clip_or_pad(&format!(" Export to CSV{filter_note}:"), inner_width),
            label_style,
        ),
        Line::styled(
            clip_or_pad(" Target:", inner_width),
            label_style,
        ),
        input_line,
    ];
    if let Some(err) = &popup.error {
        lines.push(Line::styled(
            clip_or_pad(&format!(" {err}"), inner_width),
            error_style,
        ));
    }
    lines.push(Line::styled(
        clip_or_pad(" Enter: export  Esc: cancel", inner_width),
        hint_style,
    ));

    let block = Block::default()
        .title(" Export to CSV ")
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
// Import popup
// ---------------------------------------------------------------------------

fn draw_import_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.import_popup.is_none() {
        return;
    }

    let has_error = app.import_popup.as_ref().is_some_and(|p| p.error.is_some());
    let base_height: u16 = if has_error { 10 } else { 9 };

    if area.width < 30 || area.height < base_height {
        return;
    }

    let width = cmp::min(76, area.width.saturating_sub(4));
    let height = base_height;
    let rect = Rect::new(
        (area.width.saturating_sub(width)) / 2,
        (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    // Store rect for mouse click-outside detection
    if let Some(popup) = app.import_popup.as_mut() {
        popup.rect = Some((rect.x, rect.y, rect.width, rect.height));
    }

    let popup = app.import_popup.as_ref().unwrap();
    let inner_width = usize::from(width.saturating_sub(2));

    let label_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let input_style = Style::default().fg(app.theme.fg).bg(app.theme.line_bg);
    let inactive_input_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.line_bg);
    let cursor_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg);
    let hint_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);

    // Source field
    let src_active = popup.active_field == ImportField::Source;
    let src_line = build_input_line(
        &popup.input,
        popup.cursor,
        inner_width,
        if src_active { input_style } else { inactive_input_style },
        if src_active { cursor_style } else { inactive_input_style },
    );

    // Target field
    let tgt_active = popup.active_field == ImportField::Target;
    let tgt_line = build_input_line(
        &popup.target,
        popup.target_cursor,
        inner_width,
        if tgt_active { input_style } else { inactive_input_style },
        if tgt_active { cursor_style } else { inactive_input_style },
    );

    let active_marker = |field: ImportField| -> &str {
        if popup.active_field == field { " ▶" } else { "  " }
    };

    let error_style = Style::default().fg(app.theme.active_fg).bg(app.theme.menu_bg);

    let mut lines = vec![
        Line::styled(
            clip_or_pad(&format!("{} Source CSV:", active_marker(ImportField::Source)), inner_width),
            label_style,
        ),
        src_line,
        Line::styled(
            clip_or_pad(&format!("{} Target Parquet:", active_marker(ImportField::Target)), inner_width),
            label_style,
        ),
        tgt_line,
    ];
    if let Some(err) = &popup.error {
        lines.push(Line::styled(
            clip_or_pad(&format!(" {err}"), inner_width),
            error_style,
        ));
    } else {
        lines.push(Line::styled(
            clip_or_pad("", inner_width),
            Style::default().bg(app.theme.menu_bg),
        ));
    }
    lines.push(Line::styled(
        clip_or_pad(" Enter: import  Tab: switch field  Esc: cancel", inner_width),
        hint_style,
    ));

    let block = Block::default()
        .title(" Import from CSV ")
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

fn build_input_line(
    input: &str,
    cursor: usize,
    width: usize,
    input_style: Style,
    cursor_style: Style,
) -> Line<'static> {
    let before: String = input.chars().take(cursor).collect();
    let cursor_char = input.chars().nth(cursor).unwrap_or(' ');
    let after: String = input.chars().skip(cursor + 1).collect();
    let after_pad_len = width
        .saturating_sub(1)
        .saturating_sub(UnicodeWidthStr::width(before.as_str()))
        .saturating_sub(UnicodeWidthChar::width(cursor_char).unwrap_or(1))
        .saturating_sub(UnicodeWidthStr::width(after.as_str()));
    let after_padded = format!("{after}{}", " ".repeat(after_pad_len));

    Line::from(vec![
        Span::styled(" ", input_style),
        Span::styled(before, input_style),
        Span::styled(cursor_char.to_string(), cursor_style),
        Span::styled(after_padded, input_style),
    ])
}

// ---------------------------------------------------------------------------
// Info popup (Keybindings / About)
// ---------------------------------------------------------------------------

fn draw_info_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.info_popup.is_none() || area.width < 30 || area.height < 10 {
        return;
    }

    // Compute rect from copied values to avoid borrow issues
    let (content_width, line_count) = {
        let popup = app.info_popup.as_ref().unwrap();
        let cw = popup.lines.iter().map(|l| l.chars().count()).max().unwrap_or(20);
        (cw, popup.lines.len())
    };

    let width = cmp::min(cmp::max(content_width as u16 + 6, 40), area.width.saturating_sub(4));
    let height = cmp::min(line_count as u16 + 4, area.height.saturating_sub(2));
    let rect = Rect::new(
        area.x + (area.width.saturating_sub(width)) / 2,
        area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    // Store rect so mouse clicks outside can close the popup
    app.info_popup.as_mut().unwrap().rect = Some((rect.x, rect.y, rect.width, rect.height));

    let popup = app.info_popup.as_ref().unwrap();
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
    // Right-align the index in a 5-display-width column
    let idx_w = UnicodeWidthStr::width(index_text.as_str());
    let padded_idx = if idx_w < 5 {
        format!("{}{}", " ".repeat(5 - idx_w), index_text)
    } else {
        index_text
    };
    if cells.is_empty() {
        return padded_idx;
    }
    format!("{} | {}", padded_idx, cells.join(" | "))
}

/// Clip or pad a string to exactly `width` display columns.
/// Replaces control characters and tabs with spaces to avoid alignment issues.
fn clip_or_pad(text: &str, width: usize) -> String {
    let mut out = String::new();
    let mut used = 0;
    for ch in text.chars() {
        // Replace control chars / tabs with a space
        let ch = if ch.is_control() { ' ' } else { ch };
        let cw = UnicodeWidthChar::width(ch).unwrap_or(0);
        if used + cw > width {
            break;
        }
        out.push(ch);
        used += cw;
    }
    if used < width {
        out.push_str(&" ".repeat(width - used));
    }
    out
}

fn draw_progress_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    let Some(progress) = &app.progress_popup else {
        return;
    };

    let width = 50u16.min(area.width.saturating_sub(4));
    let height = 5u16;
    let x = area.x + (area.width.saturating_sub(width)) / 2;
    let y = area.y + (area.height.saturating_sub(height)) / 2;
    let popup_area = Rect::new(x, y, width, height);

    frame.render_widget(Clear, popup_area);

    let title = format!(" {} ", progress.title);
    let block = Block::default()
        .title(title.as_str())
        .title_style(Style::default().fg(app.theme.fg).bg(app.theme.bg).add_modifier(Modifier::BOLD))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(block.clone(), popup_area);
    let inner = block.inner(popup_area);

    let spinner = app.progress_spinner();
    let (line1, line2) = if let Some(done_msg) = &progress.done_message {
        (
            format!("{spinner} {done_msg}"),
            String::new(),
        )
    } else {
        let elapsed = progress.started.elapsed().as_secs();
        (
            format!("{spinner} {}", progress.message),
            format!("  Elapsed: {elapsed}s"),
        )
    };

    let is_done = progress.done_message.is_some();
    let fg = if is_done { app.theme.active_fg } else { app.theme.fg };

    let lines = vec![
        Line::styled(
            clip_or_pad(&line1, usize::from(inner.width)),
            Style::default().fg(fg).bg(app.theme.bg),
        ),
        Line::styled(
            clip_or_pad(&line2, usize::from(inner.width)),
            Style::default().fg(app.theme.dim_fg).bg(app.theme.bg),
        ),
    ];

    frame.render_widget(
        Paragraph::new(lines).style(Style::default().bg(app.theme.bg)),
        inner,
    );
}

// ---------------------------------------------------------------------------
// SQL Query popup (DuckDB)
// ---------------------------------------------------------------------------

fn draw_sql_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    if app.sql_popup.is_none() {
        return;
    }

    let width = cmp::min(100, area.width.saturating_sub(4));
    let height = cmp::min(area.height.saturating_sub(4), 30);
    if width < 40 || height < 10 {
        return;
    }

    let rect = Rect::new(
        (area.width.saturating_sub(width)) / 2,
        (area.height.saturating_sub(height)) / 2,
        width,
        height,
    );

    if let Some(popup) = app.sql_popup.as_mut() {
        popup.rect = Some((rect.x, rect.y, rect.width, rect.height));
    }

    let popup = app.sql_popup.as_ref().unwrap();
    let inner_width = usize::from(width.saturating_sub(2));

    let label_style = Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg);
    let input_style = Style::default().fg(app.theme.fg).bg(app.theme.line_bg);
    let cursor_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.active_bg);
    let hint_style = Style::default().fg(app.theme.dim_fg).bg(app.theme.menu_bg);
    let header_style = Style::default()
        .fg(app.theme.active_fg)
        .bg(app.theme.menu_bg)
        .add_modifier(Modifier::BOLD);
    let row_style = Style::default().fg(app.theme.fg).bg(app.theme.menu_bg);
    let error_style = Style::default().fg(app.theme.active_fg).bg(app.theme.menu_bg);

    let input = &popup.input;
    let cursor = popup.cursor;
    let before: String = input.chars().take(cursor).collect();
    let cursor_char = input.chars().nth(cursor).unwrap_or(' ');
    let after: String = input.chars().skip(cursor + 1).collect();
    let after_pad_len = inner_width
        .saturating_sub(1)
        .saturating_sub(UnicodeWidthStr::width(before.as_str()))
        .saturating_sub(UnicodeWidthChar::width(cursor_char).unwrap_or(1))
        .saturating_sub(UnicodeWidthStr::width(after.as_str()));
    let after_padded = format!("{after}{}", " ".repeat(after_pad_len));

    let input_line = Line::from(vec![
        Span::styled(" ", input_style),
        Span::styled(before, input_style),
        Span::styled(cursor_char.to_string(), cursor_style),
        Span::styled(after_padded, input_style),
    ]);

    let file_label = app
        .loaded
        .as_ref()
        .map(|l| format!(" Table: data  ({})", app.display_path(&l.path)))
        .unwrap_or_else(|| " Table: data".to_string());

    let mut lines = vec![
        Line::styled(clip_or_pad(&file_label, inner_width), label_style),
        Line::styled(clip_or_pad(" SQL:", inner_width), label_style),
        input_line,
    ];

    if let Some(err) = &popup.error {
        lines.push(Line::styled(
            clip_or_pad(&format!(" Error: {err}"), inner_width),
            error_style,
        ));
    }

    if let Some(result) = &popup.result {
        lines.push(Line::styled(
            clip_or_pad(
                &format!(
                    " {} rows, {} columns{}",
                    result.row_count,
                    result.column_names.len(),
                    if result.capped { " (capped)" } else { "" }
                ),
                inner_width,
            ),
            label_style,
        ));

        // Column layout
        let col_offset = popup.col_offset;
        let cell_width = 18;
        let avail = inner_width.saturating_sub(1);
        let visible_cols = cmp::max(avail / (cell_width + 1), 1);
        let end_col = cmp::min(col_offset + visible_cols, result.column_names.len());
        let visible_names = &result.column_names[col_offset..end_col];

        // Header
        let header_str: String = visible_names
            .iter()
            .map(|n| {
                let s = if n.len() > cell_width {
                    format!("{}...", &n[..cell_width - 3])
                } else {
                    format!("{:width$}", n, width = cell_width)
                };
                s
            })
            .collect::<Vec<_>>()
            .join("|");
        lines.push(Line::styled(
            clip_or_pad(&format!(" {header_str}"), inner_width),
            header_style,
        ));

        // Separator
        let sep_str = visible_names
            .iter()
            .map(|_| "-".repeat(cell_width))
            .collect::<Vec<_>>()
            .join("+");
        lines.push(Line::styled(
            clip_or_pad(&format!(" {sep_str}"), inner_width),
            label_style,
        ));

        // Data rows
        let max_data_lines = height.saturating_sub(2) as usize // border
            - lines.len()
            - 1; // hint line
        let scroll = popup.result_scroll;
        let end_row = cmp::min(scroll + max_data_lines, result.rows.len());

        for row in &result.rows[scroll..end_row] {
            let row_str: String = (col_offset..end_col)
                .map(|ci| {
                    let cell = row.get(ci).map(|s| s.as_str()).unwrap_or("");
                    let s = if cell.len() > cell_width {
                        format!("{}...", &cell[..cell_width - 3])
                    } else {
                        format!("{:width$}", cell, width = cell_width)
                    };
                    s
                })
                .collect::<Vec<_>>()
                .join("|");
            lines.push(Line::styled(
                clip_or_pad(&format!(" {row_str}"), inner_width),
                row_style,
            ));
        }
    }

    lines.push(Line::styled(
        clip_or_pad(
            " Enter: run  Up/Down: scroll  Shift+Left/Right: columns  Esc: close",
            inner_width,
        ),
        hint_style,
    ));

    let block = Block::default()
        .title(" SQL Query (DuckDB) ")
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
