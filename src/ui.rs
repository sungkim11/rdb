use std::cmp;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph};

use crate::app::{ActivePane, App};
use crate::theme::PaletteTheme;

pub fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    if area.width < 40 || area.height < 12 {
        app.clear_mouse_regions();
        app.clear_top_menu_regions();
        app.clear_file_menu_regions();
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

    frame.render_widget(Clear, area);
    draw_top_bar(frame, chunks[0], app);
    draw_body(frame, chunks[1], app);
    draw_status(frame, chunks[2], app);
    draw_message(frame, chunks[3], app);

    if app.file_menu_open {
        draw_file_menu_popup(frame, area, app);
    } else {
        app.clear_file_menu_regions();
    }

    if app.palette_popup.is_some() {
        draw_palette_popup(frame, area, app);
    }

    if app.file_action_popup.is_some() {
        draw_file_action_popup(frame, area, app);
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
    spans.push(Span::styled(
        "File",
        if app.file_menu_open { active } else { base },
    ));
    spans.push(Span::styled("  ", base));
    spans.push(Span::styled("View", base));
    spans.push(Span::styled("  ", base));
    spans.push(Span::styled(
        "Tools",
        if app.palette_popup.is_some() {
            active
        } else {
            base
        },
    ));
    spans.push(Span::styled("  ", base));
    spans.push(Span::styled("Help", base));
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

    let block = Block::default()
        .title(title)
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

    let list = List::new(items).highlight_style(if app.active_pane == ActivePane::Files {
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

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(block.clone(), area);
    let inner = block.inner(area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
        .split(inner);

    let schema_block = Block::default()
        .title(" Schema ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(schema_block.clone(), sections[0]);
    let schema_inner = schema_block.inner(sections[0]);

    let schema_height = usize::from(schema_inner.height);
    let schema_lines = app
        .schema_lines_for_render()
        .iter()
        .take(schema_height)
        .map(|line| {
            Line::styled(
                clip_or_pad(line, usize::from(schema_inner.width)),
                Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(schema_lines), schema_inner);

    let rows_block = Block::default()
        .title(" Rows (Left/Right: cols, Up/Down: rows) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(app.theme.panel_border).bg(app.theme.bg));
    frame.render_widget(rows_block.clone(), sections[1]);
    let rows_inner = rows_block.inner(sections[1]);

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
        return rows_inner;
    }

    let table_width = usize::from(rows_inner.width);
    let visible_cols = cmp::max(preview.header.len(), 1);
    let cell_width = compute_cell_width(table_width, visible_cols);

    let header_line = format_table_row(
        Some("#".to_string()),
        preview
            .header
            .iter()
            .map(|name| clip_or_pad(name, cell_width))
            .collect::<Vec<_>>(),
    );
    lines.push(Line::styled(
        clip_or_pad(&header_line, table_width),
        Style::default()
            .fg(app.theme.active_fg)
            .bg(app.theme.active_bg)
            .add_modifier(Modifier::BOLD),
    ));

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

fn draw_file_menu_popup(frame: &mut Frame, area: Rect, app: &mut App) {
    if area.height < 4 || area.width < 18 {
        app.clear_file_menu_regions();
        return;
    }

    let file_x = area.x.saturating_add(7);
    let width = 12u16;
    let height = 3u16;
    let x = file_x
        .saturating_sub(1)
        .min(area.x + area.width.saturating_sub(width));
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
    let line = Line::styled(
        clip_or_pad(" Quit", usize::from(inner.width)),
        Style::default().fg(app.theme.menu_fg).bg(app.theme.menu_bg),
    );
    frame.render_widget(Paragraph::new(vec![line]), inner);

    app.update_file_menu_quit_region((inner.x, inner.y, inner.width, inner.height));
}

fn draw_palette_popup(frame: &mut Frame, area: Rect, app: &App) {
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
    frame.render_widget(Paragraph::new(lines), block.inner(rect));
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
