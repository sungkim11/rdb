use std::env;
use std::io;
use std::time::Duration;

use crate::app::LeftTab;
use anyhow::Context;
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode, size,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

mod app;
mod duckdb;
mod parquet;
mod postgres;
mod theme;
mod ui;

use app::{ActivePane, App};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let root = env::current_dir().context("failed to read current directory")?;
    let mut app = App::new(root)?;

    if let Some(file_arg) = args.get(1) {
        let path = std::path::Path::new(file_arg);
        let result = app.open_file(path);
        app.apply_result(result);
    }

    enable_raw_mode().context("failed to enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)
        .context("failed to enter alternate screen")?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("failed to create terminal")?;

    let run_result = run_app(&mut terminal, &mut app);

    disable_raw_mode().context("failed to disable raw mode")?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .context("failed to restore terminal screen")?;
    terminal.show_cursor().context("failed to show cursor")?;

    run_result
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
) -> anyhow::Result<()> {
    loop {
        terminal.draw(|frame| ui::draw(frame, app))?;

        // Poll background operations (export/import progress)
        app.poll_progress();

        if !event::poll(Duration::from_millis(200))? {
            continue;
        }

        match event::read()? {
            Event::Key(key) => {
                if key.kind == KeyEventKind::Release {
                    continue;
                }

                // Progress popup blocks all input
                if app.progress_popup.is_some() {
                    continue;
                }

                // Popups consume keys first (priority order)
                if app.info_popup.is_some() {
                    app.handle_info_popup_key(key);
                    continue;
                }

                if app.export_popup.is_some() {
                    let result = app.handle_export_key(key);
                    app.apply_result(result.map(|_| ()));
                    continue;
                }

                if app.import_popup.is_some() {
                    let result = app.handle_import_key(key);
                    app.apply_result(result.map(|_| ()));
                    continue;
                }

                if app.pg_connect_popup.is_some() {
                    let result = app.handle_pg_connect_key(key);
                    app.apply_result(result.map(|_| ()));
                    continue;
                }

                if app.search_state.is_some() {
                    let result = app.handle_search_key(key);
                    app.apply_result(result.map(|_| ()));
                    continue;
                }

                if app.palette_popup.is_some() {
                    app.handle_palette_key(key);
                    continue;
                }

                if app.open_menu.is_some() {
                    let result = app.handle_menu_key(key);
                    app.apply_result(result.map(|_| ()));
                    if app.consume_quit_requested() {
                        break;
                    }
                    continue;
                }

                if app.file_action_popup.is_some() {
                    app.handle_file_action_key(key)?;
                    continue;
                }

                if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('q') {
                    break;
                }

                if (key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('p'))
                    || (key.modifiers.contains(KeyModifiers::ALT) && key.code == KeyCode::Char('t'))
                {
                    app.open_palette_popup();
                    continue;
                }

                if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('d') {
                    app.toggle_sql_mode();
                    continue;
                }

                // SQL pane: when active and preview focused, route keys to SQL handler
                if app.sql_state.is_some() && app.active_pane == ActivePane::Preview {
                    // Let Tab through to toggle focus, let Ctrl+Q through to quit
                    if key.code != KeyCode::Tab {
                        let result = app.handle_sql_key(key);
                        app.apply_result(result.map(|_| ()));
                        continue;
                    }
                }

                match key.code {
                    KeyCode::F(1) => {
                        app.open_keybindings_popup();
                    }
                    KeyCode::Char('r') => {
                        let result = app.rescan_files();
                        app.apply_result(result);
                    }
                    KeyCode::Tab => app.toggle_focus(),
                    KeyCode::Enter => {
                        if app.active_pane == ActivePane::Files {
                            match app.left_tab {
                                LeftTab::Postgres => {
                                    app.pg_tree_toggle_or_select();
                                }
                                LeftTab::Connections => {
                                    app.conn_activate_selected();
                                }
                                LeftTab::Files => {
                                    let result = app.load_selected();
                                    app.apply_result(result);
                                }
                            }
                        }
                    }
                    KeyCode::Up => match app.active_pane {
                        ActivePane::Files => match app.left_tab {
                            LeftTab::Postgres => {
                                app.pg_tree_move_selection(-1);
                            }
                            LeftTab::Connections => app.conn_move_selection(-1),
                            LeftTab::Files => app.move_selection(-1),
                        },
                        ActivePane::Preview => {
                            if key.modifiers.contains(KeyModifiers::SHIFT) {
                                app.scroll_info_panel(-1);
                            } else {
                                let result = app.scroll_preview_rows(-1);
                                app.apply_result(result);
                            }
                        }
                    },
                    KeyCode::Down => match app.active_pane {
                        ActivePane::Files => match app.left_tab {
                            LeftTab::Postgres => {
                                app.pg_tree_move_selection(1);
                            }
                            LeftTab::Connections => app.conn_move_selection(1),
                            LeftTab::Files => app.move_selection(1),
                        },
                        ActivePane::Preview => {
                            if key.modifiers.contains(KeyModifiers::SHIFT) {
                                app.scroll_info_panel(1);
                            } else {
                                let result = app.scroll_preview_rows(1);
                                app.apply_result(result);
                            }
                        }
                    },
                    KeyCode::PageUp => {
                        if app.active_pane == ActivePane::Preview {
                            let result = app.page_preview_rows(-1);
                            app.apply_result(result);
                        }
                    }
                    KeyCode::PageDown => {
                        if app.active_pane == ActivePane::Preview {
                            let result = app.page_preview_rows(1);
                            app.apply_result(result);
                        }
                    }
                    KeyCode::Left => match app.active_pane {
                        ActivePane::Preview => {
                            let step = if key.modifiers.contains(KeyModifiers::CONTROL) {
                                -5
                            } else {
                                -1
                            };
                            let result = app.scroll_preview_cols(step);
                            app.apply_result(result);
                        }
                        ActivePane::Files => app.collapse_selected_directory_or_parent(),
                    },
                    KeyCode::Right => match app.active_pane {
                        ActivePane::Preview => {
                            let step = if key.modifiers.contains(KeyModifiers::CONTROL) {
                                5
                            } else {
                                1
                            };
                            let result = app.scroll_preview_cols(step);
                            app.apply_result(result);
                        }
                        ActivePane::Files => app.expand_selected_directory(),
                    },
                    KeyCode::Backspace => {
                        if app.active_pane == ActivePane::Files && app.left_tab == LeftTab::Files {
                            app.collapse_selected_directory_or_parent();
                        }
                    }
                    KeyCode::Delete => {
                        if app.active_pane == ActivePane::Files
                            && app.left_tab == LeftTab::Connections
                        {
                            app.conn_delete_selected();
                        }
                    }
                    // Import
                    KeyCode::Char('i')
                        if key.modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        app.open_import_popup();
                    }
                    // Info tab keys
                    KeyCode::Char('i') => {
                        let result = app.switch_info_tab(app::InfoTab::Metadata);
                        app.apply_result(result);
                    }
                    KeyCode::Char('s') => {
                        let result = app.switch_info_tab(app::InfoTab::Statistics);
                        app.apply_result(result);
                    }
                    KeyCode::Char('1') => {
                        let result = app.switch_info_tab(app::InfoTab::Schema);
                        app.apply_result(result);
                    }
                    KeyCode::Char('2') => {
                        let result = app.switch_info_tab(app::InfoTab::Statistics);
                        app.apply_result(result);
                    }
                    KeyCode::Char('3') => {
                        let result = app.switch_info_tab(app::InfoTab::Metadata);
                        app.apply_result(result);
                    }
                    KeyCode::Char('/') => {
                        app.open_search();
                    }
                    KeyCode::Char('e')
                        if key.modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        app.open_export_popup();
                    }
                    KeyCode::Char('o') => {
                        if app.active_pane == ActivePane::Preview {
                            let result = app.toggle_sort_current_column();
                            app.apply_result(result);
                        }
                    }
                    _ => {}
                }
            }
            Event::Mouse(mouse) => {
                if app.progress_popup.is_some() {
                    continue;
                }
                let (cols, rows) = size()?;
                let result = app.handle_mouse_event(mouse, cols, rows);
                app.apply_result(result);
            }
            _ => {
                continue;
            }
        }

        if app.consume_quit_requested() {
            break;
        }
    }

    Ok(())
}
