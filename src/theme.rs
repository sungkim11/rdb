use ratatui::style::Color;

#[derive(Clone, Copy)]
pub struct Theme {
    pub bg: Color,
    pub fg: Color,
    pub dim_fg: Color,
    pub bar_bg: Color,
    pub bar_fg: Color,
    pub active_bg: Color,
    pub active_fg: Color,
    pub menu_bg: Color,
    pub menu_fg: Color,
    pub line_bg: Color,
    pub panel_border: Color,
    pub message_bg: Color,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PaletteTheme {
    MainframeGreen,
    BlackWhite,
    Amber,
    OceanBlue,
    LightPaper,
}

impl PaletteTheme {
    pub const ALL: [PaletteTheme; 5] = [
        PaletteTheme::MainframeGreen,
        PaletteTheme::BlackWhite,
        PaletteTheme::Amber,
        PaletteTheme::OceanBlue,
        PaletteTheme::LightPaper,
    ];

    pub fn name(self) -> &'static str {
        match self {
            PaletteTheme::MainframeGreen => "Mainframe Green",
            PaletteTheme::BlackWhite => "Black & White",
            PaletteTheme::Amber => "Amber",
            PaletteTheme::OceanBlue => "Ocean Blue",
            PaletteTheme::LightPaper => "Light Paper",
        }
    }

    pub fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|theme| *theme == self)
            .unwrap_or(0)
    }

    pub fn from_index(index: usize) -> PaletteTheme {
        Self::ALL[index % Self::ALL.len()]
    }

    pub fn settings_value(self) -> &'static str {
        match self {
            PaletteTheme::MainframeGreen => "mainframe_green",
            PaletteTheme::BlackWhite => "black_white",
            PaletteTheme::Amber => "amber",
            PaletteTheme::OceanBlue => "ocean_blue",
            PaletteTheme::LightPaper => "light_paper",
        }
    }

    pub fn from_settings_value(value: &str) -> Option<Self> {
        match value {
            "mainframe_green" => Some(PaletteTheme::MainframeGreen),
            "black_white" => Some(PaletteTheme::BlackWhite),
            "amber" => Some(PaletteTheme::Amber),
            "ocean_blue" => Some(PaletteTheme::OceanBlue),
            "light_paper" => Some(PaletteTheme::LightPaper),
            _ => None,
        }
    }

    pub fn theme(self) -> Theme {
        match self {
            PaletteTheme::MainframeGreen => Theme {
                bg: Color::Rgb(2, 12, 4),
                fg: Color::Rgb(255, 255, 255),
                dim_fg: Color::Rgb(55, 148, 79),
                bar_bg: Color::Rgb(16, 62, 30),
                bar_fg: Color::Rgb(185, 255, 205),
                active_bg: Color::Rgb(170, 255, 170),
                active_fg: Color::Black,
                menu_bg: Color::Rgb(7, 36, 18),
                menu_fg: Color::Rgb(152, 245, 176),
                line_bg: Color::Rgb(9, 24, 12),
                panel_border: Color::Rgb(64, 164, 94),
                message_bg: Color::Rgb(11, 33, 17),
            },
            PaletteTheme::BlackWhite => Theme {
                bg: Color::Black,
                fg: Color::White,
                dim_fg: Color::Rgb(170, 170, 170),
                bar_bg: Color::Rgb(32, 32, 32),
                bar_fg: Color::White,
                active_bg: Color::White,
                active_fg: Color::Black,
                menu_bg: Color::Rgb(18, 18, 18),
                menu_fg: Color::White,
                line_bg: Color::Rgb(12, 12, 12),
                panel_border: Color::Rgb(200, 200, 200),
                message_bg: Color::Rgb(24, 24, 24),
            },
            PaletteTheme::Amber => Theme {
                bg: Color::Rgb(20, 10, 2),
                fg: Color::Rgb(255, 236, 190),
                dim_fg: Color::Rgb(201, 147, 70),
                bar_bg: Color::Rgb(78, 41, 10),
                bar_fg: Color::Rgb(255, 221, 160),
                active_bg: Color::Rgb(255, 204, 120),
                active_fg: Color::Black,
                menu_bg: Color::Rgb(44, 22, 6),
                menu_fg: Color::Rgb(255, 210, 133),
                line_bg: Color::Rgb(27, 13, 3),
                panel_border: Color::Rgb(201, 132, 38),
                message_bg: Color::Rgb(35, 17, 5),
            },
            PaletteTheme::OceanBlue => Theme {
                bg: Color::Rgb(4, 10, 22),
                fg: Color::Rgb(230, 242, 255),
                dim_fg: Color::Rgb(112, 156, 214),
                bar_bg: Color::Rgb(12, 39, 82),
                bar_fg: Color::Rgb(196, 226, 255),
                active_bg: Color::Rgb(133, 188, 255),
                active_fg: Color::Rgb(4, 20, 51),
                menu_bg: Color::Rgb(7, 24, 51),
                menu_fg: Color::Rgb(173, 212, 255),
                line_bg: Color::Rgb(7, 18, 40),
                panel_border: Color::Rgb(91, 151, 222),
                message_bg: Color::Rgb(9, 22, 46),
            },
            PaletteTheme::LightPaper => Theme {
                bg: Color::Rgb(248, 247, 241),
                fg: Color::Rgb(34, 39, 46),
                dim_fg: Color::Rgb(101, 109, 124),
                bar_bg: Color::Rgb(217, 221, 230),
                bar_fg: Color::Rgb(35, 39, 46),
                active_bg: Color::Rgb(86, 132, 210),
                active_fg: Color::Rgb(255, 255, 255),
                menu_bg: Color::Rgb(232, 235, 242),
                menu_fg: Color::Rgb(47, 57, 69),
                line_bg: Color::Rgb(237, 239, 244),
                panel_border: Color::Rgb(123, 140, 163),
                message_bg: Color::Rgb(227, 231, 239),
            },
        }
    }
}
