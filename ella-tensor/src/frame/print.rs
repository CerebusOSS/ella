use std::fmt::Display;

use super::Frame;
use comfy_table::{presets, Attribute, Cell, ContentArrangement, Table};

pub fn print_frames<F: Frame>(frames: &[F]) -> impl Display {
    // Modified from:
    // https://github.com/apache/arrow-rs/blob/master/arrow-cast/src/pretty.rs

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.load_preset(presets::UTF8_FULL);

    if frames.is_empty() {
        return table;
    }

    let header = frames[0]
        .columns()
        .map(|col| Cell::new(col.name()).add_attribute(Attribute::Bold))
        .collect::<Vec<_>>();
    table.set_header(header);

    for frame in frames {
        for row in 0..frame.nrows() {
            let cells = frame
                .columns()
                .map(|col| Cell::new(col.format_row(row)))
                .collect::<Vec<_>>();
            table.add_row(cells);
        }
    }

    table
}
