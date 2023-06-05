//! Formatted Tensor display implementations

use std::fmt;

pub(crate) fn fmt_overflow(
    f: &mut fmt::Formatter<'_>,
    length: usize,
    limit: usize,
    sep: &str,
    ellipsis: &str,
    fmt_elem: &mut dyn FnMut(&mut fmt::Formatter, usize) -> fmt::Result,
) -> fmt::Result {
    if length == 0 {
    } else if length <= limit {
        fmt_elem(f, 0)?;
        for i in 1..length {
            f.write_str(sep)?;
            fmt_elem(f, i)?;
        }
    } else {
        let edge = limit / 2;
        fmt_elem(f, 0)?;
        for i in 1..edge {
            f.write_str(sep)?;
            fmt_elem(f, i)?;
        }
        f.write_str(sep)?;
        f.write_str(ellipsis)?;
        for i in (length - edge)..length {
            f.write_str(sep)?;
            fmt_elem(f, i)?;
        }
    }
    Ok(())
}

pub(crate) fn collapse_limit(rindex: usize) -> usize {
    match rindex {
        0 => 11,
        1 => 11,
        _ => 6,
    }
}
