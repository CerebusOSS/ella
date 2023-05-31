use crate::{
    fmt::{collapse_limit, fmt_overflow},
    Axis, Dyn, Mask, Shape, TensorValue,
};
use std::fmt;

impl<'a, S> fmt::Debug for Mask<'a, S>
where
    S: Shape,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_mask(self.as_dyn(), f, |v, f| v.format(f), 0, self.ndim())?;
        write!(
            f,
            ", shape={:?}, strides={:?}",
            self.shape(),
            self.strides()
        )?;
        Ok(())
    }
}

fn fmt_mask<F>(
    m: Mask<Dyn>,
    f: &mut fmt::Formatter<'_>,
    mut format: F,
    depth: usize,
    ndim: usize,
) -> fmt::Result
where
    F: FnMut(&bool, &mut fmt::Formatter<'_>) -> fmt::Result + Clone,
{
    match m.shape().slice() {
        &[] => format(&m.index::<[usize; 0]>([]), f)?,
        &[len] => {
            f.write_str("[")?;
            fmt_overflow(f, len, collapse_limit(0), ", ", "...", &mut |f, index| {
                format(&m.index([index]), f)
            })?;
            f.write_str("]")?;
        }
        shape => {
            let blank_lines = "\n".repeat(shape.len() - 2);
            let indent = " ".repeat(depth + 1);
            let sep = format!(",\n{}{}", blank_lines, indent);
            f.write_str("[")?;

            let limit = collapse_limit(ndim - depth - 1);
            fmt_overflow(f, shape[0], limit, &sep, "...", &mut |f, index| {
                fmt_mask(
                    m.index_axis(Axis(0), index),
                    f,
                    format.clone(),
                    depth + 1,
                    ndim,
                )
            })?;
            f.write_str("]")?;
        }
    };
    Ok(())
}
