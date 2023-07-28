use crate::{shape::Dyn, TensorType};

use super::RowFormat;

pub trait NamedRowFormat: RowFormat {
    fn name(&self, i: usize) -> String;
    fn data_type(&self, i: usize) -> TensorType;
    fn row_shape(&self, i: usize) -> Dyn;
}
