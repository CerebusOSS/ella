use datafusion::arrow::{
    array::{make_array, ArrayRef},
    datatypes::DataType,
};

pub fn flatten(array: ArrayRef) -> crate::Result<ArrayRef> {
    if !array.data_type().is_nested() {
        return Ok(array);
    }

    match array.data_type() {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Ok(
            make_array(array.to_data().child_data().first().unwrap().clone()),
        ),
        _ => Err(crate::Error::Unimplemented(format!(
            "flattening array of type {}",
            array.data_type()
        ))),
    }
}
