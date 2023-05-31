use std::collections::HashMap;

use crate::Dyn;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FixedShapeTensor {
    #[serde(rename = "shape")]
    pub row_shape: Dyn,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dim_names: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permutation: Option<Vec<u32>>,
}

impl FixedShapeTensor {
    const EXT_NAME: &str = "arrow.fixed_shape_tensor";

    fn new(row_shape: Dyn) -> Self {
        Self {
            row_shape,
            dim_names: None,
            permutation: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExtensionType {
    FixedShapeTensor(FixedShapeTensor),
}

impl ExtensionType {
    const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
    const EXTENSION_META_KEY: &str = "ARROW:extension:metadata";

    pub fn tensor(row_shape: Dyn) -> Self {
        Self::FixedShapeTensor(FixedShapeTensor::new(row_shape))
    }

    pub fn encode(&self) -> HashMap<String, String> {
        let mut meta = HashMap::with_capacity(2);
        let (name, value) = match self {
            ExtensionType::FixedShapeTensor(tensor) => (
                FixedShapeTensor::EXT_NAME,
                serde_json::to_string(tensor).unwrap(),
            ),
        };
        meta.insert(Self::EXTENSION_NAME_KEY.to_string(), name.to_string());
        meta.insert(Self::EXTENSION_META_KEY.to_string(), value);
        meta
    }

    pub fn decode(meta: &HashMap<String, String>) -> crate::Result<Option<Self>> {
        if let Some(name) = meta.get(Self::EXTENSION_NAME_KEY) {
            let value = meta
                .get(Self::EXTENSION_META_KEY)
                .ok_or_else(|| crate::Error::MissingMetadata(name.to_owned()))?;
            match name.as_str() {
                FixedShapeTensor::EXT_NAME => {
                    Ok(Some(Self::FixedShapeTensor(serde_json::from_str(value)?)))
                }
                _ => Err(crate::Error::UnknownExtension(name.to_owned())),
            }
        } else {
            Ok(None)
        }
    }
}
