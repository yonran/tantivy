use schema::Field;
use schema::Value;

/// `FieldValue` holds together a `Field` and its `Value`.
#[derive(Debug, Clone, Ord, PartialEq, Eq, PartialOrd, Serialize, Deserialize)]
pub struct FieldValue {
    field: Field,
    value: Value,
}

impl FieldValue {
    /// Constructor
    pub fn new(field: Field, value: Value) -> FieldValue {
        FieldValue {
            field: field,
            value: value,
        }
    }

    /// Field accessor
    pub fn field(&self) -> Field {
        self.field
    }

    /// Value accessor
    pub fn value(&self) -> &Value {
        &self.value
    }
}
