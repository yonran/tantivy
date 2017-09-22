use std::collections::BTreeMap;
use schema::Value;
use std::fmt;
use serde::{Serialize, Deserialize, Deserializer, Serializer};
use serde::de::Visitor;

/// Internal representation of a document used for JSON
/// serialization.
///
/// A `NamedFieldDocument` is a simple representation of a document
/// as a `BTreeMap<String, Vec<Value>>`.
///
#[derive(Serialize)]
pub struct NamedFieldDocument(BTreeMap<String, Vec<ImplicitelyTypedValue>>);

impl NamedFieldDocument {

    pub fn new() -> NamedFieldDocument {
        NamedFieldDocument(BTreeMap::new())
    }

    pub fn insert(&mut self, field_name: String, values: Vec<Value>) {
        let typed_value = values
            .into_iter()
            .map(ImplicitelyTypedValue)
            .collect();
        self.0.insert(field_name, typed_value);
    }
}

pub(crate) struct ImplicitelyTypedValue(pub Value);

impl Serialize for ImplicitelyTypedValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer
    {
        match self.0 {
            Value::Str(ref v) => serializer.serialize_str(v),
            Value::U64(u) => serializer.serialize_u64(u),
            Value::I64(u) => serializer.serialize_i64(u),
        }
    }
}

impl<'de> Deserialize<'de> for ImplicitelyTypedValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or u32")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Value::U64(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Value::I64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(Value::Str(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(Value::Str(v))
            }
        }

        deserializer.deserialize_any(ValueVisitor).map(ImplicitelyTypedValue)
    }
}
