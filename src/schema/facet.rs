use std::fmt::{self, Display, Debug, Formatter};
use std::str;
use std::io::{self, Read, Write};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use common::BinarySerializable;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Facet(Vec<u8>);

const SLASH_BYTE: u8 = '/' as u8;
const SEP: &'static str = "\u{1f}";
const SEP_BYTE: u8 = 31u8;
const ESCAPE_BYTE: u8 = '\\' as u8;

#[derive(Copy, Clone)]
enum State {
    Escaped,
    Idle,
}


impl Facet {

    pub(crate) fn new() -> Facet {
        Facet(Vec::new())
    }

    pub(crate) fn encoded_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn from_encoded(encoded_bytes: Vec<u8>) -> Facet {
        Facet(encoded_bytes)
    }

    pub fn from_path<Path>(path: Path) -> Facet
        where
            Path: IntoIterator,
            Path::Item: Display {
        let mut facet_bytes: Vec<u8> = Vec::with_capacity(100);
        let mut step_it = path.into_iter();
        if let Some(step) = step_it.next() {
            write!(&mut facet_bytes, "{}", step);
        }
        for step in step_it {
            facet_bytes.push(SEP_BYTE);
            write!(&mut facet_bytes, "{}", step);
        }
        Facet(facet_bytes)
    }

    pub fn from_str(path: &str) -> Facet {
        // TODO check that path has the right format
        assert!(!path.contains(SEP));
        let mut facet_encoded = Vec::new();
        let mut state = State::Idle;
        let path_bytes = path.as_bytes();
        for &c in &path_bytes[1..] {
            match (state, c) {
                (State::Idle, ESCAPE_BYTE) => {
                    state = State::Escaped
                }
                (State::Idle, SLASH_BYTE) => {
                    facet_encoded.push(SEP_BYTE);
                }
                (State::Escaped, any_char) => {
                    state = State::Idle;
                    facet_encoded.push(any_char);
                }
                (State::Idle, other_char) => {
                    facet_encoded.push(other_char);
                }
            }
        }
        Facet(facet_encoded)
    }

    pub(crate) fn inner_buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    pub fn to_string(&self) -> String {
        format!("{}", self)
    }

    pub fn prefixes(&self) -> Vec<&[u8]> {
        let mut prefixes: Vec<&[u8]> = self.0
            .iter()
            .cloned()
            .enumerate()
            .filter(|&(_, b)| b==SEP_BYTE)
            .map(|(pos, _)| &self.0[0..pos])
            .collect();
        prefixes.push(&self.0[..]);
        prefixes
    }
}

impl BinarySerializable for Facet {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        <Vec<u8> as BinarySerializable>::serialize(&self.0, writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let bytes = <Vec<u8> as BinarySerializable>::deserialize(reader)?;
        Ok(Facet(bytes))
    }
}

impl Display for Facet {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for step in self.0.split(|&b| b == SEP_BYTE) {
            write!(f, "/")?;
            let step_str = unsafe { str::from_utf8_unchecked(step) };
            write!(f, "{}", escape_slashes(step_str))?;
        }
        Ok(())
    }
}

fn escape_slashes(s: &str) -> Cow<str> {
    lazy_static! {
        static ref SLASH_PTN: Regex = Regex::new(r"[\\/]").unwrap();
    }
    SLASH_PTN.replace_all(s, "\\/")
}

impl Serialize for Facet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where
        S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Facet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where
        D: Deserializer<'de> {
        <&'de str as Deserialize<'de>>::deserialize(deserializer)
            .map(Facet::from_str)
    }
}

impl Debug for Facet {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Facet({})", self)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::Facet;

    #[test]
    fn test_facet_display() {
        {
            let v = ["first", "second", "third"];
            let facet = Facet::from_path(v.iter());
            assert_eq!(format!("{}", facet), "/first/second/third");
        }
        {
            let v = ["first", "sec/ond", "third"];
            let facet = Facet::from_path(v.iter());
            assert_eq!(format!("{}", facet), "/first/sec\\/ond/third");
        }
    }


    #[test]
    fn test_facet_debug() {
        let v = ["first", "second", "third"];
        let facet = Facet::from_path(v.iter());
        assert_eq!(format!("{:?}", facet), "Facet(/first/second/third)");
    }

}