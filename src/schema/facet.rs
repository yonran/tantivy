use itertools::join;
use std::fmt::{self, Display, Debug, Formatter};
use std::str;
use std::io::{self, Read, Write};
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use common::BinarySerializable;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct Facet(String);

const SEP: &'static str = "\u{1f}";
const SEP_BYTE: u8 = 31u8;

#[derive(Copy, Clone)]
enum State {
    Escaped,
    Idle,
}

impl Facet

    pub(crate) fn from_encoded(encoded_str: String) -> Facet {
        Facet(encoded_str)
    }

    pub fn from_path<Path>(path: Path) -> Facet
        where
            Path: IntoIterator,
            Path::Item: Display {
        Facet(join(path, SEP))
    }

    pub fn from_str(path: &str) -> Facet {
        assert!(!path.contains(SEP));
        let mut facet_encoded = String::new();
        let mut state = State::Idle;
        for c in path.chars() {
            match (state, c) {
                (State::Idle, '\\') => {
                    state = State::Escaped
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

    pub fn steps(&self) -> str::Split<&&str> {
        self.0.split(&SEP)
    }

    pub fn prefixes(&self) -> Vec<&[u8]> {
        let bytes = self.0.as_bytes();
        let mut prefixes: Vec<&[u8]> = bytes
            .iter()
            .cloned()
            .enumerate()
            .filter(|&(_, b)| b==SEP_BYTE)
            .map(|(pos, _)| &bytes[0..pos])
            .collect();
        prefixes.push(bytes);
        prefixes
    }
}

impl BinarySerializable for Facet {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        <String as BinarySerializable>::serialize(&self.0, writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        Ok(Facet(<String as BinarySerializable>::deserialize(reader)?))
    }
}

impl Display for Facet {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for step in self.steps() {
            write!(f, "/")?;
            write!(f, "{}", escape_slashes(step))?;
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
        serializer.serialize_str(&format!("{}", self.0))
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