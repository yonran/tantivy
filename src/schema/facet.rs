use itertools::join;
use std::fmt::{self, Display, Debug, Formatter};
use std::str;
use regex::Regex;
use std::borrow::Cow;

pub struct Facet(String);

impl Facet {
    pub fn from_path<Path>(path: Path) -> Facet
        where
            Path: IntoIterator,
            Path::Item: Display {
        Facet(join(path, "\u{31}"))
    }

    pub fn steps<'a>(&'a self) -> str::Split<'a, &&str> {
        self.0.split(&"\u{31}")
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