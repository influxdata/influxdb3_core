use std::ops::{Deref, DerefMut};

use log::debug;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    combinator::{map, opt},
    sequence::{preceded, separated_pair, terminated, tuple},
};
use smallvec::SmallVec;

use crate::{
    escaped_value, field_set, is_whitespace_boundary_char, measurement,
    parameterized_separated_list, parse_and_recognize, split_lines, timestamp, trim_leading,
    whitespace, Error, EscapedStr, FieldSet, FieldValue, IResult, Measurement,
    SeriesKeyMalformedSnafu,
};

use super::Result;

/// A parsed line of v3 line protocol
///
/// v3 line protocol introduces the concept of a series key, which is the set of columns in a table
/// that, when combined with the `time` column, form the primary key for the table. The elements of
/// the series key have a specific user-defined order in the line protocol, and are passed like so:
///
/// ```
/// measurent,key_1/val_1/key_2/val_2 field=123 12345
///             ↑           ↑
///             series key columns 'key_1' and 'key_2'
/// ```
///
/// Some important points about the series key:
/// * Must always have the same members, so needs to be figured out up-front
/// * Members must always appear in the same order
/// * Cannot have null values, all members must be present in a write to that table
/// * May support more than string data types in future (may stick with strings while prototyping)
///
/// See <https://github.com/influxdata/influxdb/issues/24979>
#[derive(Debug)]
pub struct ParsedLine<'a> {
    pub series: Series<'a>,
    // TODO: v3 will extend the type system and therefore likely need
    // a new type to represent fields from the original version. For now,
    // we re-use the v1 field type and its associated parsers:
    pub field_set: FieldSet<'a>,
    pub timestamp: Option<i64>,
}

impl<'a> ParsedLine<'a> {
    /// Total number of columns in this line, including fields, series keys, and timestamp
    pub fn column_count(&self) -> usize {
        1 + self.field_set.len() + self.series.series_key.as_ref().map_or(0, |sk| sk.len())
    }

    /// Get the value for a member column of the series key, by its name
    pub fn series_key_value(&self, key: &str) -> Option<&SeriesValue<'a>> {
        match &self.series.series_key {
            Some(sk) => {
                let k = sk.iter().find(|(k, _)| *k == key);
                k.map(|(_, val)| val)
            }
            None => None,
        }
    }

    /// Get the value for a field, by its name
    pub fn field_value(&self, key: &str) -> Option<&FieldValue<'a>> {
        let f = self.field_set.iter().find(|(f, _)| *f == key);
        f.map(|(_, val)| val)
    }
}

/// A v3 series entry
#[derive(Debug)]
pub struct Series<'a> {
    // raw_input is added to replicate the original parser, but is only used
    // in tests there, so may be removed?
    #[allow(dead_code)]
    raw_input: &'a str,
    pub measurement: Measurement<'a>,
    pub series_key: Option<SeriesKey<'a>>,
}

type SeriesKeyInner<'a> = SmallVec<[(EscapedStr<'a>, SeriesValue<'a>); 8]>;

/// An ordered set of key value paris that defines the time series that a line
/// of v3 line protocol is associated with.
#[derive(Debug)]
pub struct SeriesKey<'a>(SeriesKeyInner<'a>);

impl<'a> SeriesKey<'a> {
    /// Create a new `SeriesKey`
    fn new() -> Self {
        Self(SmallVec::new())
    }

    /// Get an `Iterator` over the keys of a series key
    pub fn keys(&self) -> impl Iterator<Item = &EscapedStr<'a>> {
        self.0.iter().map(|(k, _)| k)
    }

    /// Get an `Iterator` over the values of a series key
    pub fn values(&self) -> impl Iterator<Item = &SeriesValue<'a>> {
        self.0.iter().map(|(_, v)| v)
    }
}

impl<'a> Deref for SeriesKey<'a> {
    type Target = SeriesKeyInner<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for SeriesKey<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// The value associated with a series key
///
/// Currently only strings are supported, but we may support other types in the future.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SeriesValue<'a> {
    String(EscapedStr<'a>),
}

impl<'a> PartialEq<String> for SeriesValue<'a> {
    fn eq(&self, other: &String) -> bool {
        match self {
            SeriesValue::String(s) => s == other,
        }
    }
}

impl<'a> PartialEq<&str> for SeriesValue<'a> {
    fn eq(&self, other: &&str) -> bool {
        match self {
            SeriesValue::String(s) => s.as_str() == *other,
        }
    }
}

/// Parse the lines in a body of v3 line protocol
pub fn parse_lines(input: &str) -> impl Iterator<Item = Result<ParsedLine<'_>>> {
    split_lines(input).filter_map(|line| {
        let i = trim_leading(line);

        if i.is_empty() {
            return None;
        }

        let res = match parse_line(i) {
            Ok((remaining, line)) => {
                // should have parsed the whole input line; if any
                // data remains it is a parse error for this line.
                if !remaining.is_empty() {
                    Some(Err(Error::CannotParseEntireLine {
                        trailing_content: String::from(remaining),
                    }))
                } else {
                    Some(Ok(line))
                }
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Some(Err(e)),
            Err(nom::Err::Incomplete(_)) => unreachable!("Cannot have incomplete data"), // Only streaming parsers have this
        };

        if let Some(Err(r)) = &res {
            debug!("Error parsing line: '{}'. Error was {:?}", line, r);
        }
        res
    })
}

fn parse_line(i: &str) -> IResult<&str, ParsedLine<'_>> {
    let field_set = preceded(whitespace, field_set);
    let timestamp = preceded(whitespace, terminated(timestamp, opt(whitespace)));

    let line = tuple((series, field_set, opt(timestamp)));

    map(line, |(series, field_set, timestamp)| ParsedLine {
        series,
        field_set,
        timestamp,
    })(i)
}

fn series(i: &str) -> IResult<&str, Series<'_>> {
    let series = tuple((measurement, maybe_series_key));
    let series_and_raw_input = parse_and_recognize(series);

    map(
        series_and_raw_input,
        |(raw_input, (measurement, series_key))| Series {
            raw_input,
            measurement,
            series_key,
        },
    )(i)
}

/// Series Keys are optional, but similar to tags, if a comma follows the measurement, then we
/// must have atleast one key/value pair, otherwise it's an error.
fn maybe_series_key(i: &str) -> IResult<&str, Option<SeriesKey<'_>>, Error> {
    match tag::<&str, &str, Error>(",")(i) {
        Err(nom::Err::Error(_)) => Ok((i, None)),
        Ok((remainder, _)) => match series_key(remainder) {
            Ok((i, sk)) => {
                // If we get here then there should be atleast one series key/value pair
                if sk.is_empty() {
                    Err(nom::Err::Error(Error::SeriesKeyMalformed))
                } else {
                    Ok((i, Some(sk)))
                }
            }
            Err(nom::Err::Error(_)) => SeriesKeyMalformedSnafu.fail().map_err(nom::Err::Error),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}

fn series_key(i: &str) -> IResult<&str, SeriesKey<'_>> {
    let one_key = separated_pair(series_key_key, tag("/"), series_key_value);
    parameterized_separated_list(tag("/"), one_key, SeriesKey::new, |v, i| v.push(i))(i)
}

fn series_key_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != '/' && c != '\\');

    escaped_value(normal_char)(i)
}

fn series_key_value(i: &str) -> IResult<&str, SeriesValue<'_>> {
    let string = map(series_key_string_value, SeriesValue::String);

    alt((string,))(i)
}

fn series_key_string_value(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| !is_whitespace_boundary_char(c) && c != '/' && c != '\\');

    escaped_value(normal_char)(i)
}

#[cfg(test)]
mod tests {
    use crate::{v3::SeriesValue, Error, EscapedStr, FieldValue};

    use super::ParsedLine;

    fn parse(s: &str) -> Result<Vec<ParsedLine<'_>>, Error> {
        super::parse_lines(s).collect()
    }

    #[test]
    fn parse_line_with_series_key() {
        let input = "foo,key1/val1/key2/val2 value=1 123";
        let vals = parse(input).unwrap();
        assert_eq!(vals[0].series.measurement, "foo");
        let sk = vals[0].series.series_key.as_ref().unwrap();
        assert_eq!(sk[0].0, "key1");
        assert_eq!(sk[0].1, "val1");
        assert_eq!(sk[1].0, "key2");
        assert_eq!(sk[1].1, "val2");
        let fs = &vals[0].field_set;
        assert_eq!(fs.len(), 1);
        assert_eq!(fs[0].0, "value");
        assert!(matches!(fs[0].1, FieldValue::F64(1.0)));
        let ts = &vals[0].timestamp;
        assert!(matches!(ts, Some(123)));
    }

    #[test]
    fn parse_line_no_series_key() {
        let input = "foo value=1 123";
        let res = parse(input);
        println!("result: {res:#?}");
        let vals = res.unwrap();
        assert_eq!(vals.len(), 1);
        assert!(vals[0].series.series_key.is_none());
        let fs = &vals[0].field_set;
        assert_eq!(fs.len(), 1);
        assert_eq!(fs[0].0, "value");
        assert!(matches!(fs[0].1, FieldValue::F64(1.0)));
        let ts = &vals[0].timestamp;
        assert!(matches!(ts, Some(123)));
    }

    #[test]
    fn test_series() {
        let input = "foo,bar/baz";
        let (r, s) = super::series(input).unwrap();
        assert!(r.is_empty());
        assert_eq!(s.measurement, "foo");
        assert!(s.series_key.is_some_and(|sk| {
            sk[0].0 == "bar"
                && matches!(sk[0].1, SeriesValue::String(EscapedStr::SingleSlice("baz")))
        }));
    }

    #[test]
    fn test_series_no_key() {
        let input = "foo val=1";
        let (r, s) = super::series(input).unwrap();
        assert_eq!(r, " val=1");
        assert_eq!(s.measurement, "foo");
        assert!(s.series_key.is_none());
    }

    #[test]
    fn test_series_key_key() {
        assert!(matches!(
            super::series_key_key("some_key"),
            Ok(("", EscapedStr::SingleSlice("some_key")))
        ));
        assert!(matches!(
            super::series_key_key("some_key/"),
            Ok(("/", EscapedStr::SingleSlice("some_key")))
        ));
    }

    #[test]
    fn test_series_key_value() {
        assert!(matches!(
            super::series_key_value("some_key"),
            Ok(("", SeriesValue::String(EscapedStr::SingleSlice("some_key"))))
        ));
        assert!(matches!(
            super::series_key_value("some_key "),
            Ok((
                " ",
                SeriesValue::String(EscapedStr::SingleSlice("some_key"))
            ))
        ));
        assert!(matches!(
            super::series_key_value("some_key/"),
            Ok((
                "/",
                SeriesValue::String(EscapedStr::SingleSlice("some_key"))
            ))
        ));
    }

    // Checks for malformed series keys using the main parser function `parse`
    //
    // This does not check for the error type, but just ensures that lines with malformed
    // series keys will fail to parse. If we can write a more specific parser that bubbles
    // up errors when there is a malformed series key, then that would be more ideal.
    #[test]
    fn test_series_key_malformed() {
        let test_cases = [
            "foo, value=1 123",
            "foo,  value=1 123",
            "foo,\t value=1 123",
            "foo,\n value=1 123",
            "foo,\\ value=1 123",
            "foo,bar value=1 123",
            "foo,bar  value=1 123",
            "foo,bar\t value=1 123",
            "foo,bar\n value=1 123",
            "foo,bar\\ value=1 123",
            "foo,bar/ value=1 123",
            "foo,bar// value=1 123",
            "foo,bar /baz value=1 123",
            "foo,bar\n/baz value=1 123",
            "foo,bar\t/baz value=1 123",
            "foo,bar\\/baz value=1 123",
            "foo,bar/baz/cat value=1 123",
            "foo,bar/baz/cat  value=1 123",
            "foo,bar/baz/cat\t value=1 123",
            "foo,bar/baz/cat\n value=1 123",
            "foo,bar/baz/cat\\ value=1 123",
            "foo,bar/baz/cat/ value=1 123",
            "foo,bar/baz/cat// value=1 123",
            "foo,bar/baz/cat /dog value=1 123",
            "foo,bar/baz/cat\t/dog value=1 123",
            "foo,bar/baz/cat\n/dog value=1 123",
            "foo,bar/baz/cat\\/dog value=1 123",
        ];
        for t in test_cases {
            println!("input: {t}");
            let res = parse(t);
            println!("result: {res:#?}");
            assert!(res.is_err());
        }
    }
}
