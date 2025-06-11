//! Types and parsers for an InfluxQL statement.

use crate::create::{CreateDatabaseStatement, create_statement};
use crate::delete::{DeleteStatement, delete_statement};
use crate::drop::{DropMeasurementStatement, drop_statement};
use crate::explain::{ExplainStatement, explain_statement};
use crate::internal::ParseResult;
use crate::select::{SelectStatement, select_statement};
use crate::show::{ShowDatabasesStatement, show_statement};
use crate::show_field_keys::ShowFieldKeysStatement;
use crate::show_measurements::ShowMeasurementsStatement;
use crate::show_retention_policies::ShowRetentionPoliciesStatement;
use crate::show_tag_keys::ShowTagKeysStatement;
use crate::show_tag_values::ShowTagValuesStatement;
use nom::Parser;
use nom::branch::alt;
use nom::combinator::map;
use std::fmt::{Display, Formatter};

/// An InfluxQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// Represents a `CREATE DATABASE` statement.
    CreateDatabase(Box<CreateDatabaseStatement>),
    /// Represents a `DELETE` statement.
    Delete(Box<DeleteStatement>),
    /// Represents a `DROP MEASUREMENT` statement.
    DropMeasurement(Box<DropMeasurementStatement>),
    /// Represents an `EXPLAIN` statement.
    Explain(Box<ExplainStatement>),
    /// Represents a `SELECT` statement.
    Select(Box<SelectStatement>),
    /// Represents a `SHOW DATABASES` statement.
    ShowDatabases(Box<ShowDatabasesStatement>),
    /// Represents a `SHOW MEASUREMENTS` statement.
    ShowMeasurements(Box<ShowMeasurementsStatement>),
    /// Represents a `SHOW RETENTION POLICIES` statement.
    ShowRetentionPolicies(Box<ShowRetentionPoliciesStatement>),
    /// Represents a `SHOW TAG KEYS` statement.
    ShowTagKeys(Box<ShowTagKeysStatement>),
    /// Represents a `SHOW TAG VALUES` statement.
    ShowTagValues(Box<ShowTagValuesStatement>),
    /// Represents a `SHOW FIELD KEYS` statement.
    ShowFieldKeys(Box<ShowFieldKeysStatement>),
}

impl Statement {
    /// Is this a `SHOW DATABASES` statement
    pub fn is_show_databases(&self) -> bool {
        matches!(self, Self::ShowDatabases(_))
    }

    /// Is this a `SHOW RETENTION POLICIES` statement
    pub fn is_show_retention_policies(&self) -> bool {
        matches!(self, Self::ShowRetentionPolicies(_))
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateDatabase(s) => Display::fmt(s, f),
            Self::Delete(s) => Display::fmt(s, f),
            Self::DropMeasurement(s) => Display::fmt(s, f),
            Self::Explain(s) => Display::fmt(s, f),
            Self::Select(s) => Display::fmt(s, f),
            Self::ShowDatabases(s) => Display::fmt(s, f),
            Self::ShowMeasurements(s) => Display::fmt(s, f),
            Self::ShowRetentionPolicies(s) => Display::fmt(s, f),
            Self::ShowTagKeys(s) => Display::fmt(s, f),
            Self::ShowTagValues(s) => Display::fmt(s, f),
            Self::ShowFieldKeys(s) => Display::fmt(s, f),
        }
    }
}

/// Parse a single InfluxQL statement.
pub fn statement(i: &str) -> ParseResult<&str, Statement> {
    alt((
        map(delete_statement, |s| Statement::Delete(Box::new(s))),
        map(drop_statement, |s| Statement::DropMeasurement(Box::new(s))),
        map(explain_statement, |s| Statement::Explain(Box::new(s))),
        map(select_statement, |s| Statement::Select(Box::new(s))),
        create_statement,
        show_statement,
    ))
    .parse(i)
}

#[cfg(test)]
mod test {
    use crate::statement;

    #[test]
    fn test_statement() {
        // Validate one of each statement parser is accepted and that all input is consumed

        // create_statement combinator
        let (got, _) = statement("CREATE DATABASE foo").unwrap();
        assert_eq!(got, "");

        // delete_statement combinator
        let (got, _) = statement("DELETE FROM foo").unwrap();
        assert_eq!(got, "");

        // drop_statement combinator
        let (got, _) = statement("DROP MEASUREMENT foo").unwrap();
        assert_eq!(got, "");

        // explain_statement combinator
        let (got, _) = statement("EXPLAIN SELECT * FROM cpu").unwrap();
        assert_eq!(got, "");

        let (got, _) = statement("SELECT * FROM foo WHERE time > now() - 5m AND host = 'bar' GROUP BY TIME(5m) FILL(previous) ORDER BY time DESC").unwrap();
        assert_eq!(got, "");

        // show_statement combinator
        let (got, _) = statement("SHOW TAG KEYS").unwrap();
        assert_eq!(got, "");
    }

    #[test]
    fn statement_helpers() {
        let (_, got) = statement("SHOW DATABASES").unwrap();
        assert_eq!(got.to_string(), "SHOW DATABASES");
        assert!(got.is_show_databases());
        assert!(!got.is_show_retention_policies());

        let (_, got) = statement("SHOW RETENTION POLICIES ON \"foo\"").unwrap();
        assert_eq!(got.to_string(), "SHOW RETENTION POLICIES ON foo");
        assert!(got.is_show_retention_policies());
        assert!(!got.is_show_databases());

        let (_, got) = statement("SHOW RETENTION POLICIES").unwrap();
        assert_eq!(got.to_string(), "SHOW RETENTION POLICIES");
        assert!(got.is_show_retention_policies());
        assert!(!got.is_show_databases());
    }
}
