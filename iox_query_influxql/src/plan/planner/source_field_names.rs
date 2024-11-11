use datafusion::common::DataFusionError;
use influxdb_influxql_parser::expression::VarRef;
use influxdb_influxql_parser::visit::{Recursion, Visitor};
use std::collections::BTreeSet;

/// Visitor that collects the names of all the fields referenced in an expression.
#[derive(Debug)]
pub(super) struct SourceFieldNamesVisitor<'a>(pub(super) &'a mut BTreeSet<String>);

impl<'a> Visitor for SourceFieldNamesVisitor<'a> {
    type Error = DataFusionError;
    fn pre_visit_var_ref(self, varref: &VarRef) -> Result<Recursion<Self>, Self::Error> {
        if let Some(dt) = varref.data_type {
            if dt.is_field_type() {
                self.0.insert(varref.name.clone().take());
            }
        }
        Ok(Recursion::Continue(self))
    }
}
