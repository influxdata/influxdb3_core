//! A panic-safe write abstraction for [`MutableBatch`]

use crate::{
    column::{Column, ColumnData, NULL_DID},
    noop_validator::NoopValidator,
    MutableBatch,
};
use arrow_util::bitset::{iter_set_positions, iter_set_positions_with_offset, BitSet};
use data_types::{IsNan, StatValues, Statistics};
use hashbrown::hash_map::RawEntryMut;
use schema::{InfluxColumnType, InfluxFieldType};
use snafu::{ResultExt, Snafu};
use std::{num::NonZeroU64, ops::Range};

#[allow(missing_docs, missing_copy_implementations)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Unable to insert {inserted} type into column {column} with type {existing}"
    ))]
    TypeMismatch {
        column: String,
        existing: InfluxColumnType,
        inserted: InfluxColumnType,
    },

    #[snafu(display("Incorrect number of values provided"))]
    InsufficientValues,

    #[snafu(display("Key not found in dictionary: {}", key))]
    KeyNotFound { key: usize },

    #[snafu(display("Could not insert column: {source}"))]
    ColumnInsertionRejected { source: InvalidInsertionError },
}

/// A specialized `Error` for [`Writer`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum InvalidInsertionError {
    #[snafu(display(
        "Existing table schema specifies column {column} is type {table_type}, but type {given} was given"
    ))]
    TableSchemaConflict {
        column: String,
        table_type: InfluxColumnType,
        given: InfluxColumnType,
    },
}

/// A type capable of checking the validity of a column insertion into a
/// [`MutableBatch`] using information external to the writer.
pub trait ColumnInsertValidator {
    /// Validates whether a new column with `col_name` and `col_type` can be
    /// added to the writer's [`MutableBatch`]
    fn validate_insertion(
        &self,
        col_name: &str,
        col_type: InfluxColumnType,
    ) -> std::result::Result<(), InvalidInsertionError>;
}

impl<T> ColumnInsertValidator for &T
where
    T: ColumnInsertValidator,
{
    fn validate_insertion(
        &self,
        col_name: &str,
        col_type: InfluxColumnType,
    ) -> std::result::Result<(), InvalidInsertionError> {
        T::validate_insertion(self, col_name, col_type)
    }
}

/// A no-op [`ColumnInsertValidator`] implementation that always allows an
/// insert to proceed.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopColumnInsertValidator;

impl ColumnInsertValidator for NoopColumnInsertValidator {
    fn validate_insertion(
        &self,
        _col_name: &str,
        _col_type: schema::InfluxColumnType,
    ) -> Result<(), InvalidInsertionError> {
        Ok(())
    }
}

/// [`Writer`] provides a panic-safe abstraction to append a number of rows to a [`MutableBatch`]
///
/// If a [`Writer`] is dropped without calling [`Writer::commit`], the [`MutableBatch`] will be
/// truncated to the original number of rows, and the statistics not updated
#[derive(Debug)]
pub struct Writer<'a, T> {
    /// A check is delegated to this validator before a new, unseen column is
    /// added to `batch`, in order to allow consumers of the writer to impose
    /// additional constraints outside of the writer's knowledge.
    column_insert_validator: T,
    /// The mutable batch that is being mutated
    batch: &'a mut MutableBatch,
    /// A list of column index paired with Statistics
    ///
    /// Statistics updates are deferred to commit time
    statistics: Vec<(usize, Statistics)>,
    /// The initial number of rows in the MutableBatch
    initial_rows: usize,
    /// The initial number of columns in the MutableBatch
    initial_cols: usize,
    /// The number of rows to insert
    to_insert: usize,
    /// If this Writer committed successfully
    success: bool,
}
impl<'a> Writer<'a, NoopValidator> {
    /// Create a [`Writer`] for inserting `to_insert` rows to the provided `batch`
    ///
    /// If the writer is dropped without calling commit all changes will be rolled back
    pub fn new(batch: &'a mut MutableBatch, to_insert: usize) -> Self {
        Self::new_with_column_validator(batch, to_insert, Default::default())
    }
}

impl<'a, T> Writer<'a, T>
where
    T: ColumnInsertValidator,
{
    /// Create a [`Writer`] for inserting `to_insert` rows into the provided
    /// `batch`, accepting new columns only on passing checks from the
    /// [`ColumnInsertValidator`].
    ///
    /// If the writer is dropped without calling commit all changes will be rolled back
    pub fn new_with_column_validator(
        batch: &'a mut MutableBatch,
        to_insert: usize,
        column_insert_validator: T,
    ) -> Self {
        let initial_rows = batch.rows();
        let initial_cols = batch.columns.len();
        Self {
            column_insert_validator,
            batch,
            statistics: vec![],
            initial_rows,
            initial_cols,
            to_insert,
            success: false,
        }
    }

    /// Write the f64 typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_f64<I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = f64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::Float))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::F64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_f64);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value);
                }
            }
            x => unreachable!("expected f64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::F64(stats)));

        Ok(())
    }

    /// Write the i64 typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_i64<I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = i64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::Integer))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::I64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_i64);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value);
                }
            }
            x => unreachable!("expected i64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::I64(stats)));

        Ok(())
    }

    /// Write the u64 typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_u64<I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = u64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::UInteger))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::U64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_u64);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value);
                }
            }
            x => unreachable!("expected u64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::U64(stats)));

        Ok(())
    }

    /// Write the boolean typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_bool<I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = bool>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::Boolean))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::Bool(col_data, _) => {
                col_data.append_unset(to_insert);
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    if value {
                        col_data.set(initial_rows + idx);
                    }
                    stats.update(&value);
                }
            }
            x => unreachable!("expected bool got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::Bool(stats)));

        Ok(())
    }

    /// Write the string field typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_string<'s, I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = &'s str>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) =
            self.column_mut(name, InfluxColumnType::Field(InfluxFieldType::String))?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::String(col_data, _) => {
                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data.extend(initial_rows + idx - col_data.len());
                    col_data.append(value);
                    stats.update(value);
                }
                col_data.extend(initial_rows + to_insert - col_data.len());
            }
            x => unreachable!("expected tag got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::String(stats)));

        Ok(())
    }

    /// Write the tag typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_tag<'s, I>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut values: I,
    ) -> Result<()>
    where
        I: Iterator<Item = &'s str>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, InfluxColumnType::Tag)?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::Tag(col_data, dict, _) => {
                col_data.resize(initial_rows + to_insert, NULL_DID);

                for idx in set_position_iterator(valid_mask, to_insert) {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = dict.lookup_value_or_insert(value);
                    stats.update(value);
                }
            }
            x => unreachable!("expected tag got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::String(stats)));

        Ok(())
    }

    /// Write the tag typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_tag_dict<'s, K, V>(
        &mut self,
        name: &str,
        valid_mask: Option<&[u8]>,
        mut keys: K,
        values: V,
    ) -> Result<()>
    where
        K: Iterator<Item = usize>,
        V: Iterator<Item = &'s str>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, InfluxColumnType::Tag)?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::Tag(col_data, dict, _) => {
                // Lazily compute mappings to handle dictionaries with unused mappings
                let mut mapping: Vec<_> = values.map(|value| (value, None)).collect();

                col_data.resize(initial_rows + to_insert, NULL_DID);

                for idx in set_position_iterator(valid_mask, to_insert) {
                    let key = keys.next().ok_or(Error::InsufficientValues)?;
                    let (value, maybe_did) =
                        mapping.get_mut(key).ok_or(Error::KeyNotFound { key })?;

                    match maybe_did {
                        Some(did) => col_data[initial_rows + idx] = *did,
                        None => {
                            let did = dict.lookup_value_or_insert(value);
                            *maybe_did = Some(did);
                            col_data[initial_rows + idx] = did
                        }
                    }
                    stats.update(*value);
                }
            }
            x => unreachable!("expected tag got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, valid_mask, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::String(stats)));

        Ok(())
    }

    /// Write the time typed column identified by `name`
    ///
    /// For each set bit in `valid_mask` an a value from `values` is inserted at the
    /// corresponding index in the column. Nulls are inserted for the other rows
    ///
    /// # Panic
    ///
    /// - panics if this column has already been written to by this `Writer`
    ///
    pub fn write_time<I>(&mut self, name: &str, mut values: I) -> Result<()>
    where
        I: Iterator<Item = i64>,
    {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;

        let (col_idx, col) = self.column_mut(name, InfluxColumnType::Timestamp)?;

        let mut stats = StatValues::new_empty();
        match &mut col.data {
            ColumnData::I64(col_data, _) => {
                col_data.resize(initial_rows + to_insert, 0_i64);
                for idx in 0..to_insert {
                    let value = values.next().ok_or(Error::InsufficientValues)?;
                    col_data[initial_rows + idx] = value;
                    stats.update(&value)
                }
            }
            x => unreachable!("expected i64 got {} for column \"{}\"", x, name),
        }

        append_valid_mask(col, None, to_insert);

        stats.update_for_nulls(to_insert as u64 - stats.total_count);
        self.statistics.push((col_idx, Statistics::I64(stats)));

        Ok(())
    }

    /// Write the provided MutableBatch
    pub(crate) fn write_batch(&mut self, src: &MutableBatch) -> Result<()> {
        assert_eq!(src.row_count, self.to_insert);

        for (src_col_name, src_col_idx) in &src.column_names {
            let src_col = &src.columns[*src_col_idx];
            let (dst_col_idx, dst_col) = self.column_mut(src_col_name, src_col.influx_type)?;

            let stats = match (&mut dst_col.data, &src_col.data) {
                (ColumnData::F64(dst_data, _), ColumnData::F64(src_data, stats)) => {
                    dst_data.extend_from_slice(src_data);
                    Statistics::F64(stats.clone())
                }
                (ColumnData::I64(dst_data, _), ColumnData::I64(src_data, stats)) => {
                    dst_data.extend_from_slice(src_data);
                    Statistics::I64(stats.clone())
                }
                (ColumnData::U64(dst_data, _), ColumnData::U64(src_data, stats)) => {
                    dst_data.extend_from_slice(src_data);
                    Statistics::U64(stats.clone())
                }
                (ColumnData::Bool(dst_data, _), ColumnData::Bool(src_data, stats)) => {
                    dst_data.extend_from(src_data);
                    Statistics::Bool(stats.clone())
                }
                (ColumnData::String(dst_data, _), ColumnData::String(src_data, stats)) => {
                    dst_data.extend_from(src_data);
                    Statistics::String(stats.clone())
                }
                (
                    ColumnData::Tag(dst_data, dst_dict, _),
                    ColumnData::Tag(src_data, src_dict, stats),
                ) => {
                    let mapping: Vec<_> = src_dict
                        .values()
                        .iter()
                        .map(|value| dst_dict.lookup_value_or_insert(value))
                        .collect();

                    dst_data.extend(src_data.iter().map(|src_id| match *src_id {
                        NULL_DID => NULL_DID,
                        _ => mapping[*src_id as usize],
                    }));

                    Statistics::String(stats.clone())
                }
                _ => unreachable!("src: {}, dst: {}", src_col.data, dst_col.data),
            };

            dst_col.valid.extend_from(&src_col.valid);
            self.statistics.push((dst_col_idx, stats));
        }
        Ok(())
    }

    /// Write `range` rows from the provided MutableBatch
    pub(crate) fn write_batch_range(
        &mut self,
        src: &MutableBatch,
        range: Range<usize>,
    ) -> Result<()> {
        self.write_batch_ranges(src, &[range])
    }

    /// Write the rows identified by `ranges` to the provided MutableBatch
    pub(crate) fn write_batch_ranges(
        &mut self,
        src: &MutableBatch,
        ranges: &[Range<usize>],
    ) -> Result<()> {
        let to_insert = self.to_insert;

        if to_insert == src.row_count {
            return self.write_batch(src);
        }

        for (src_col_name, src_col_idx) in &src.column_names {
            let src_col = &src.columns[*src_col_idx];
            let (dst_col_idx, dst_col) = self.column_mut(src_col_name, src_col.influx_type)?;
            let stats = match (&mut dst_col.data, &src_col.data) {
                (ColumnData::F64(dst_data, _), ColumnData::F64(src_data, _)) => Statistics::F64(
                    write_slice(to_insert, ranges, src_col.valid.bytes(), src_data, dst_data),
                ),
                (ColumnData::I64(dst_data, _), ColumnData::I64(src_data, _)) => Statistics::I64(
                    write_slice(to_insert, ranges, src_col.valid.bytes(), src_data, dst_data),
                ),
                (ColumnData::U64(dst_data, _), ColumnData::U64(src_data, _)) => Statistics::U64(
                    write_slice(to_insert, ranges, src_col.valid.bytes(), src_data, dst_data),
                ),
                (ColumnData::Bool(dst_data, _), ColumnData::Bool(src_data, _)) => {
                    dst_data.reserve(to_insert);
                    let mut stats = StatValues::new_empty();
                    for range in ranges {
                        dst_data.extend_from_range(src_data, range.clone());
                        compute_bool_stats(
                            src_col.valid.bytes(),
                            range.clone(),
                            src_data,
                            &mut stats,
                        )
                    }
                    Statistics::Bool(stats)
                }
                (ColumnData::String(dst_data, _), ColumnData::String(src_data, _)) => {
                    let mut stats = StatValues::new_empty();
                    for range in ranges {
                        dst_data.extend_from_range(src_data, range.clone());
                        compute_stats(src_col.valid.bytes(), range.clone(), &mut stats, |x| {
                            src_data.get(x).unwrap()
                        })
                    }
                    Statistics::String(stats)
                }
                (
                    ColumnData::Tag(dst_data, dst_dict, _),
                    ColumnData::Tag(src_data, src_dict, _),
                ) => {
                    dst_data.reserve(to_insert);

                    let mut mapping: Vec<_> = vec![None; src_dict.values().len()];
                    let mut stats = StatValues::new_empty();
                    for range in ranges {
                        dst_data.extend(src_data[range.clone()].iter().map(
                            |src_id| match *src_id {
                                NULL_DID => {
                                    stats.update_for_nulls(1);
                                    NULL_DID
                                }
                                _ => {
                                    let maybe_did = &mut mapping[*src_id as usize];
                                    match maybe_did {
                                        Some(did) => {
                                            stats.total_count += 1;
                                            *did
                                        }
                                        None => {
                                            let value = src_dict.lookup_id(*src_id).unwrap();
                                            stats.update(value);

                                            let did = dst_dict.lookup_value_or_insert(value);
                                            *maybe_did = Some(did);
                                            did
                                        }
                                    }
                                }
                            },
                        ));
                    }

                    Statistics::String(stats)
                }
                _ => unreachable!(),
            };

            dst_col.valid.reserve(to_insert);
            for range in ranges {
                dst_col
                    .valid
                    .extend_from_range(&src_col.valid, range.clone());
            }

            self.statistics.push((dst_col_idx, stats));
        }
        Ok(())
    }

    fn column_mut(
        &mut self,
        name: &str,
        influx_type: InfluxColumnType,
    ) -> Result<(usize, &mut Column)> {
        let columns_len = self.batch.columns.len();

        // Fetch the index of the column with `name`, inserting a new column
        // into the writer's batch if none exists and the column insert
        // validator allows.
        let column_idx = *match self.batch.column_names.raw_entry_mut().from_key(name) {
            RawEntryMut::Occupied(ref v) => v.get(),
            RawEntryMut::Vacant(v) => {
                self.column_insert_validator
                    .validate_insertion(name, influx_type)
                    .context(ColumnInsertionRejectedSnafu)?;
                v.insert(name.to_string(), columns_len).1
            }
        };

        if columns_len == column_idx {
            self.batch
                .columns
                .push(Column::new(self.initial_rows, influx_type))
        }

        let col = &mut self.batch.columns[column_idx];

        if col.influx_type != influx_type {
            return Err(Error::TypeMismatch {
                column: name.to_string(),
                existing: col.influx_type,
                inserted: influx_type,
            });
        }

        assert_eq!(
            col.valid.len(),
            self.initial_rows,
            "expected {} rows in column \"{}\" got {} when performing write of {} rows",
            self.initial_rows,
            name,
            col.valid.len(),
            self.to_insert
        );

        Ok((column_idx, col))
    }

    /// Commits the writes performed on this [`Writer`]. This will update the statistics
    /// and pad any unwritten columns with nulls
    pub fn commit(mut self) {
        let initial_rows = self.initial_rows;
        let to_insert = self.to_insert;
        let final_rows = initial_rows + to_insert;

        self.statistics
            .sort_unstable_by_key(|(col_idx, _)| *col_idx);
        let mut statistics = self.statistics.iter();

        for (col_idx, col) in self.batch.columns.iter_mut().enumerate() {
            // All columns should either have received a write and have statistics or not
            if col.valid.len() == initial_rows {
                col.push_nulls_to_len(final_rows);
            } else {
                assert_eq!(
                    col.valid.len(),
                    final_rows,
                    "expected {} rows in column index {} got {} when performing write of {} rows",
                    final_rows,
                    col_idx,
                    col.valid.len(),
                    to_insert
                );

                let (stats_col_idx, stats) = statistics.next().unwrap();
                assert_eq!(*stats_col_idx, col_idx);
                assert_eq!(stats.total_count(), to_insert as u64);

                match (&mut col.data, stats) {
                    (ColumnData::F64(col_data, stats), Statistics::F64(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::I64(col_data, stats), Statistics::I64(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::U64(col_data, stats), Statistics::U64(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::String(col_data, stats), Statistics::String(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::Bool(col_data, stats), Statistics::Bool(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                    }
                    (ColumnData::Tag(col_data, dict, stats), Statistics::String(new)) => {
                        assert_eq!(col_data.len(), final_rows);
                        stats.update_from(new);
                        stats.distinct_count = match stats.null_count {
                            Some(0) => NonZeroU64::new(dict.values().len() as u64),
                            Some(_) => NonZeroU64::new(dict.values().len() as u64 + 1),
                            None => unreachable!("mutable batch keeps null counts"),
                        }
                    }
                    _ => unreachable!("column: {}, statistics: {}", col.data, stats.type_name()),
                }
            }
        }
        self.batch.row_count = final_rows;
        self.success = true;
    }
}

fn set_position_iterator(
    valid_mask: Option<&[u8]>,
    to_insert: usize,
) -> impl Iterator<Item = usize> + '_ {
    match valid_mask {
        Some(mask) => itertools::Either::Left(
            iter_set_positions(mask).take_while(move |idx| *idx < to_insert),
        ),
        None => itertools::Either::Right(0..to_insert),
    }
}

fn append_valid_mask(column: &mut Column, valid_mask: Option<&[u8]>, to_insert: usize) {
    match valid_mask {
        Some(mask) => column.valid.append_bits(to_insert, mask),
        None => column.valid.append_set(to_insert),
    }
}

fn compute_bool_stats(
    valid: &[u8],
    range: Range<usize>,
    col_data: &BitSet,
    stats: &mut StatValues<bool>,
) {
    // There are likely faster ways to do this
    let indexes =
        iter_set_positions_with_offset(valid, range.start).take_while(|idx| *idx < range.end);

    let mut non_null_count = 0_u64;
    for index in indexes {
        let value = col_data.get(index);
        stats.update(&value);
        non_null_count += 1;
    }

    let to_insert = range.end - range.start;
    stats.update_for_nulls(to_insert as u64 - non_null_count);
}

fn write_slice<T>(
    to_insert: usize,
    ranges: &[Range<usize>],
    valid: &[u8],
    src_data: &[T],
    dst_data: &mut Vec<T>,
) -> StatValues<T>
where
    T: Clone + PartialOrd + IsNan,
{
    dst_data.reserve(to_insert);
    let mut stats = StatValues::new_empty();
    for range in ranges {
        dst_data.extend_from_slice(&src_data[range.clone()]);
        compute_stats(valid, range.clone(), &mut stats, |x| &src_data[x]);
    }
    stats
}

fn compute_stats<'a, T, U, F>(
    valid: &[u8],
    range: Range<usize>,
    stats: &mut StatValues<T>,
    accessor: F,
) where
    U: 'a + ToOwned<Owned = T> + PartialOrd + ?Sized + IsNan,
    F: Fn(usize) -> &'a U,
    T: std::borrow::Borrow<U>,
{
    let values = iter_set_positions_with_offset(valid, range.start)
        .take_while(|idx| *idx < range.end)
        .map(accessor);

    let mut non_null_count = 0_u64;
    for value in values {
        stats.update(value);
        non_null_count += 1;
    }

    let to_insert = range.end - range.start;
    stats.update_for_nulls(to_insert as u64 - non_null_count);
}

impl<'a, T> Drop for Writer<'a, T> {
    fn drop(&mut self) {
        if !self.success {
            let initial_rows = self.initial_rows;
            let initial_cols = self.initial_cols;

            if self.batch.columns.len() != initial_cols {
                self.batch.columns.truncate(initial_cols);
                self.batch.column_names.retain(|_, v| *v < initial_cols)
            }

            for col in &mut self.batch.columns {
                col.valid.truncate(initial_rows);
                match &mut col.data {
                    ColumnData::F64(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::I64(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::U64(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::String(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::Bool(col_data, _) => col_data.truncate(initial_rows),
                    ColumnData::Tag(col_data, dict, _) => {
                        col_data.truncate(initial_rows);
                        match col_data.iter().max() {
                            Some(max) => dict.truncate(*max),
                            None => dict.clear(),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    struct AlwaysRejectingValidator<F> {
        return_err: F,
    }

    impl<F> ColumnInsertValidator for AlwaysRejectingValidator<F>
    where
        F: Fn() -> InvalidInsertionError,
    {
        fn validate_insertion(
            &self,
            _col_name: &str,
            _col_type: InfluxColumnType,
        ) -> std::result::Result<(), InvalidInsertionError> {
            Err((self.return_err)())
        }
    }

    /// A basic test to assert that each write method results in the
    /// [`ColumnInsertValidator`] being invoked.
    #[test]
    fn writer_delegates_to_column_validator() {
        const COLUMN: &str = "anything";

        let mut b = MutableBatch::new();
        // Provide the writer with a validator that always returns a well know error.
        let mut w = Writer::new_with_column_validator(
            &mut b,
            7,
            AlwaysRejectingValidator {
                return_err: || InvalidInsertionError::TableSchemaConflict {
                    column: COLUMN.to_string(),
                    table_type: InfluxColumnType::Timestamp,
                    given: InfluxColumnType::Tag,
                },
            },
        );

        assert_matches!(
            w.write_tag("bread", None, std::iter::once("fluffy")),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_tag_dict(
                "bread",
                None,
                vec![1, 0, 0, 1].into_iter(),
                vec!["fluffy", "stale"].into_iter(),
            ),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_f64("foo", None, std::iter::once(4.2)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_i64("bar", None, std::iter::once(-42)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_u64("baz", None, std::iter::once(42)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_string("bananas", None, std::iter::once("great")),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
        assert_matches!(
            w.write_bool("answer", None, std::iter::once(true)),
            Err(Error::ColumnInsertionRejected { source }) => {
                assert_matches!(source, InvalidInsertionError::TableSchemaConflict { column, .. } if column == COLUMN)
            }
        );
    }
}
