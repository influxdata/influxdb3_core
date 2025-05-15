//! A mechanism to hint what size the underlying object has.
//!
//! # Usage
//!
//! ```
//! # use object_store_size_hinting::{hint_size, extract_size_hint};
//! let options = hint_size(13);
//! let (_options, maybe_size) = extract_size_hint(options).unwrap();
//! assert_eq!(maybe_size.unwrap(), 13);
//! ```
//!
//! # Implementation
//! Until we have [`object_store` support for GET extensions](https://github.com/apache/arrow-rs/issues/7155),
//! this is a bit hacky. It uses [`GetOptions::range`] together with [`GetOptions::if_match`] to encode the size hint.

use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    Error, GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOpts, PutOptions, PutPayload, PutResult, Result, path::Path,
};

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

const IF_MATCH_VALUE: &str = "iox_size_hint";
const STORE_NAME: &str = "iox_size_hint";

/// Embed a size hint within [`GetOptions`].
///
/// Use [`extract_size_hint`] to extract the size again.
pub fn hint_size(size: usize) -> GetOptions {
    GetOptions {
        if_match: Some(IF_MATCH_VALUE.to_owned()),
        range: Some((0..size).into()),
        ..Default::default()
    }
}

/// Extract size hint from [`GetOptions`].
///
/// Returns the options with the potential size information stripped and the extracted size -- if there was a hint.
///
/// Use [`hint_size`] to create a size hint.
pub fn extract_size_hint(mut options: GetOptions) -> Result<(GetOptions, Option<usize>)> {
    if !options
        .if_match
        .as_ref()
        .map(|s| s == IF_MATCH_VALUE)
        .unwrap_or_default()
    {
        // this is NOT a size hint
        return Ok((options, None));
    }

    // strip if-match value
    options.if_match.take();

    let range = options
        .range
        .take()
        .ok_or_else(|| err("missing range".to_owned()))?;

    let GetRange::Bounded(range) = range else {
        return Err(err(format!("expected bounded range but got {range:?}")));
    };
    if range.start != 0 {
        return Err(err(format!(
            "range should start at zero but starts at {}",
            range.start
        )));
    }
    Ok((options, Some(range.end)))
}

fn err(msg: String) -> Error {
    Error::Generic {
        store: STORE_NAME,
        source: msg.into(),
    }
}

/// Wrapper that strips hints created with [`hint_size`] and only leaves the [`GetOptions::range`] part.
#[derive(Debug)]
pub struct ObjectStoreStripSizeHinting {
    inner: Arc<dyn ObjectStore>,
}

impl ObjectStoreStripSizeHinting {
    /// Create new wrapper.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Display for ObjectStoreStripSizeHinting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "strip_size_hinting({})", self.inner)
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ObjectStoreStripSizeHinting {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_opts(location, Default::default()).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (options, _size_hint) = extract_size_hint(options)?;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use futures::StreamExt;
    use object_store_mock::{DATA, MockCall, MockStore, WrappedGetOptions, object_meta, path};

    use super::*;

    #[test]
    fn test_roundtrip() {
        assert_roundtrip(0);
        assert_roundtrip(1);
        assert_roundtrip(1337);
        assert_roundtrip(usize::MAX);
    }

    #[test]
    #[expect(clippy::reversed_empty_ranges)]
    fn test_errors() {
        assert_err(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                ..Default::default()
            },
            "Generic iox_size_hint error: missing range",
        );
        assert_err(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                range: Some(GetRange::Offset(0)),
                ..Default::default()
            },
            "Generic iox_size_hint error: expected bounded range but got Offset(0)",
        );
        assert_err(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                range: Some(GetRange::Suffix(0)),
                ..Default::default()
            },
            "Generic iox_size_hint error: expected bounded range but got Suffix(0)",
        );
        assert_err(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                range: Some(GetRange::Bounded(1..12)),
                ..Default::default()
            },
            "Generic iox_size_hint error: range should start at zero but starts at 1",
        );
        assert_err(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                range: Some(GetRange::Bounded(1..1)),
                ..Default::default()
            },
            "Generic iox_size_hint error: range should start at zero but starts at 1",
        );
        assert_err(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                range: Some(GetRange::Bounded(1..0)),
                ..Default::default()
            },
            "Generic iox_size_hint error: range should start at zero but starts at 1",
        );
    }

    #[test]
    fn test_no_range() {
        assert_extract(GetOptions::default(), None);
        assert_extract(
            GetOptions {
                if_match: None,
                ..hint_size(42)
            },
            None,
        );
        assert_extract(options_all_fields_set(), None);
    }

    #[test]
    fn test_keep_fields() {
        assert_extract(
            GetOptions {
                if_match: Some(IF_MATCH_VALUE.to_owned()),
                range: Some((0..13).into()),
                ..options_all_fields_set()
            },
            Some(13),
        );
    }

    #[tokio::test]
    async fn test_strip_wrapper() {
        let path = path();

        let object_store = MockStore::new()
            .mock_next(MockCall::GetOpts {
                params: (path.clone(), GetOptions::default().into()),
                barriers: vec![],
                res: Ok(GetResult {
                    payload: object_store::GetResultPayload::Stream(
                        futures::stream::iter([Ok(Bytes::from_static(DATA))]).boxed(),
                    ),
                    meta: object_meta(),
                    range: 0..DATA.len(),
                    attributes: Default::default(),
                }),
            })
            .as_store();
        let object_store = ObjectStoreStripSizeHinting::new(object_store);

        assert_eq!(format!("{object_store}"), "strip_size_hinting(mock)");

        let actual_data = object_store
            .get_opts(&path, hint_size(DATA.len()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(actual_data, DATA);
    }

    #[track_caller]
    fn assert_roundtrip(size: usize) {
        let options = hint_size(size);
        assert_extract(options, Some(size));
    }

    #[track_caller]
    fn assert_extract(options: GetOptions, maybe_size: Option<usize>) {
        let (returned_options, extracted_size) = extract_size_hint(options.clone()).unwrap();

        let expected_options = if extracted_size.is_some() {
            GetOptions {
                if_match: None,
                range: None,
                ..options
            }
        } else {
            options
        };

        assert_eq!(
            WrappedGetOptions::from(returned_options),
            WrappedGetOptions::from(expected_options)
        );
        assert_eq!(extracted_size, maybe_size);
    }

    #[track_caller]
    fn assert_err(options: GetOptions, err: &'static str) {
        assert_eq!(extract_size_hint(options).unwrap_err().to_string(), err,);
    }

    fn options_all_fields_set() -> GetOptions {
        GetOptions {
            if_match: Some("val-if-match".to_owned()),
            if_none_match: Some("val-if-none-match".to_owned()),
            if_modified_since: Some(Utc.timestamp_nanos(42)),
            if_unmodified_since: Some(Utc.timestamp_nanos(43)),
            range: Some((0..13).into()),
            version: Some("val-version".to_owned()),
            head: true,
        }
    }
}
