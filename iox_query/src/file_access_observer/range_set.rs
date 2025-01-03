use std::ops::Range;

/// Normalized [`Range`] set of a file.
///
/// # Invariants
/// - **non-overlapping:** Ranges do NOT overlap.
/// - **sorted:** Ranges are sorted[^sorted].
/// - **non-empty:** None of the ranges is empty.
///
///
/// [^sorted]: Since the ranges do NOT overlap, they are sorted by both the start and the end position.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RangeSet {
    set: Box<[Range<usize>]>,

    /// Coalesce ranges.
    ///
    /// If two ranges are at most that many bytes apart, they are considered one range. This helps with fragmentation.
    coalesce: usize,
}

impl RangeSet {
    /// Create new set from given ranges.
    ///
    /// They input is not required to fullfill the invariants, i.e. it may contain overlapping ranges in a random order.
    ///
    /// If two ranges are at most `coalesce` bytes apart, they are considered one range. This helps with fragmentation.
    pub(crate) fn new(ranges: &[Range<usize>], coalesce: usize) -> Self {
        let mut ranges = ranges.to_vec();
        ranges.sort_unstable_by_key(|range| range.start);

        let mut set = Vec::with_capacity(ranges.len());
        let mut start_idx = 0;
        let mut end_idx = 1;

        while start_idx != ranges.len() {
            let mut range_end = ranges[start_idx].end;

            while end_idx != ranges.len()
                && ranges[end_idx]
                    .start
                    .checked_sub(range_end)
                    .map(|delta| delta <= coalesce)
                    .unwrap_or(true)
            {
                range_end = range_end.max(ranges[end_idx].end);
                end_idx += 1;
            }

            let start = ranges[start_idx].start;
            let end = range_end;
            if start < end {
                set.push(start..end);
            }

            start_idx = end_idx;
            end_idx += 1;
        }

        Self {
            set: set.into(),
            coalesce,
        }
    }

    /// Iterate over ranges.
    pub(crate) fn iter(&self) -> <&Self as IntoIterator>::IntoIter {
        self.into_iter()
    }

    /// Size of all ranges combined, in bytes.
    pub(crate) fn bytes(&self) -> usize {
        self.iter().map(|r| r.len()).sum()
    }

    /// Builds union of both sets.
    ///
    /// # Panic
    /// Both sets must have the same `coalesce` value.
    pub(crate) fn union(&self, other: &Self) -> Self {
        assert_eq!(self.coalesce, other.coalesce, "coalesce value differs");

        let mut ranges = Vec::with_capacity(self.bytes() + other.bytes());
        ranges.extend_from_slice(self.set.as_ref());
        ranges.extend_from_slice(other.set.as_ref());
        Self::new(&ranges, self.coalesce)
    }

    /// Returns ranges that are in `self`` but NOT in `other`.
    ///
    /// # Panic
    /// Both sets must have the same `coalesce` value.
    pub(crate) fn difference(&self, other: &Self) -> Self {
        assert_eq!(self.coalesce, other.coalesce, "coalesce value differs");

        let mut delta = Vec::with_capacity(self.set.len());
        let mut other_it = other.set.iter().peekable();
        'outer: for s in &self.set {
            let mut s = s.clone();

            loop {
                // ignore all `other` ranges that are before this range, because they cannot intersect
                while other_it
                    .peek()
                    .map(|o| o.end <= s.start)
                    .unwrap_or_default()
                {
                    //       |--S--)
                    // |--O--)
                    other_it.next();
                }

                match other_it.peek() {
                    // no `other` left
                    //
                    // |---S---)
                    // |---D---)
                    None => {
                        delta.push(s);
                        continue 'outer;
                    }
                    // `other` is past `self` and doesn't intersect, include `self` entirely
                    //
                    // |---S---)
                    //         |---O---)
                    // |---D---)
                    Some(o) if o.start >= s.end => {
                        delta.push(s);
                        continue 'outer;
                    }
                    // |---S---)
                    //     |---O---)
                    // |-D-)
                    //
                    // |---S---)
                    //     |-O-)
                    // |-D-)
                    Some(o) if o.start > s.start && o.end >= s.end => {
                        assert!(o.start < s.end);
                        delta.push(s.start..o.start);
                        continue 'outer;
                    }
                    //   |-S-)
                    // |---O---)
                    //
                    // |---S---)
                    // |---O---)
                    Some(o) if o.start <= s.start && o.end >= s.end => {
                        assert!(o.start < s.end);
                        continue 'outer;
                    }
                    //     |---S---)
                    // |---O---)
                    //         |-S-)
                    Some(o) if o.start <= s.start => {
                        assert!(o.start < s.end);
                        s = o.end..s.end;
                    }
                    // |---S---)
                    //   |-O-)
                    // |D)   |S)
                    Some(o) => {
                        assert!(o.start < s.end);
                        assert!(o.start > s.start);
                        delta.push(s.start..o.start);
                        s = o.end..s.end;
                    }
                }
            }
        }

        Self::new(&delta, self.coalesce)
    }
}

impl<'a> IntoIterator for &'a RangeSet {
    type Item = &'a Range<usize>;
    type IntoIter = std::slice::Iter<'a, Range<usize>>;

    fn into_iter(self) -> Self::IntoIter {
        self.set.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;

    #[test]
    #[allow(clippy::reversed_empty_ranges, clippy::single_range_in_vec_init)]
    fn test_new_cases() {
        assert_eq!(RangeSet::new(&[], 0), range_set_0(&[]),);
        assert_eq!(
            RangeSet::new(&[0..10, 12..13], 0),
            range_set_0(&[0..10, 12..13]),
        );
        assert_eq!(
            RangeSet::new(&[12..13, 0..10], 0),
            range_set_0(&[0..10, 12..13]),
        );
        assert_eq!(RangeSet::new(&[0..5, 3..10], 0), range_set_0(&[0..10]),);
        assert_eq!(
            RangeSet::new(&[0..5, 3..10, 10..12], 0),
            range_set_0(&[0..12]),
        );
        assert_eq!(RangeSet::new(&[3..3], 0), range_set_0(&[]),);
        assert_eq!(RangeSet::new(&[3..2], 0), range_set_0(&[]),);
        assert_eq!(RangeSet::new(&[0..10, 15..20], 5), range_set_5(&[0..20]),);
        assert_eq!(
            RangeSet::new(&[0..10, 16..20], 5),
            range_set_5(&[0..10, 16..20]),
        );
    }

    proptest! {
        #[test]
        fn test_new_proptest(
            ranges in prop::collection::vec(any::<Range<usize>>(), 0..20),
            coalesce in 0..usize::MAX,
        ) {
            let set = RangeSet::new(&ranges, coalesce);
            check_set_invariants(&set);
        }
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_union_cases() {
        assert_eq!(range_set_0(&[]).union(&range_set_0(&[])), range_set_0(&[]),);
        assert_eq!(
            range_set_0(&[3..3]).union(&range_set_0(&[3..3])),
            range_set_0(&[]),
        );
        assert_eq!(
            range_set_0(&[3..10]).union(&range_set_0(&[5..8])),
            range_set_0(&[3..10]),
        );
        assert_eq!(
            range_set_0(&[3..10, 19..20]).union(&range_set_0(&[1..2, 15..19])),
            range_set_0(&[1..2, 3..10, 15..20]),
        );
    }

    #[test]
    #[should_panic(expected = "coalesce value differs")]
    fn test_union_coalesce_check() {
        RangeSet::new(&[], 0).union(&RangeSet::new(&[], 1));
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_difference_cases() {
        assert_eq!(
            range_set_0(&[]).difference(&range_set_0(&[])),
            range_set_0(&[])
        );
        assert_eq!(
            range_set_0(&[10..20]).difference(&range_set_0(&[])),
            range_set_0(&[10..20])
        );
        assert_eq!(
            range_set_0(&[]).difference(&range_set_0(&[10..20])),
            range_set_0(&[])
        );
        assert_eq!(
            range_set_0(&[10..20]).difference(&range_set_0(&[10..20])),
            range_set_0(&[])
        );
        assert_eq!(
            range_set_0(&[0..2, 10..20, 30..31]).difference(&range_set_0(&[10..20])),
            range_set_0(&[0..2, 30..31])
        );
        assert_eq!(
            range_set_0(&[11..20]).difference(&range_set_0(&[10..15])),
            range_set_0(&[15..20])
        );
        assert_eq!(
            range_set_0(&[10..20]).difference(&range_set_0(&[10..15])),
            range_set_0(&[15..20])
        );
        assert_eq!(
            range_set_0(&[10..20]).difference(&range_set_0(&[15..20])),
            range_set_0(&[10..15])
        );
        assert_eq!(
            range_set_0(&[10..19]).difference(&range_set_0(&[15..20])),
            range_set_0(&[10..15])
        );
        assert_eq!(
            range_set_0(&[11..19]).difference(&range_set_0(&[10..20])),
            range_set_0(&[])
        );
        assert_eq!(
            range_set_0(&[10..20]).difference(&range_set_0(&[14..16])),
            range_set_0(&[10..14, 16..20])
        );
        assert_eq!(
            range_set_0(&[10..20]).difference(&range_set_0(&[10..11, 13..14, 16..17, 19..30])),
            range_set_0(&[11..13, 14..16, 17..19])
        );
        assert_eq!(
            range_set_5(&[10..20]).difference(&range_set_5(&[12..18])),
            range_set_5(&[10..12, 18..20])
        );
        assert_eq!(
            range_set_5(&[10..20]).difference(&range_set_5(&[13..18])),
            range_set_5(&[10..20])
        );
    }

    #[test]
    #[should_panic(expected = "coalesce value differs")]
    fn test_difference_coalesce_check() {
        RangeSet::new(&[], 0).difference(&RangeSet::new(&[], 1));
    }

    proptest! {
        #[test]
        fn test_difference_proptest(
            ranges_1 in prop::collection::vec(any::<Range<usize>>(), 0..20),
            ranges_2 in prop::collection::vec(any::<Range<usize>>(), 0..20),
            coalesce in 0..usize::MAX,
        ) {
            let set_1 = RangeSet::new(&ranges_1, coalesce);
            check_set_invariants(&set_1);
            let set_2 = RangeSet::new(&ranges_2, coalesce);
            check_set_invariants(&set_2);

            let set_3 = set_1.difference(&set_2);
            check_set_invariants(&set_3);

            // due to the coalesce logic, the size of the parts (set_2 and set_3) may by slightly larger than the whole
            // (set_1), but the whole is NEVER bigger than the parts
            assert!(set_1.bytes() <= set_2.bytes().saturating_add(set_3.bytes()));

            // the difference logic may introduce up to one additional range for every range in the "subtrahend"
            assert!(set_3.iter().len() <= set_1.iter().len() + set_2.iter().len());
        }
    }

    #[test]
    fn test_bytes() {
        assert_eq!(range_set_0(&[]).bytes(), 0);
        assert_eq!(range_set_0(&[10..13, 20..21]).bytes(), 4);
    }

    /// Build a [`RangeSet`] w/o checking invariants.
    ///
    /// This is done purely for testing.
    fn range_set_0(ranges: &[Range<usize>]) -> RangeSet {
        RangeSet {
            set: Box::from(ranges.to_vec()),
            coalesce: 0,
        }
    }

    /// Build a [`RangeSet`] w/o checking invariants.
    ///
    /// This is done purely for testing.
    fn range_set_5(ranges: &[Range<usize>]) -> RangeSet {
        RangeSet {
            set: Box::from(ranges.to_vec()),
            coalesce: 5,
        }
    }

    fn check_set_invariants(set: &RangeSet) {
        for r in &set.set {
            assert!(r.start < r.end);
        }

        for w in set.set.windows(2) {
            let a = &w[0];
            let b = &w[1];
            assert!(a.end + set.coalesce < b.start);
        }
    }
}
