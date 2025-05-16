use std::num::NonZeroUsize;

use crate::interface::Paginated;

pub(crate) fn paginate<T>(
    items: Vec<T>,
    page_number: NonZeroUsize,
    page_size: NonZeroUsize,
) -> Paginated<T> {
    let (page_number, page_size) = (page_number.get(), page_size.get());
    let offset = (page_number - 1) * page_size;
    let total = items.len();
    let pages = get_num_pages(total, page_size);
    let item_page = items.into_iter().skip(offset).take(page_size).collect();

    Paginated {
        items: item_page,
        total: total as i64,
        pages: pages as i64,
    }
}

fn get_num_pages(total: usize, page_size: usize) -> usize {
    total.div_ceil(page_size)
}

#[cfg(test)]
mod tests {

    use crate::interface::PaginationOptions;

    use super::*;

    struct TestCase {
        v: Vec<usize>,
        page_number: i32,
        page_size: i32,
        expected: Paginated<usize>,
    }

    #[test]
    fn test_pagination() {
        fn test_paginate(case_number: usize, case: TestCase) {
            let pagination_opts = PaginationOptions::from((case.page_number, case.page_size));
            let actual = paginate(
                case.v,
                pagination_opts.page_number,
                pagination_opts.page_size,
            );
            assert_eq!(actual, case.expected, "Test case {} failed", case_number);
        }
        let items = (1..100).collect::<Vec<usize>>();
        let total = items.len() as i64;
        let default_page_size = PaginationOptions::default().page_size.get();
        let default_page_number = PaginationOptions::default().page_number.get();
        let cases = vec![
            // 1. Return the first page of the input
            TestCase {
                v: items.clone(),
                page_number: 1,
                page_size: 2,
                expected: Paginated {
                    items: vec![1, 2],
                    total,
                    pages: 50,
                },
            },
            // 2. Return the last page of the input
            TestCase {
                v: items.clone(),
                page_number: 3,
                page_size: 2,
                expected: Paginated {
                    items: vec![5, 6],
                    total,
                    pages: 50,
                },
            },
            // 3. Page numbers that do not exist will return the default page (1)
            TestCase {
                v: items.clone(),
                page_number: 0,
                page_size: 1,
                expected: Paginated {
                    items: vec![1],
                    total,
                    pages: 99,
                },
            },
            // 4. Page number/size cannot be 0 and thus will be defaulted
            TestCase {
                v: items.clone(),
                page_number: 0,
                page_size: 0,
                expected: Paginated {
                    items: items
                        .iter()
                        .skip((default_page_number - 1) * default_page_size)
                        .take(default_page_size)
                        .copied()
                        .collect(),
                    total,
                    pages: get_num_pages(total as usize, default_page_size) as i64,
                },
            },
            // 5. Page number/size cannot be negative and thus will be defaulted
            TestCase {
                v: items.clone(),
                page_number: -5,
                page_size: -5,
                expected: Paginated {
                    items: items
                        .iter()
                        .skip((default_page_number - 1) * default_page_size)
                        .take(default_page_size)
                        .copied()
                        .collect(),
                    total,
                    pages: get_num_pages(total as usize, default_page_size) as i64,
                },
            },
            // 6. Page size is greater than the total number of elements, so all elements should be
            // returned
            TestCase {
                v: items.clone(),
                page_number: 1,
                page_size: items.len() as i32 + 1,
                expected: Paginated {
                    items: items.clone(), // All elements
                    total,
                    pages: 1,
                },
            },
            // 7. Requesting a non-existent page should return an empty list
            TestCase {
                v: items.clone(),
                page_number: 2,
                page_size: items.len() as i32,
                expected: Paginated {
                    items: vec![],
                    total,
                    pages: 1,
                },
            },
        ];

        for (i, case) in cases.into_iter().enumerate() {
            test_paginate(i + 1, case);
        }
    }
}
