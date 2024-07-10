use std::sync::Arc;

use crate::cache_system::{hook::Hook, interfaces::DynError};

/// Chains multiple [hooks](Hook).
///
/// For [fetched](Hook::fetched) this will combine errors.
pub(crate) struct HookChain<K> {
    hooks: Box<[Arc<dyn Hook<K = K>>]>,
}

impl<K> HookChain<K> {
    pub(crate) fn new(hooks: impl IntoIterator<Item = Arc<dyn Hook<K = K>>>) -> Self {
        Self {
            hooks: hooks.into_iter().collect(),
        }
    }
}

impl<K> std::fmt::Debug for HookChain<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookChain")
            .field("hooks", &self.hooks)
            .finish()
    }
}

impl<K> Hook for HookChain<K> {
    type K = K;

    fn insert(&self, gen: u64, k: &Self::K) {
        for hook in self.hooks.iter() {
            hook.insert(gen, k);
        }
    }

    fn fetched(
        &self,
        gen: u64,
        k: &Self::K,
        res: &Result<usize, DynError>,
    ) -> Result<(), DynError> {
        let mut e = None;

        for hook in self.hooks.iter() {
            let res = match (&e, res) {
                (Some(e), _) => Err(Arc::clone(e)),
                (None, res) => res.clone(),
            };

            match hook.fetched(gen, k, &res) {
                Ok(()) => (),
                Err(new_e) => match (e, res.as_ref()) {
                    (Some(existing_e), _) => {
                        e = Some(Arc::new(ErrorChain {
                            error: new_e,
                            cause: existing_e,
                        }));
                    }
                    (None, Err(existing_e)) => {
                        e = Some(Arc::new(ErrorChain {
                            error: new_e,
                            cause: Arc::clone(existing_e),
                        }));
                    }
                    (None, Ok(_)) => {
                        e = Some(new_e);
                    }
                },
            }
        }

        match e {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn evict(&self, gen: u64, k: &Self::K, res: &Option<Result<usize, ()>>) {
        for hook in self.hooks.iter() {
            hook.evict(gen, k, res);
        }
    }
}

#[derive(Debug)]
struct ErrorChain {
    error: DynError,
    cause: DynError,
}

impl std::fmt::Display for ErrorChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error, self.cause)
    }
}

impl std::error::Error for ErrorChain {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[cfg(test)]
mod tests {
    use crate::cache_system::{
        hook::test_utils::{TestHook, TestHookRecord},
        test_utils::str_err,
    };

    use super::*;

    #[test]
    fn test_empty_hook_chain() {
        let chain = HookChain::<()>::new([]);

        chain.insert(1, &());
        chain.fetched(2, &(), &Ok(1)).unwrap();
        chain.fetched(3, &(), &Err(str_err("foo"))).unwrap();
        chain.evict(4, &(), &None);
        chain.evict(5, &(), &Some(Ok(1)));
        chain.evict(6, &(), &Some(Err(())));
    }

    #[test]
    fn test_hook_chain() {
        let h1 = Arc::new(TestHook::<u8>::default());
        let h2 = Arc::new(TestHook::<u8>::default());
        let chain = HookChain::<u8>::new([Arc::clone(&h1) as _, Arc::clone(&h2) as _]);

        chain.insert(1, &1);

        chain.fetched(2, &2, &Ok(1000)).unwrap();
        chain.fetched(3, &3, &Err(str_err("e1"))).unwrap();

        h1.mock_next_fetch(Err("e2"));
        assert_eq!(
            chain.fetched(4, &4, &Ok(2000)).unwrap_err().to_string(),
            "e2",
        );

        h1.mock_next_fetch(Err("e3"));
        assert_eq!(
            chain
                .fetched(5, &5, &Err(str_err("e4")))
                .unwrap_err()
                .to_string(),
            "e3: e4",
        );

        h2.mock_next_fetch(Err("e5"));
        assert_eq!(
            chain.fetched(6, &6, &Ok(3000)).unwrap_err().to_string(),
            "e5",
        );

        h2.mock_next_fetch(Err("e6"));
        assert_eq!(
            chain
                .fetched(7, &7, &Err(str_err("e7")))
                .unwrap_err()
                .to_string(),
            "e6: e7",
        );

        h1.mock_next_fetch(Err("e9"));
        h2.mock_next_fetch(Err("e10"));
        assert_eq!(
            chain.fetched(8, &8, &Ok(4000)).unwrap_err().to_string(),
            "e10: e9",
        );

        h1.mock_next_fetch(Err("e11"));
        h2.mock_next_fetch(Err("e12"));
        assert_eq!(
            chain
                .fetched(9, &9, &Err(str_err("e13")))
                .unwrap_err()
                .to_string(),
            "e12: e11: e13",
        );

        chain.evict(10, &10, &None);
        chain.evict(11, &11, &Some(Ok(5000)));
        chain.evict(12, &12, &Some(Err(())));

        assert_eq!(
            h1.records(),
            vec![
                TestHookRecord::Insert(1, 1),
                TestHookRecord::Fetched(2, 2, Ok(1000)),
                TestHookRecord::Fetched(3, 3, Err("e1".to_owned())),
                TestHookRecord::Fetched(4, 4, Ok(2000)),
                TestHookRecord::Fetched(5, 5, Err("e4".to_owned())),
                TestHookRecord::Fetched(6, 6, Ok(3000)),
                TestHookRecord::Fetched(7, 7, Err("e7".to_owned())),
                TestHookRecord::Fetched(8, 8, Ok(4000)),
                TestHookRecord::Fetched(9, 9, Err("e13".to_owned())),
                TestHookRecord::Evict(10, 10, None),
                TestHookRecord::Evict(11, 11, Some(Ok(5000))),
                TestHookRecord::Evict(12, 12, Some(Err(()))),
            ],
        );
        assert_eq!(
            h2.records(),
            vec![
                TestHookRecord::Insert(1, 1),
                TestHookRecord::Fetched(2, 2, Ok(1000)),
                TestHookRecord::Fetched(3, 3, Err("e1".to_owned())),
                TestHookRecord::Fetched(4, 4, Err("e2".to_owned())),
                TestHookRecord::Fetched(5, 5, Err("e3: e4".to_owned())),
                TestHookRecord::Fetched(6, 6, Ok(3000)),
                TestHookRecord::Fetched(7, 7, Err("e7".to_owned())),
                TestHookRecord::Fetched(8, 8, Err("e9".to_owned())),
                TestHookRecord::Fetched(9, 9, Err("e11: e13".to_owned())),
                TestHookRecord::Evict(10, 10, None),
                TestHookRecord::Evict(11, 11, Some(Ok(5000))),
                TestHookRecord::Evict(12, 12, Some(Err(()))),
            ],
        );
    }
}
