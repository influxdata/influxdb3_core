use std::{collections::VecDeque, sync::Mutex};

use crate::cache_system::{hook::Hook, interfaces::DynError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TestHookRecord<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    Insert(u64, K),
    Fetched(u64, K, Result<usize, String>),
    Evict(u64, K, Option<Result<usize, ()>>),
}

#[derive(Debug, Default)]
pub(crate) struct TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    state: Mutex<TestHookState<K>>,
}

#[derive(Debug, Default)]
struct TestHookState<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    records: Vec<TestHookRecord<K>>,
    fetch_results: VecDeque<Result<(), &'static str>>,
}

impl<K> TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    pub(crate) fn records(&self) -> Vec<TestHookRecord<K>> {
        self.state.lock().unwrap().records.clone()
    }

    pub(crate) fn mock_next_fetch(&self, res: Result<(), &'static str>) {
        self.state.lock().unwrap().fetch_results.push_back(res);
    }
}

impl<K> Hook for TestHook<K>
where
    K: Clone + Eq + std::fmt::Debug + Send,
{
    type K = K;

    fn insert(&self, gen: u64, k: &Self::K) {
        self.state
            .lock()
            .unwrap()
            .records
            .push(TestHookRecord::Insert(gen, k.clone()))
    }

    fn fetched(
        &self,
        gen: u64,
        k: &Self::K,
        res: &Result<usize, DynError>,
    ) -> Result<(), DynError> {
        let mut state = self.state.lock().unwrap();
        state.records.push(TestHookRecord::Fetched(
            gen,
            k.clone(),
            res.as_ref().map(|s| *s).map_err(|e| e.to_string()),
        ));
        state
            .fetch_results
            .pop_front()
            .unwrap_or(Ok(()))
            .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(e).into())
    }

    fn evict(&self, gen: u64, k: &Self::K, res: &Option<Result<usize, ()>>) {
        self.state
            .lock()
            .unwrap()
            .records
            .push(TestHookRecord::Evict(gen, k.clone(), res.as_ref().cloned()));
    }
}
