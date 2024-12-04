use std::collections::VecDeque;

use crate::cache_system::HasSize;

pub(crate) struct Fifo<T>
where
    T: HasSize,
{
    queue: VecDeque<T>,
    memory_size: usize,
}

impl<T> Fifo<T>
where
    T: HasSize,
{
    pub(crate) fn memory_size(&self) -> usize {
        self.memory_size
    }

    pub(crate) fn push_back(&mut self, o: T) {
        self.memory_size += o.size();
        self.queue.push_back(o);
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        match self.queue.pop_front() {
            Some(o) => {
                self.memory_size -= o.size();
                Some(o)
            }
            None => None,
        }
    }
}

impl<T> Default for Fifo<T>
where
    T: HasSize,
{
    fn default() -> Self {
        Self {
            queue: Default::default(),
            memory_size: 0,
        }
    }
}

impl<T> std::fmt::Debug for Fifo<T>
where
    T: HasSize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fifo")
            .field("memory_size", &self.memory_size)
            .finish_non_exhaustive()
    }
}
