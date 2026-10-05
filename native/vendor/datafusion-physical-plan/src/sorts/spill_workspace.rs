// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! COMET PATCH: the memory an external sort spill merges its buffered batches in.
//!
//! Follows `MergeMemoryPool` from apache/datafusion#24740, but retains the sorter's
//! whole reservation rather than only `sort_spill_reservation_bytes`.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use datafusion_common::{Result, resources_err};
use datafusion_execution::memory_pool::{MemoryPool, MemoryReservation};
use parking_lot::Mutex;

/// A [`MemoryPool`] for the reservations of one in-memory spill merge.
///
/// A spill sorts and merges the batches the sorter has buffered. The sorted runs release
/// their memory as the merge's cursors, encoded rows and batch buffers acquire it. Through
/// the execution pool that is a release followed by a new request, which fails once the
/// pool can no longer grant what the sorter held, for example when Spark has lowered the
/// task's share because more tasks became active. Nothing is left to spill then, so the
/// sort fails although it holds enough memory.
///
/// This pool keeps the reservations it is built from, which remain charged to the
/// execution pool, and lets its child reservations share them. Children release into the
/// workspace, and only usage beyond it grows the first parent reservation, under the
/// execution pool's limits. [`Self::close`] ends the retention, and [`Self::keep_at_most`]
/// limits it.
///
/// A workspace built with [`Self::for_spill`] grows its first parent with the infallible
/// `grow` instead, for a merge that writes a spill: everything its children reserve
/// already exists, and the spill it is writing releases all of it.
#[derive(Debug)]
pub(super) struct SpillWorkspace {
    state: Mutex<State>,
    fallible: bool,
}

#[derive(Debug)]
struct State {
    /// Charged to the execution pool. Only the first one grows.
    parents: Vec<MemoryReservation>,
    /// Total size of the child reservations and loans.
    used: usize,
    /// How many unused bytes stay reserved in the parents.
    keep: usize,
}

impl State {
    fn reserved(&self) -> usize {
        self.parents.iter().map(MemoryReservation::size).sum()
    }

    fn cover(&mut self, used: usize, fallible: bool) -> Result<()> {
        let reserved = self.reserved();
        if used > reserved {
            if fallible {
                self.parents[0].try_grow(used - reserved)?;
            } else {
                self.parents[0].grow(used - reserved);
            }
        }
        self.used = used;
        Ok(())
    }

    fn trim(&mut self) {
        let mut excess = (self.reserved() - self.used).saturating_sub(self.keep);
        for parent in self.parents.iter().rev() {
            let shrink = excess.min(parent.size());
            parent.shrink(shrink);
            excess -= shrink;
        }
    }
}

/// Bytes lent from a [`SpillWorkspace`] without growing its parents. Dropping it returns
/// them.
#[derive(Debug)]
pub(super) struct WorkspaceLoan {
    workspace: Arc<SpillWorkspace>,
    size: usize,
}

impl Drop for WorkspaceLoan {
    fn drop(&mut self) {
        self.workspace.release(self.size);
    }
}

impl SpillWorkspace {
    /// Takes over `parents`. The first one is grown if the children need more.
    pub(super) fn new(parents: Vec<MemoryReservation>) -> Arc<Self> {
        Self::with_growth(parents, true)
    }

    /// [`Self::new`] for a merge that writes a spill, which never refuses its children.
    pub(super) fn for_spill(parents: Vec<MemoryReservation>) -> Arc<Self> {
        Self::with_growth(parents, false)
    }

    fn with_growth(parents: Vec<MemoryReservation>, fallible: bool) -> Arc<Self> {
        assert!(!parents.is_empty());
        Arc::new(Self {
            state: Mutex::new(State {
                parents,
                used: 0,
                keep: usize::MAX,
            }),
            fallible,
        })
    }

    /// Lends up to `size` bytes of unused workspace.
    pub(super) fn borrow(self: &Arc<Self>, size: usize) -> WorkspaceLoan {
        let mut state = self.state.lock();
        let size = size.min(state.reserved() - state.used);
        state.used += size;
        WorkspaceLoan {
            workspace: Arc::clone(self),
            size,
        }
    }

    /// Whether the parents could cover `extra` bytes more than the children and loans use
    /// now. Asks the execution pool for any part not already reserved, and gives it back.
    pub(super) fn can_grow(&self, extra: usize) -> bool {
        let state = self.state.lock();
        let Some(total) = state.used.checked_add(extra) else {
            return false;
        };
        let missing = total.saturating_sub(state.reserved());
        if missing == 0 {
            return true;
        }
        if state.parents[0].try_grow(missing).is_err() {
            return false;
        }
        state.parents[0].shrink(missing);
        true
    }

    /// Returns unused workspace to the execution pool, and every later release too.
    pub(super) fn close(&self) {
        self.keep_at_most(0);
    }

    /// Returns unused workspace beyond `bytes` to the execution pool, now and on every
    /// later release.
    pub(super) fn keep_at_most(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.keep = bytes;
        state.trim();
    }

    fn release(&self, size: usize) {
        let mut state = self.state.lock();
        state.used = state
            .used
            .checked_sub(size)
            .expect("spill workspace underflow");
        state.trim();
    }
}

impl Display for SpillWorkspace {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SpillWorkspace")
    }
}

impl MemoryPool for SpillWorkspace {
    fn name(&self) -> &str {
        "SpillWorkspace"
    }

    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        let used = state.used.saturating_add(additional);
        state
            .cover(used, false)
            .expect("an infallible grow cannot fail");
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.release(shrink);
    }

    fn try_grow(
        &self,
        _reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<()> {
        let mut state = self.state.lock();
        let Some(used) = state.used.checked_add(additional) else {
            return resources_err!("Sort spill workspace overflow");
        };
        state.cover(used, self.fallible)
    }

    fn reserved(&self) -> usize {
        self.state.lock().reserved()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};

    fn setup(
        limit: usize,
        held: usize,
    ) -> (Arc<dyn MemoryPool>, Arc<SpillWorkspace>, MemoryReservation) {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let sorter = MemoryConsumer::new("sorter").register(&parent);
        sorter.try_grow(held).unwrap();
        let workspace = SpillWorkspace::new(vec![sorter]);
        let pool = Arc::clone(&workspace) as Arc<dyn MemoryPool>;
        let child = MemoryConsumer::new("child").register(&pool);
        (parent, workspace, child)
    }

    #[test]
    fn children_reuse_released_bytes_without_the_parent_pool() {
        let (parent, workspace, runs) = setup(100, 80);
        runs.grow(80);

        // The parent pool now has only 20 bytes free, and another consumer takes them.
        let contender = MemoryConsumer::new("contender").register(&parent);
        contender.try_grow(20).unwrap();

        runs.shrink(50);
        let cursors = runs.new_empty();
        cursors.try_grow(50).unwrap();
        assert!(cursors.try_grow(1).is_err());
        assert_eq!(parent.reserved(), 100);

        drop(cursors);
        runs.free();
        assert_eq!(parent.reserved(), 100);
        workspace.close();
        assert_eq!(parent.reserved(), 20);
    }

    #[test]
    fn growth_past_the_workspace_uses_the_first_parent() {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let sorter = MemoryConsumer::new("sorter").register(&parent);
        sorter.try_grow(30).unwrap();
        let merge = MemoryConsumer::new("merge").register(&parent);
        merge.try_grow(10).unwrap();
        let workspace = SpillWorkspace::new(vec![sorter.split(30), merge.split(10)]);
        let pool = Arc::clone(&workspace) as Arc<dyn MemoryPool>;
        let child = MemoryConsumer::new("child").register(&pool);

        child.try_grow(90).unwrap();
        assert_eq!(parent.reserved(), 90);
        assert!(child.try_grow(11).is_err());
        child.grow(20);
        assert_eq!(parent.reserved(), 110);

        workspace.close();
        child.shrink(105);
        assert_eq!(parent.reserved(), 5);
        drop(child);
        assert_eq!(parent.reserved(), 0);
        drop(workspace);
        drop(pool);
        assert_eq!(parent.reserved(), 0);
    }

    #[test]
    fn a_spill_workspace_grows_its_first_parent_past_the_pool() {
        let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let sorter = MemoryConsumer::new("sorter").register(&parent);
        sorter.try_grow(60).unwrap();
        let contender = MemoryConsumer::new("contender").register(&parent);
        contender.try_grow(40).unwrap();
        let workspace = SpillWorkspace::for_spill(vec![sorter]);
        let pool = Arc::clone(&workspace) as Arc<dyn MemoryPool>;
        let child = MemoryConsumer::new("child").register(&pool);

        child.try_grow(60).unwrap();
        child.try_grow(15).unwrap();
        assert_eq!(parent.reserved(), 115);
        assert!(contender.try_grow(1).is_err());

        workspace.close();
        drop(child);
        assert_eq!(parent.reserved(), 40);
    }

    #[test]
    fn can_grow_counts_unused_workspace_and_leaves_reservations_unchanged() {
        let (parent, workspace, child) = setup(100, 40);
        child.grow(30);
        assert!(workspace.can_grow(10));
        assert_eq!(parent.reserved(), 40);
        assert!(workspace.can_grow(70));
        assert_eq!(parent.reserved(), 40);
        assert!(!workspace.can_grow(71));
        assert!(!workspace.can_grow(usize::MAX));
        assert_eq!(parent.reserved(), 40);
        assert_eq!(child.size(), 30);
    }

    #[test]
    fn keep_at_most_returns_only_unused_bytes_beyond_the_limit() {
        let (parent, workspace, child) = setup(100, 60);
        child.grow(30);
        workspace.keep_at_most(20);
        assert_eq!(parent.reserved(), 50);
        child.shrink(25);
        assert_eq!(parent.reserved(), 25);
        child.try_grow(15).unwrap();
        assert_eq!(parent.reserved(), 25);
        child.try_grow(10).unwrap();
        assert_eq!(parent.reserved(), 30);
        drop(child);
        assert_eq!(parent.reserved(), 20);
        drop(workspace);
        assert_eq!(parent.reserved(), 0);
    }

    #[test]
    fn loans_take_only_unused_workspace() {
        let (parent, workspace, child) = setup(100, 50);
        child.grow(30);
        let loan = workspace.borrow(40);
        assert_eq!(loan.size, 20);
        assert!(child.try_grow(51).is_err());
        child.try_grow(50).unwrap();
        assert_eq!(parent.reserved(), 100);

        workspace.close();
        drop(loan);
        assert_eq!(parent.reserved(), 80);
        drop(child);
        assert_eq!(parent.reserved(), 0);
    }
}
