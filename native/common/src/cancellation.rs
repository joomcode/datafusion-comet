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

//! Cancellation of a native plan whose Spark task has been killed. The JVM sets it from outside
//! the task thread, and the plan's operators, its blocking waits and its Tokio tasks stop at
//! their next check instead of running the plan to completion.

use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub const CANCELLED_MESSAGE: &str =
    "Comet native execution was cancelled because its Spark task was killed";

type Callback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
pub struct PlanCancellation {
    cancelled: AtomicBool,
    waiters: Mutex<Waiters>,
}

#[derive(Default)]
struct Waiters {
    wakers: Vec<Waker>,
    callbacks: Vec<Callback>,
}

impl std::fmt::Debug for PlanCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl PlanCancellation {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The cancellation of the plan `context` runs for, if it has one.
    pub fn of(context: &TaskContext) -> Option<Arc<Self>> {
        context.session_config().get_extension::<Self>()
    }

    pub fn cancel(&self) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let Waiters { wakers, callbacks } = std::mem::take(&mut *self.lock());
        wakers.into_iter().for_each(Waker::wake);
        callbacks.into_iter().for_each(|callback| callback());
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<(), DataFusionError> {
        if self.is_cancelled() {
            Err(cancelled_error())
        } else {
            Ok(())
        }
    }

    /// `Ready` once cancelled; otherwise wakes `cx` when it is.
    pub fn poll_cancelled(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_cancelled() {
            return Poll::Ready(());
        }
        let mut waiters = self.lock();
        if self.is_cancelled() {
            return Poll::Ready(());
        }
        if !waiters.wakers.iter().any(|w| w.will_wake(cx.waker())) {
            waiters.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }

    /// Runs `callback` once the plan is cancelled, at once if it already is.
    pub fn on_cancel(&self, callback: impl FnOnce() + Send + 'static) {
        {
            let mut waiters = self.lock();
            if !self.is_cancelled() {
                waiters.callbacks.push(Box::new(callback));
                return;
            }
        }
        callback()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Waiters> {
        self.waiters.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub fn cancelled_error() -> DataFusionError {
    DataFusionError::Execution(CANCELLED_MESSAGE.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::task::Wake;

    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn cancel_wakes_each_registered_waker_once_and_runs_callbacks() {
        let cancellation = PlanCancellation::new();
        let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        assert!(cancellation.poll_cancelled(&mut cx).is_pending());
        assert!(cancellation.poll_cancelled(&mut cx).is_pending());
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        cancellation.on_cancel(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        assert!(cancellation.check().is_ok());

        cancellation.cancel();
        cancellation.cancel();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cancellation.poll_cancelled(&mut cx).is_ready());
        assert!(cancellation
            .check()
            .unwrap_err()
            .to_string()
            .contains(CANCELLED_MESSAGE));

        let c = Arc::clone(&calls);
        cancellation.on_cancel(move || {
            c.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
