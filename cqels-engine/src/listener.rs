//! Callback-based result delivery for continuous queries.
//!
//! [`QueryResultListener`] provides a trait for receiving query results
//! via callbacks instead of consuming a stream. Use [`listener_from_fn`]
//! to create a listener from a closure.

use cqels_model::CqelsError;

/// Callback interface for receiving query results.
///
/// # Examples
///
/// ```
/// use cqels_engine::listener::{QueryResultListener, listener_from_fn};
///
/// let listener = listener_from_fn(|result: String| {
///     println!("Got: {result}");
/// });
/// listener.on_result("hello".to_string());
/// ```
pub trait QueryResultListener<R: Send + 'static>: Send + Sync {
    /// Called when a new result is available.
    fn on_result(&self, result: R);

    /// Called when an error occurs. Default implementation logs the error.
    fn on_error(&self, error: CqelsError) {
        tracing::warn!("query listener error: {error}");
    }

    /// Called when the query completes. Default is a no-op.
    fn on_complete(&self) {}
}

/// Creates a [`QueryResultListener`] from a closure.
pub fn listener_from_fn<R, F>(f: F) -> FnListener<F>
where
    R: Send + 'static,
    F: Fn(R) + Send + Sync,
{
    FnListener { f }
}

/// A listener backed by a closure. Created via [`listener_from_fn`].
pub struct FnListener<F> {
    f: F,
}

impl<R, F> QueryResultListener<R> for FnListener<F>
where
    R: Send + 'static,
    F: Fn(R) + Send + Sync,
{
    fn on_result(&self, result: R) {
        (self.f)(result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_fn_listener() {
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        let listener = listener_from_fn(move |_val: i32| {
            count_clone.fetch_add(1, Ordering::Relaxed);
        });

        listener.on_result(1);
        listener.on_result(2);
        listener.on_result(3);

        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_fn_listener_with_string() {
        let values = Arc::new(std::sync::Mutex::new(Vec::new()));
        let values_clone = values.clone();
        let listener = listener_from_fn(move |val: String| {
            values_clone.lock().unwrap().push(val);
        });

        listener.on_result("hello".to_string());
        listener.on_result("world".to_string());

        let collected = values.lock().unwrap();
        assert_eq!(&*collected, &["hello", "world"]);
    }

    #[test]
    fn test_default_on_error() {
        // Just ensure it doesn't panic
        let listener = listener_from_fn(|_: i32| {});
        listener.on_error(CqelsError::Stream {
            message: "test".to_string(),
        });
    }

    #[test]
    fn test_default_on_complete() {
        // Just ensure it doesn't panic
        let listener = listener_from_fn(|_: i32| {});
        listener.on_complete();
    }
}
