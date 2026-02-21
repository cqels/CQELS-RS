use std::pin::Pin;

use futures::stream::{Stream, StreamExt};

use cqels_model::{BindingSet, Value};

/// Bind operator that adds new variable bindings computed from expressions.
///
/// Maps to Java's `BindOperator`.
pub struct BindOperator<F: Fn(&BindingSet) -> Option<Value> + Send + Sync> {
    variable: String,
    expression: F,
}

impl<F: Fn(&BindingSet) -> Option<Value> + Send + Sync> BindOperator<F> {
    pub fn new(variable: impl Into<String>, expression: F) -> Self {
        Self {
            variable: variable.into(),
            expression,
        }
    }

    /// Applies the bind to a stream of binding sets.
    pub fn apply(
        &self,
        stream: Pin<Box<dyn Stream<Item = BindingSet> + Send + '_>>,
    ) -> Pin<Box<dyn Stream<Item = BindingSet> + Send + '_>> {
        let var = self.variable.clone();
        let stream = stream.map(move |mut bs| {
            if let Some(value) = (self.expression)(&bs) {
                bs.insert(var.clone(), value);
            }
            bs
        });
        Box::pin(stream)
    }

    /// Evaluates the bind expression and adds the result to the binding set.
    pub fn evaluate(&self, binding: &mut BindingSet) {
        if let Some(value) = (self.expression)(binding) {
            binding.insert(self.variable.clone(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bind_evaluate() {
        let bind = BindOperator::new("doubled", |bs: &BindingSet| {
            bs.get("x")
                .and_then(|v| v.as_integer())
                .map(|i| Value::Integer(i * 2))
        });

        let mut bs = BindingSet::new(0);
        bs.insert("x", Value::Integer(5));
        bind.evaluate(&mut bs);
        assert_eq!(bs.get("doubled"), Some(&Value::Integer(10)));
    }

    #[tokio::test]
    async fn test_bind_stream() {
        use futures::StreamExt;

        let bind = BindOperator::new("label", |bs: &BindingSet| {
            bs.get("id")
                .and_then(|v| v.as_integer())
                .map(|i| Value::String(format!("item-{i}")))
        });

        let bindings: Vec<BindingSet> = (0..3)
            .map(|i| {
                let mut bs = BindingSet::new(i);
                bs.insert("id", Value::Integer(i));
                bs
            })
            .collect();

        let stream = Box::pin(futures::stream::iter(bindings));
        let results: Vec<_> = bind.apply(stream).collect().await;
        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].get("label"),
            Some(&Value::String("item-0".into()))
        );
        assert_eq!(
            results[2].get("label"),
            Some(&Value::String("item-2".into()))
        );
    }
}
