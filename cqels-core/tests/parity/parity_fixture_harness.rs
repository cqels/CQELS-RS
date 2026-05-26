//! Parity fixture harness (Rust side).
//!
//! Mirrors `ParityFixtureHarness.java` step for step. The harness owns script interpretation and
//! comparison orchestration; your RDF library and your operator are injected via the `RdfBackend`
//! and `FixtureOperator` traits. The only cross-language risk surface is the RDFC-1.0 canonical
//! form, which the `_meta/` self-tests verify is identical to the Java side.
//!
//! Depends only on `serde_json` for parsing. Wire `RdfBackend` to oxrdf / rdf-canon (oxrdfc), and
//! `FixtureOperator` to your real RxRust / futures::Stream windowing pipeline.

use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Terminal {
    pub kind: String, // "complete" | "error"
    pub error_type: Option<String>,
    pub message: Option<String>,
}

/// Inject your RDF library. BOTH languages must implement RDFC-1.0 identically.
pub trait RdfBackend {
    /// RDFC-1.0 canonical N-Quads (blank-node-isomorphism safe).
    fn canonicalize(&self, nquads: &str) -> String;
    /// Quad set WITHOUT blank-node relabeling; labels compared as written.
    fn exact_quad_set(&self, nquads: &str) -> HashSet<String>;
}

/// Adapt your real operator to this stepping interface (bridge to your async pipeline internally).
pub trait FixtureOperator {
    fn configure(&mut self, config: &Value);
    fn emit(&mut self, event_time_ms: Option<i64>, nquads: &str);
    fn advance_watermark(&mut self, ms: i64);
    fn advance_time(&mut self, by_ms: i64);
    fn cancel(&mut self);
    fn complete(&mut self);
    fn error(&mut self, error_type: &str, message: Option<&str>);
    /// Emissions produced since the previous drain, in order. Each is an N-Quads string.
    fn drain_emissions(&mut self) -> Vec<String>;
    /// Terminal signal once reached, else None.
    fn terminal_or_none(&self) -> Option<Terminal>;
}

#[derive(Debug)]
pub struct ParityMismatch(pub String);

pub struct Harness<'a, B: RdfBackend> {
    rdf: &'a B,
}

impl<'a, B: RdfBackend> Harness<'a, B> {
    pub fn new(rdf: &'a B) -> Self {
        Self { rdf }
    }

    /// Run one case; Ok(()) = pass, Err = mismatch. For `_meta` cases with
    /// `expect_outcome == "comparison-failure"`, see `run_self_test`.
    pub fn run_case<O: FixtureOperator>(
        &self,
        case_file: &Path,
        op: &mut O,
    ) -> Result<(), ParityMismatch> {
        let text = std::fs::read_to_string(case_file)
            .map_err(|e| ParityMismatch(format!("read {}: {e}", case_file.display())))?;
        let doc: Value =
            serde_json::from_str(&text).map_err(|e| ParityMismatch(format!("parse json: {e}")))?;

        op.configure(doc.get("config").unwrap_or(&Value::Null));
        let script = doc["script"]
            .as_array()
            .ok_or_else(|| ParityMismatch("script is not an array".into()))?;

        let mut pending: VecDeque<String> = VecDeque::new();

        for step in script {
            if let Some(action) = step.get("do").and_then(Value::as_str) {
                self.drain_into(op, &mut pending);
                match action {
                    "emit" => op.emit(
                        step.get("event_time_ms").and_then(Value::as_i64),
                        str_at(step, "quads"),
                    ),
                    "advance_watermark" => op.advance_watermark(i64_at(step, "watermark_ms")),
                    "advance_time" => op.advance_time(i64_at(step, "by_ms")),
                    "cancel" => op.cancel(),
                    "complete" => op.complete(),
                    "error" => op.error(
                        str_at(step, "error_type"),
                        step.get("message").and_then(Value::as_str),
                    ),
                    other => return Err(ParityMismatch(format!("unknown do: {other}"))),
                }
                self.drain_into(op, &mut pending);
            } else if let Some(kind) = step.get("expect").and_then(Value::as_str) {
                match kind {
                    "emission" => {
                        self.drain_into(op, &mut pending);
                        let actual = pending.pop_front().ok_or_else(|| {
                            ParityMismatch("expected an emission, none produced".into())
                        })?;
                        self.assert_emission(
                            str_at(step, "match"),
                            str_at(step, "order"),
                            str_at(step, "quads"),
                            &actual,
                        )?;
                        if let Some(expected_count) = step.get("quad_count").and_then(Value::as_i64)
                        {
                            // Spec B2 set-cardinality check: count non-empty raw N-Quads lines in
                            // the actual emission BEFORE any canonicalize/parse (which would dedup
                            // and mask a bag-leak). See schema docs for the rationale.
                            let actual_count =
                                actual.lines().filter(|l| !l.trim().is_empty()).count() as i64;
                            if actual_count != expected_count {
                                return Err(ParityMismatch(format!(
                                    "quad_count mismatch: expected {expected_count}, got \
                                     {actual_count} raw line(s) in actual emission:\n{actual}"
                                )));
                            }
                        }
                    }
                    "no_emission" => {
                        self.drain_into(op, &mut pending);
                        if let Some(e) = pending.front() {
                            return Err(ParityMismatch(format!("expected no emission, got: {e}")));
                        }
                    }
                    "no_emission_for" => {
                        op.advance_time(i64_at(step, "by_ms"));
                        self.drain_into(op, &mut pending);
                        if let Some(e) = pending.front() {
                            return Err(ParityMismatch(format!(
                                "expected no emission within window, got: {e}"
                            )));
                        }
                    }
                    "terminal" => self.assert_terminal(step, op, &mut pending)?,
                    "no_terminal" => {
                        // Spec B8 cancel and similar: assert the operator hasn't
                        // signaled a terminal at this point in the script.
                        if let Some(t) = op.terminal_or_none() {
                            return Err(ParityMismatch(format!(
                                "expected no terminal, got: kind={} error_type={:?} message={:?}",
                                t.kind, t.error_type, t.message,
                            )));
                        }
                    }
                    other => return Err(ParityMismatch(format!("unknown expect: {other}"))),
                }
            }
        }
        self.drain_into(op, &mut pending);
        if let Some(e) = pending.front() {
            return Err(ParityMismatch(format!(
                "unconsumed emission at end of script: {e}"
            )));
        }
        Ok(())
    }

    /// Wrapper honoring `expect_outcome`: inverts the result for comparator self-tests.
    pub fn run_self_test<O: FixtureOperator>(
        &self,
        case_file: &Path,
        op: &mut O,
    ) -> Result<(), String> {
        let text = std::fs::read_to_string(case_file).map_err(|e| e.to_string())?;
        let doc: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        let want_fail =
            doc.get("expect_outcome").and_then(Value::as_str) == Some("comparison-failure");
        match (self.run_case(case_file, op), want_fail) {
            (Ok(()), false) => Ok(()),
            (Err(_), true) => Ok(()),
            (Ok(()), true) => Err("self-test expected comparison-failure but case passed".into()),
            (Err(m), false) => Err(format!("self-test expected pass but failed: {}", m.0)),
        }
    }

    fn drain_into<O: FixtureOperator>(&self, op: &mut O, pending: &mut VecDeque<String>) {
        pending.extend(op.drain_emissions());
    }

    fn assert_emission(
        &self,
        mat: &str,
        order: &str,
        expected: &str,
        actual: &str,
    ) -> Result<(), ParityMismatch> {
        // canonical+ordered is permitted by the schema but not meaningfully implemented: canonical
        // comparison normalizes to a sorted dataset, so "ordered" cannot be honored. Fail loudly
        // rather than silently degrade to canonical+unordered (which would mask an ordering bug).
        if mat == "canonical" && order == "ordered" {
            return Err(ParityMismatch(
                "canonical + ordered is unsupported; use exact+ordered (positional) or \
                 canonical+unordered (dataset semantics)"
                    .into(),
            ));
        }
        let equal = if mat == "exact" {
            self.equal_exact(order, expected, actual)
        } else {
            self.rdf.canonicalize(expected) == self.rdf.canonicalize(actual)
        };
        if equal {
            Ok(())
        } else {
            Err(ParityMismatch(format!(
                "emission mismatch [match={mat}, order={order}]\n  expected:\n{expected}\n  actual:\n{actual}"
            )))
        }
    }

    fn equal_exact(&self, order: &str, expected: &str, actual: &str) -> bool {
        if order == "ordered" {
            normalize_lines(expected) == normalize_lines(actual)
        } else {
            self.rdf.exact_quad_set(expected) == self.rdf.exact_quad_set(actual)
        }
    }

    fn assert_terminal<O: FixtureOperator>(
        &self,
        step: &Value,
        op: &mut O,
        pending: &mut VecDeque<String>,
    ) -> Result<(), ParityMismatch> {
        self.drain_into(op, pending);
        if let Some(e) = pending.front() {
            return Err(ParityMismatch(format!(
                "emission produced before terminal: {e}"
            )));
        }
        let t = op
            .terminal_or_none()
            .ok_or_else(|| ParityMismatch("expected terminal, stream not terminated".into()))?;
        let kind = str_at(step, "kind");
        if kind != t.kind {
            return Err(ParityMismatch(format!(
                "terminal kind mismatch: expected {kind}, got {}",
                t.kind
            )));
        }
        if kind == "error" {
            if let Some(et) = step.get("error_type").and_then(Value::as_str) {
                if Some(et.to_string()) != t.error_type {
                    return Err(ParityMismatch(format!(
                        "error_type mismatch: expected {et}, got {:?}",
                        t.error_type
                    )));
                }
            }
            if let Some(mc) = step.get("message_contains").and_then(Value::as_str) {
                if !t.message.as_deref().unwrap_or("").contains(mc) {
                    return Err(ParityMismatch(format!(
                        "error message did not contain: {mc}"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn normalize_lines(nq: &str) -> Vec<String> {
    nq.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(String::from)
        .collect()
}

fn str_at<'v>(v: &'v Value, key: &str) -> &'v str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

fn i64_at(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}
