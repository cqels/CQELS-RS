//! Parity fixture harness (Rust side).
//!
//! Mirrors `ParityFixtureHarness.java` step for step. The harness owns script interpretation and
//! comparison orchestration; your RDF library and your operator are injected via the `RdfBackend`
//! and `FixtureOperator` traits. The only cross-language risk surface is the RDFC-1.0 canonical
//! form, which the `_meta/` self-tests verify is identical to the Java side.
//!
//! Depends only on `serde_json` for parsing. Wire `RdfBackend` to oxrdf / rdf-canon (oxrdfc), and
//! `FixtureOperator` to your real RxRust / futures::Stream windowing pipeline.

use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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

/// A single SPARQL solution mapping: variable name → canonical N-Triples term string.
///
/// Keys are variable names without a leading `?`. Values are normalized to
/// the engine's canonical N-Triples form before being placed here, so equality
/// of `Solution` is byte-equality of canonical term strings. Absence of a
/// key means the variable is UNBOUND in this solution.
#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
pub type Solution = BTreeMap<String, String>;

/// Adapt a SPARQL binding-set operator (e.g. `MinusOperator`) to the harness.
///
/// Parallel to [`FixtureOperator`] but for solution payloads — used by the
/// `do_set_exclusions` / `do_emit_solution` / `expect_solutions_multiset`
/// step family. Cases that drive a [`SolutionFixtureOperator`] should declare
/// `"payload_kind": "solutions"`.
#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
pub trait SolutionFixtureOperator {
    fn configure(&mut self, config: &Value);
    /// Sets the right-hand side of an anti-join-style operator (MinusOperator).
    /// Called at most once per case, before any `emit_solution`. Empty list
    /// is the M4 identity case (Ω₂ = ∅).
    fn set_exclusions(&mut self, solutions: &[Solution]);
    fn emit_solution(&mut self, solution: &Solution);
    fn cancel(&mut self);
    fn complete(&mut self);
    fn error(&mut self, error_type: &str, message: Option<&str>);
    /// Solutions produced since the previous drain, in the operator's emission order.
    /// The harness's multiset comparator ignores ordering between solutions.
    fn drain_solutions(&mut self) -> Vec<Solution>;
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
        let doc: Value = serde_json::from_str(&text)
            .map_err(|e| ParityMismatch(format!("parse json: {e}")))?;

        validate_payload_kind(&doc, "quads")?;

        op.configure(doc.get("config").unwrap_or(&Value::Null));
        let script = doc["script"]
            .as_array()
            .ok_or_else(|| ParityMismatch("script is not an array".into()))?;

        let mut pending: VecDeque<String> = VecDeque::new();

        for step in script {
            if let Some(action) = step.get("do").and_then(Value::as_str) {
                self.drain_into(op, &mut pending);
                match action {
                    "emit" => op.emit(step.get("event_time_ms").and_then(Value::as_i64), str_at(step, "quads")),
                    "advance_watermark" => op.advance_watermark(i64_at(step, "watermark_ms")),
                    "advance_time" => op.advance_time(i64_at(step, "by_ms")),
                    "cancel" => op.cancel(),
                    "complete" => op.complete(),
                    "error" => op.error(str_at(step, "error_type"), step.get("message").and_then(Value::as_str)),
                    other => return Err(ParityMismatch(format!("unknown do: {other}"))),
                }
                self.drain_into(op, &mut pending);
            } else if let Some(kind) = step.get("expect").and_then(Value::as_str) {
                match kind {
                    "emission" => {
                        self.drain_into(op, &mut pending);
                        let actual = pending
                            .pop_front()
                            .ok_or_else(|| ParityMismatch("expected an emission, none produced".into()))?;
                        self.assert_emission(str_at(step, "match"), str_at(step, "order"), str_at(step, "quads"), &actual)?;
                        if let Some(expected_count) = step.get("quad_count").and_then(Value::as_i64) {
                            // Spec B2 set-cardinality check: count non-empty raw N-Quads lines in
                            // the actual emission BEFORE any canonicalize/parse (which would dedup
                            // and mask a bag-leak). See schema docs for the rationale.
                            let actual_count = actual
                                .lines()
                                .filter(|l| !l.trim().is_empty())
                                .count() as i64;
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
                            return Err(ParityMismatch(format!("expected no emission within window, got: {e}")));
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
            return Err(ParityMismatch(format!("unconsumed emission at end of script: {e}")));
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
        let want_fail = doc.get("expect_outcome").and_then(Value::as_str) == Some("comparison-failure");
        match (self.run_case(case_file, op), want_fail) {
            (Ok(()), false) => Ok(()),
            (Err(_), true) => Ok(()),
            (Ok(()), true) => Err("self-test expected comparison-failure but case passed".into()),
            (Err(m), false) => Err(format!("self-test expected pass but failed: {}", m.0)),
        }
    }

    /// Run one case driving a [`SolutionFixtureOperator`].
    ///
    /// Parallel to [`Self::run_case`] but for SPARQL binding-set operators.
    /// The case must declare `"payload_kind": "solutions"` (or have no
    /// `payload_kind` field and use only solution-shaped steps); a mixed-kind
    /// script is rejected at load time.
    #[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
    pub fn run_solution_case<O: SolutionFixtureOperator>(
        &self,
        case_file: &Path,
        op: &mut O,
    ) -> Result<(), ParityMismatch> {
        let text = std::fs::read_to_string(case_file)
            .map_err(|e| ParityMismatch(format!("read {}: {e}", case_file.display())))?;
        let doc: Value = serde_json::from_str(&text)
            .map_err(|e| ParityMismatch(format!("parse json: {e}")))?;

        validate_payload_kind(&doc, "solutions")?;

        op.configure(doc.get("config").unwrap_or(&Value::Null));
        let script = doc["script"]
            .as_array()
            .ok_or_else(|| ParityMismatch("script is not an array".into()))?;

        let mut pending: Vec<Solution> = Vec::new();
        let mut exclusions_set = false;
        let mut emitted_solution = false;

        for step in script {
            if let Some(action) = step.get("do").and_then(Value::as_str) {
                pending.extend(op.drain_solutions());
                match action {
                    "set_exclusions" => {
                        if exclusions_set {
                            return Err(ParityMismatch(
                                "do_set_exclusions appears more than once".into(),
                            ));
                        }
                        if emitted_solution {
                            return Err(ParityMismatch(
                                "do_set_exclusions must precede any do_emit_solution".into(),
                            ));
                        }
                        let solutions = parse_solutions_array(step, "solutions")?;
                        op.set_exclusions(&solutions);
                        exclusions_set = true;
                    }
                    "emit_solution" => {
                        let solution = parse_solution_object(step, "solution")?;
                        op.emit_solution(&solution);
                        emitted_solution = true;
                    }
                    "cancel" => op.cancel(),
                    "complete" => op.complete(),
                    "error" => op.error(
                        str_at(step, "error_type"),
                        step.get("message").and_then(Value::as_str),
                    ),
                    other => {
                        return Err(ParityMismatch(format!(
                            "step '{other}' is not valid in a solutions-payload script"
                        )));
                    }
                }
                pending.extend(op.drain_solutions());
            } else if let Some(kind) = step.get("expect").and_then(Value::as_str) {
                match kind {
                    "solutions_multiset" => {
                        pending.extend(op.drain_solutions());
                        let expected = parse_solutions_array(step, "solutions")?;
                        if let Some(declared) =
                            step.get("expected_vars").and_then(Value::as_array)
                        {
                            warn_unused_expected_vars(declared, &expected);
                        }
                        compare_solution_multisets(&expected, &pending)?;
                        pending.clear();
                    }
                    "no_solution" => {
                        pending.extend(op.drain_solutions());
                        if !pending.is_empty() {
                            return Err(ParityMismatch(format!(
                                "expected no solution, got {} pending: {}",
                                pending.len(),
                                solutions_summary(&pending),
                            )));
                        }
                    }
                    "terminal" => {
                        pending.extend(op.drain_solutions());
                        if !pending.is_empty() {
                            return Err(ParityMismatch(format!(
                                "solution(s) produced before terminal: {}",
                                solutions_summary(&pending),
                            )));
                        }
                        let t = op.terminal_or_none().ok_or_else(|| {
                            ParityMismatch("expected terminal, stream not terminated".into())
                        })?;
                        let want_kind = str_at(step, "kind");
                        if want_kind != t.kind {
                            return Err(ParityMismatch(format!(
                                "terminal kind mismatch: expected {want_kind}, got {}",
                                t.kind
                            )));
                        }
                        if want_kind == "error" {
                            if let Some(et) = step.get("error_type").and_then(Value::as_str) {
                                if Some(et.to_string()) != t.error_type {
                                    return Err(ParityMismatch(format!(
                                        "error_type mismatch: expected {et}, got {:?}",
                                        t.error_type
                                    )));
                                }
                            }
                            if let Some(mc) =
                                step.get("message_contains").and_then(Value::as_str)
                            {
                                if !t.message.as_deref().unwrap_or("").contains(mc) {
                                    return Err(ParityMismatch(format!(
                                        "error message did not contain: {mc}"
                                    )));
                                }
                            }
                        }
                    }
                    "no_terminal" => {
                        if let Some(t) = op.terminal_or_none() {
                            return Err(ParityMismatch(format!(
                                "expected no terminal, got: kind={} error_type={:?} message={:?}",
                                t.kind, t.error_type, t.message,
                            )));
                        }
                    }
                    other => {
                        return Err(ParityMismatch(format!(
                            "expect '{other}' is not valid in a solutions-payload script"
                        )));
                    }
                }
            }
        }
        pending.extend(op.drain_solutions());
        if !pending.is_empty() {
            return Err(ParityMismatch(format!(
                "unconsumed solution(s) at end of script: {}",
                solutions_summary(&pending),
            )));
        }
        Ok(())
    }

    fn drain_into<O: FixtureOperator>(&self, op: &mut O, pending: &mut VecDeque<String>) {
        pending.extend(op.drain_emissions());
    }

    fn assert_emission(&self, mat: &str, order: &str, expected: &str, actual: &str) -> Result<(), ParityMismatch> {
        // canonical+ordered is permitted by the schema but not meaningfully implemented: canonical
        // comparison normalizes to a sorted dataset, so "ordered" cannot be honored. Fail loudly
        // rather than silently degrade to canonical+unordered (which would mask an ordering bug).
        if mat == "canonical" && order == "ordered" {
            return Err(ParityMismatch(
                "canonical + ordered is unsupported; use exact+ordered (positional) or \
                 canonical+unordered (dataset semantics)".into(),
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
            return Err(ParityMismatch(format!("emission produced before terminal: {e}")));
        }
        let t = op
            .terminal_or_none()
            .ok_or_else(|| ParityMismatch("expected terminal, stream not terminated".into()))?;
        let kind = str_at(step, "kind");
        if kind != t.kind {
            return Err(ParityMismatch(format!("terminal kind mismatch: expected {kind}, got {}", t.kind)));
        }
        if kind == "error" {
            if let Some(et) = step.get("error_type").and_then(Value::as_str) {
                if Some(et.to_string()) != t.error_type {
                    return Err(ParityMismatch(format!("error_type mismatch: expected {et}, got {:?}", t.error_type)));
                }
            }
            if let Some(mc) = step.get("message_contains").and_then(Value::as_str) {
                if !t.message.as_deref().unwrap_or("").contains(mc) {
                    return Err(ParityMismatch(format!("error message did not contain: {mc}")));
                }
            }
        }
        Ok(())
    }
}

fn normalize_lines(nq: &str) -> Vec<String> {
    nq.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from).collect()
}

fn str_at<'v>(v: &'v Value, key: &str) -> &'v str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

fn i64_at(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

fn validate_payload_kind(doc: &Value, expected: &str) -> Result<(), ParityMismatch> {
    if let Some(kind) = doc.get("payload_kind").and_then(Value::as_str) {
        if kind != expected {
            return Err(ParityMismatch(format!(
                "payload_kind '{kind}' on the case does not match this run method (expected '{expected}'). \
                 Use run_case for 'quads' and run_solution_case for 'solutions'."
            )));
        }
    }
    Ok(())
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn parse_solution_object(step: &Value, key: &str) -> Result<Solution, ParityMismatch> {
    let obj = step
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| ParityMismatch(format!("step missing '{key}' object")))?;
    object_to_solution(obj, key)
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn parse_solutions_array(step: &Value, key: &str) -> Result<Vec<Solution>, ParityMismatch> {
    let arr = step
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| ParityMismatch(format!("step missing '{key}' array")))?;
    arr.iter()
        .enumerate()
        .map(|(i, sv)| {
            sv.as_object()
                .ok_or_else(|| {
                    ParityMismatch(format!("'{key}'[{i}] is not a JSON object"))
                })
                .and_then(|obj| object_to_solution(obj, &format!("{key}[{i}]")))
        })
        .collect()
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn object_to_solution(obj: &Map<String, Value>, where_: &str) -> Result<Solution, ParityMismatch> {
    let mut solution = Solution::new();
    for (var, term) in obj {
        let term_str = term.as_str().ok_or_else(|| {
            ParityMismatch(format!(
                "{where_}.'{var}' is not a string (terms are N-Triples-style strings)"
            ))
        })?;
        solution.insert(var.clone(), term_str.to_string());
    }
    Ok(solution)
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn solution_canonical_string(solution: &Solution) -> String {
    // BTreeMap iteration is sorted by key, so this is canonical by construction.
    let parts: Vec<String> = solution
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    format!("{{{}}}", parts.join(", "))
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn solutions_summary(solutions: &[Solution]) -> String {
    let mut bag: Vec<String> = solutions.iter().map(solution_canonical_string).collect();
    bag.sort();
    bag.join(" | ")
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn compare_solution_multisets(
    expected: &[Solution],
    actual: &[Solution],
) -> Result<(), ParityMismatch> {
    let mut want: HashMap<String, i64> = HashMap::new();
    for s in expected {
        *want.entry(solution_canonical_string(s)).or_insert(0) += 1;
    }
    let mut got: HashMap<String, i64> = HashMap::new();
    for s in actual {
        *got.entry(solution_canonical_string(s)).or_insert(0) += 1;
    }
    if want == got {
        return Ok(());
    }
    let mut diff = String::new();
    let mut keys: HashSet<&String> = HashSet::new();
    keys.extend(want.keys());
    keys.extend(got.keys());
    let mut sorted_keys: Vec<&String> = keys.into_iter().collect();
    sorted_keys.sort();
    for k in sorted_keys {
        let w = want.get(k).copied().unwrap_or(0);
        let g = got.get(k).copied().unwrap_or(0);
        if w != g {
            diff.push_str(&format!("  {k}: expected ×{w}, got ×{g}\n"));
        }
    }
    Err(ParityMismatch(format!(
        "solution multiset mismatch:\n{diff}"
    )))
}

#[allow(dead_code)] // used by future MinusOperator + other SPARQL-op adapters
fn warn_unused_expected_vars(declared: &[Value], solutions: &[Solution]) {
    let mut bound: HashSet<&str> = HashSet::new();
    for s in solutions {
        for k in s.keys() {
            bound.insert(k.as_str());
        }
    }
    for v in declared {
        if let Some(name) = v.as_str() {
            if !bound.contains(name) {
                eprintln!(
                    "warning: expected_vars lists '{name}' but no solution binds it; \
                     possible fixture authoring mistake"
                );
            }
        }
    }
}
