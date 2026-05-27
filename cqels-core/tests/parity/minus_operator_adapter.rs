//! Bridges `cqels_core::operator::minus::MinusOperator` to the parity fixture harness's
//! [`SolutionFixtureOperator`] interface.
//!
//! Lifecycle (lazy pipeline): exclusions accumulate via `set_exclusions`; on the first
//! `emit_solution` (or on `complete`), the adapter constructs a `MinusOperator` plus a
//! `futures::channel::mpsc::unbounded` channel and pins the operator with `Box::leak` so
//! [`MinusOperator::apply`]'s `&'a self` lifetime can be `'static`. The leaked operator
//! lives only for the duration of one fixture run; harness tests are short-lived.
//!
//! Term I/O: each fixture solution carries variable→N-Triples-style strings. Parsing wraps
//! each term in a stub triple `<urn:s> <urn:p> {term} .` and runs `oxttl::NTriplesParser`,
//! producing an `oxrdf::Term`. The existing `From<oxrdf::Term>` for `cqels_model::Term`
//! folds it into the model type; we wrap in `Value::Term` to retain the canonical form.
//!
//! `xsd:string`-typed literals are emitted as bare `"foo"` rather than
//! `"foo"^^<http://...XMLSchema#string>` so the round-trip lines up with the RDF 1.1
//! N-Triples canonical form used by the fixture authors.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use cqels_core::operator::minus::MinusOperator;
use cqels_model::{BindingSet, Term, Value};
use futures::channel::mpsc;
use futures::executor::block_on_stream;
use futures::stream::Stream;
use futures::task::noop_waker_ref;
use futures::StreamExt;
use oxttl::NTriplesParser;
use serde_json::Value as JsonValue;

use crate::parity_fixture_harness::{Solution, SolutionFixtureOperator, Terminal};

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

type SolutionStream = Pin<Box<dyn Stream<Item = BindingSet> + Send>>;

pub(crate) struct MinusOperatorAdapter {
    exclusions: Vec<BindingSet>,
    sender: Option<mpsc::UnboundedSender<BindingSet>>,
    output: Option<SolutionStream>,
    pending: VecDeque<Solution>,
    terminal: Option<Terminal>,
    cancelled: bool,
}

impl MinusOperatorAdapter {
    pub(crate) fn new() -> Self {
        Self {
            exclusions: Vec::new(),
            sender: None,
            output: None,
            pending: VecDeque::new(),
            terminal: None,
            cancelled: false,
        }
    }

    /// Construct the pipeline on demand. Empty-exclusion (spec M4) fixtures
    /// share this code path so the first `emit_solution` (or `complete`)
    /// flips an idle adapter into a running one.
    fn ensure_pipeline(&mut self) {
        if self.sender.is_some() || self.cancelled || self.terminal.is_some() {
            return;
        }
        let (tx, rx) = mpsc::unbounded::<BindingSet>();
        // `MinusOperator::apply` borrows `&'a self`; leak a Box so `'a` is `'static`.
        // Test-only — one tiny leak per fixture run.
        let op: &'static MinusOperator =
            Box::leak(Box::new(MinusOperator::excluding(self.exclusions.clone())));
        let output = op.apply(Box::pin(rx));
        self.sender = Some(tx);
        self.output = Some(output);
    }

    /// Non-blocking pull from the output stream into `pending`. Stops at the
    /// first `Pending` so the caller can resume later.
    fn drain_ready(&mut self) {
        let Some(output) = self.output.as_mut() else {
            return;
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        loop {
            match output.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(bs)) => {
                    self.pending.push_back(bindingset_to_solution(&bs));
                }
                Poll::Ready(None) => {
                    self.output = None;
                    return;
                }
                Poll::Pending => return,
            }
        }
    }

    /// Synchronously drain everything after `complete` has closed the input.
    fn drain_to_end(&mut self) {
        let Some(output) = self.output.take() else {
            return;
        };
        for bs in block_on_stream(output) {
            self.pending.push_back(bindingset_to_solution(&bs));
        }
    }
}

impl SolutionFixtureOperator for MinusOperatorAdapter {
    fn configure(&mut self, _config: &JsonValue) {
        // MinusOperator carries no config (spec D3: Java's Builder API is style-only).
    }

    fn set_exclusions(&mut self, solutions: &[Solution]) {
        self.exclusions = solutions.iter().map(solution_to_bindingset).collect();
    }

    fn emit_solution(&mut self, solution: &Solution) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.ensure_pipeline();
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        let _ = sender.unbounded_send(solution_to_bindingset(solution));
        self.drain_ready();
    }

    fn cancel(&mut self) {
        // Spec M-S4: cancellation halts emission, no terminal signal.
        self.cancelled = true;
        self.sender = None;
        self.output = None;
    }

    fn complete(&mut self) {
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.ensure_pipeline();
        // Drop the sender so the filter stream sees EOF, then drain.
        self.sender = None;
        self.drain_to_end();
        self.terminal = Some(Terminal {
            kind: "complete".into(),
            error_type: None,
            message: None,
        });
    }

    fn error(&mut self, error_type: &str, message: Option<&str>) {
        // Spec M-S2: error on the main stream propagates verbatim. Rust's
        // `Stream<Item = BindingSet>` has no error item, so the adapter
        // records the terminal locally and tears down the pipeline. Unconsumed
        // pending emissions are intentionally left for the harness's
        // end-of-script exhaustiveness check to flag.
        if self.cancelled || self.terminal.is_some() {
            return;
        }
        self.sender = None;
        self.output = None;
        self.terminal = Some(Terminal {
            kind: "error".into(),
            error_type: Some(error_type.to_string()),
            message: message.map(str::to_string),
        });
    }

    fn drain_solutions(&mut self) -> Vec<Solution> {
        self.drain_ready();
        self.pending.drain(..).collect()
    }

    fn terminal_or_none(&self) -> Option<Terminal> {
        self.terminal.clone()
    }

    /// Round-trips each expected term-string through the engine's NTriples parser
    /// and back through `value_to_canonical_nt` so the harness's multiset comparator
    /// sees byte-identical canonical form on both expected and actual. Closes the
    /// xsd:string-canon asymmetry called out in parity-root#3 (2026-05-27).
    fn canonicalize_solution(&self, solution: Solution) -> Solution {
        let mut canonical = Solution::new();
        for (var, term_str) in solution {
            let parsed = parse_term_string(&term_str);
            canonical.insert(var, value_to_canonical_nt(&parsed));
        }
        canonical
    }
}

// ─── Conversions ─────────────────────────────────────────────────────────

fn solution_to_bindingset(sol: &Solution) -> BindingSet {
    let mut bs = BindingSet::new(0);
    for (var, term_str) in sol {
        bs.insert(var.as_str(), parse_term_string(term_str));
    }
    bs
}

fn bindingset_to_solution(bs: &BindingSet) -> Solution {
    bs.iter()
        .map(|(var, val)| (var.to_string(), value_to_canonical_nt(val)))
        .collect()
}

/// Parses an N-Triples-style term string into a `cqels_model::Value`.
///
/// Accepts the four N-Triples shapes: `<iri>`, `"literal"`, `"literal"@lang`,
/// `"literal"^^<datatype>`, and `_:bnode`. Wraps the term in a stub triple to
/// reuse oxttl's parser rather than rolling a single-term tokenizer.
fn parse_term_string(s: &str) -> Value {
    let stub = format!("<urn:s> <urn:p> {s} .\n");
    let mut iter = NTriplesParser::new().for_reader(stub.as_bytes());
    let triple = iter
        .next()
        .unwrap_or_else(|| panic!("expected one triple from stub for term: {s}"))
        .unwrap_or_else(|e| panic!("NTriples parse failed for term {s}: {e}"));
    Value::Term(Term::from(triple.object))
}

/// Serializes a `Value` back to N-Triples-style notation matching the fixture format.
///
/// `xsd:string`-typed plain literals collapse to bare `"foo"` (RDF 1.1 canonical form);
/// other typed/lang literals serialize verbatim via `cqels_model::Term`'s Display impl.
///
/// Defensive on `Value::Null`: per spec D5 (parity-root, 2026-05-27), the parity surface
/// uses absence-only UNBOUND — `Value::Null` is not a valid binding value in this adapter's
/// pipeline. Hits `unreachable!` with a diagnostic rather than silently emitting the literal
/// string `"null"` (cqels-rs#78). Today's fixtures never produce `Value::Null` because all
/// term-strings parse to `Value::Term`; if a future code path leaks a null binding through,
/// this fails loudly.
fn value_to_canonical_nt(v: &Value) -> String {
    let term = match v {
        Value::Term(t) => t.clone(),
        Value::Null => unreachable!(
            "Value::Null in MinusOperatorAdapter output — parity surface uses absence-only \
             UNBOUND (spec D5, cqels-rs#78). All fixture inputs parse to Value::Term; a null \
             binding indicates a latent bug in the adapter or engine."
        ),
        other => match other.to_term() {
            Some(t) => t,
            None => panic!(
                "value_to_canonical_nt received a non-Term Value with no to_term() conversion: \
                 {other:?} — extend the match arm if a new Value variant is added."
            ),
        },
    };
    if let Term::Literal(lit) = &term {
        if lit.language().is_none() && lit.datatype() == Some(XSD_STRING) {
            return format!("\"{}\"", lit.value());
        }
    }
    term.to_string()
}
