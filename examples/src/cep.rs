use cqels_core::parser::CqelsQlParser;

fn main() {
    let query = r#"
        SELECT ?sensor
        FROM STREAM sensors [RANGE 10s]
        WHERE {
            STREAM sensors {
                ?sensor <http://example.org/status> ?status .
            }
            FILTER(SEQ(?first; ?second))
        }
    "#;

    let definition = CqelsQlParser::parse(query).expect("CEP query should parse");
    let sequence = definition
        .seq_constraint
        .expect("query should contain a sequence constraint");
    println!("CEP sequence contains {} events", sequence.args.len());
}
