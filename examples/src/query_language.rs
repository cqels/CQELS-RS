use cqels_core::parser::CqelsQlParser;

fn main() {
    let query = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?sensor ?temperature
        FROM STREAM sensors [RANGE 10s]
        WHERE { ?sensor ex:temperature ?temperature . }
        ORDER BY ?temperature DESC
        LIMIT 5
    "#;

    let definition = CqelsQlParser::parse(query).expect("query should parse");
    println!("streams: {}", definition.streams.len());
    println!("select items: {}", definition.select_elements.len());
    println!("limit: {:?}", definition.limit);
}
