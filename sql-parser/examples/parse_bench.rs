// Parse timing for a few statement shapes: `cargo run --release -p sql-parser --example parse_bench`.
use std::time::Instant;
fn main() {
    let rows: Vec<String> = (0..500)
        .map(|i| format!("({i}, {}, 'stapler', {}, {}.25, {})", i % 997, i % 10, i % 50, i % 365))
        .collect();
    let insert = format!("insert into orders values {}", rows.join(", "));
    let cases = [
        ("500-row INSERT", insert.as_str(), 200),
        ("point SELECT", "select id, customer_id, item, qty, price from orders where id = 12345", 20_000),
        (
            "join + group",
            "select c.city, count(*), sum(o.qty * o.price) from customers c join orders o on o.customer_id = c.id where o.day between 3 and 9 group by c.city order by 2 desc limit 5",
            5_000,
        ),
    ];
    for (name, sql, n) in cases {
        let t = Instant::now();
        for _ in 0..n {
            std::hint::black_box(sql_parser::parse_sql(std::hint::black_box(sql)).unwrap());
        }
        println!("{name:<16} {:>10.1} us/parse", t.elapsed().as_secs_f64() * 1e6 / n as f64);
    }
}
