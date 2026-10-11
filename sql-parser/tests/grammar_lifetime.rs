// The grammar a parse builds is freed when the parse is done.
//
// Its recursive roots (expressions, queries, statements) point at each
// other; with owning handles that is a reference cycle, and every parse
// leaked its whole grammar (13.7 KB). A binary of its own, since counting
// what is live needs the global allocator — and one test, so no other
// thread allocates while it counts.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicIsize, Ordering},
};

use chumsky::{Parser, extra, prelude::EmptyErr};
use sql_parser::{Expr, lexer::tokenize, parse_sql, parser::SQLParser};

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size as isize - layout.size() as isize, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn test_a_parse_leaves_nothing_behind() {
    let rows: Vec<String> = (0..100).map(|i| format!("({i}, 'x')")).collect();
    let bulk = format!("insert into t values {}", rows.join(", "));
    for sql in [
        "select a from t where id = 1",
        "select a from t where id in (select b from u) and exists (select 1 from v)",
        "explain select a from t",
        bulk.as_str(),
        "select from where",
    ] {
        // Once first: anything built lazily and kept for good.
        let _ = parse_sql(sql);
        let before = LIVE.load(Ordering::Relaxed);
        for _ in 0..20 {
            let _ = parse_sql(sql);
        }
        let retained = LIVE.load(Ordering::Relaxed) - before;
        assert!(retained <= 0, "{sql:?}: {retained} bytes still live after 20 parses");
    }

    // An expression's handle to the query grammar is weak: the parser
    // `Expr::parser(())` returns must keep the rest alive itself, or a
    // subquery inside it finds the query grammar gone.
    let tokens = tokenize("a in (select b from u where c = (select 1))").unwrap();
    let parser = <Expr as SQLParser<_, extra::Err<EmptyErr>>>::parser(());
    assert!(parser.parse(tokens.as_slice()).into_result().is_ok());
}
