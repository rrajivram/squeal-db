use store::valueitem::ValueItem;

use super::*;

fn select_rows(c: &Arc<Connection<MemFile>>, table_name: &str) -> Vec<Vec<ValueItem>> {
    let s = c.current_schema().unwrap();
    s.select_all(table_name, None).unwrap().rows().to_vec()
}

#[test]
fn test_inline_foreign_key_enforces_referential_integrity() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer references customers(id), \
         primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into customers values (1)").unwrap();
    execute(&c, "insert into orders values (1, 1)").unwrap();

    let err = execute(&c, "insert into orders values (2, 99)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");

    assert_eq!(
        select_rows(&c, "orders"),
        vec![vec![ValueItem::Integer(1), ValueItem::Integer(1)]],
        "the violating row must not have been written"
    );
}

#[test]
fn test_table_level_foreign_key_constraint_enforces_referential_integrity() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id), \
         foreign key(customer_id) references customers(id))",
    )
    .unwrap();
    execute(&c, "insert into customers values (1)").unwrap();
    execute(&c, "insert into orders values (1, 1)").unwrap();
    let err = execute(&c, "insert into orders values (2, 99)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_foreign_key_allows_null_value() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer references customers(id), \
         primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into orders values (1, null)").unwrap();

    assert_eq!(
        select_rows(&c, "orders"),
        vec![vec![ValueItem::Integer(1), ValueItem::Null]]
    );
}

#[test]
fn test_insert_batch_can_reference_an_earlier_row_in_the_same_batch() {
    // Exercises the read-your-own-writes fix from the transaction work:
    // row 2 references row 1's id, both in the same INSERT statement /
    // same transaction.
    let c = conn();
    execute(
        &c,
        "create table categories (id integer not null, parent_id integer references categories(id), \
         primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into categories values (1, null), (2, 1)").unwrap();

    let mut rows = select_rows(&c, "categories");
    rows.sort_by_key(|r| match &r[0] {
        ValueItem::Integer(i) => *i,
        _ => panic!("expected an integer id"),
    });
    assert_eq!(
        rows,
        vec![
            vec![ValueItem::Integer(1), ValueItem::Null],
            vec![ValueItem::Integer(2), ValueItem::Integer(1)],
        ]
    );
}

#[test]
fn test_create_table_rejects_foreign_key_to_an_unknown_table() {
    let c = conn();
    let err = execute(
        &c,
        "create table orders (id integer not null, customer_id integer references nope(id), \
         primary key(id))",
    )
    .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_table_rejects_foreign_key_to_a_non_unique_column() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, name varchar(10))").unwrap();
    let err = execute(
        &c,
        "create table orders (id integer not null, customer_name varchar(10) \
         references customers(name), primary key(id))",
    )
    .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_create_table_rejects_foreign_key_type_mismatch() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    let err = execute(
        &c,
        "create table orders (id integer not null, customer_id varchar(10) \
         references customers(id), primary key(id))",
    )
    .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_self_referential_foreign_key_at_create_table_works() {
    let c = conn();
    execute(
        &c,
        "create table employees (id integer not null, manager_id integer references employees(id), \
         primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into employees values (1, null)").unwrap();
    execute(&c, "insert into employees values (2, 1)").unwrap();
    let err = execute(&c, "insert into employees values (3, 99)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_alter_table_add_foreign_key_rejects_existing_violating_rows() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into orders values (1, 99)").unwrap();

    let err = execute(
        &c,
        "alter table orders add foreign key (customer_id) references customers(id)",
    )
    .unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");

    // Rejected, not partially applied.
    execute(&c, "insert into orders values (2, 100)").unwrap();
}

#[test]
fn test_alter_table_add_foreign_key_succeeds_when_existing_rows_are_valid() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into customers values (1)").unwrap();
    execute(&c, "insert into orders values (1, 1)").unwrap();

    execute(
        &c,
        "alter table orders add constraint fk_cust foreign key (customer_id) references customers(id)",
    )
    .unwrap();

    let err = execute(&c, "insert into orders values (2, 99)").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_alter_table_add_foreign_key_null_existing_values_are_ignored() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id))",
    )
    .unwrap();
    execute(&c, "insert into orders values (1, null)").unwrap();

    execute(
        &c,
        "alter table orders add foreign key (customer_id) references customers(id)",
    )
    .unwrap();
}

#[test]
fn test_alter_table_drop_foreign_key_removes_the_constraint() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer, primary key(id), \
         constraint fk_cust foreign key(customer_id) references customers(id))",
    )
    .unwrap();

    execute(&c, "alter table orders drop constraint fk_cust").unwrap();
    // No longer enforced.
    execute(&c, "insert into orders values (1, 99)").unwrap();
}

#[test]
fn test_alter_table_drop_foreign_key_rejects_an_unknown_constraint_name() {
    let c = conn();
    execute(&c, "create table orders (id integer not null, primary key(id))").unwrap();
    let err = execute(&c, "alter table orders drop constraint nope").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_drop_column_rejects_the_local_foreign_key_column() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer references customers(id), \
         primary key(id))",
    )
    .unwrap();
    let err = execute(&c, "alter table orders drop column customer_id").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_drop_column_rejects_a_column_referenced_by_another_tables_foreign_key() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer references customers(id), \
         primary key(id))",
    )
    .unwrap();
    let err = execute(&c, "alter table customers drop column id").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_rename_column_rejects_a_column_referenced_by_another_tables_foreign_key() {
    let c = conn();
    execute(&c, "create table customers (id integer not null, primary key(id))").unwrap();
    execute(
        &c,
        "create table orders (id integer not null, customer_id integer references customers(id), \
         primary key(id))",
    )
    .unwrap();
    let err = execute(&c, "alter table customers rename column id to uid").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}

#[test]
fn test_rename_column_rejects_the_ref_column_of_a_self_referential_foreign_key() {
    let c = conn();
    execute(
        &c,
        "create table employees (id integer not null, manager_id integer references employees(id), \
         primary key(id))",
    )
    .unwrap();
    let err = execute(&c, "alter table employees rename column id to uid").unwrap_err();
    assert!(matches!(err, SchemaError::UserError(_)), "got {err:?}");
}
