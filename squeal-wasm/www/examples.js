// Runnable examples shown in the demo's Help panel and Examples menu. Each
// is { mode: 'sql' | 'json', title, text }; clicking one loads it into the
// prompt in its mode. test-examples.mjs runs every one, in order, against
// the real wasm build, so nothing here is advertised that doesn't work.

export const EXAMPLES = [
  // ---- SQL ---------------------------------------------------------------
  {
    mode: 'sql',
    title: 'Create tables and load a few rows',
    text: `create table customers (id integer not null, name varchar(30), city varchar(20), primary key(id));
create table orders (id integer not null, customer_id integer references customers(id),
  item varchar(20), qty integer, price double, primary key(id));
insert into customers values (1, 'Ada', 'London'), (2, 'Grace', 'New York'), (3, 'Linus', 'Helsinki');
insert into orders values (10, 1, 'pen', 3, 1.5), (11, 1, 'ink', 1, 7.25), (12, 2, 'pad', 5, 2.0),
  (13, 3, 'pen', 10, 1.5), (14, 2, 'pen', 2, 1.5);`,
  },
  {
    mode: 'sql',
    title: 'Filter, sort and limit',
    text: `select item, qty, price from orders where qty >= 2 order by qty desc limit 3;`,
  },
  {
    mode: 'sql',
    title: 'Join and aggregate',
    text: `select c.name, count(*) as orders, sum(o.qty * o.price) as spent
from customers c join orders o on o.customer_id = c.id
group by c.name order by spent desc;`,
  },
  {
    mode: 'sql',
    title: 'Common table expression',
    text: `with big as (select * from orders where qty > 2)
select item, sum(qty) from big group by item;`,
  },
  {
    mode: 'sql',
    title: 'Index, then see the plan use it',
    text: `create index orders_item on orders (item);
explain select * from orders where item = 'pen';`,
  },
  {
    mode: 'sql',
    title: 'Update in a transaction, then roll back',
    text: `begin;
update orders set qty = 0 where item = 'pen';
select item, qty from orders where item = 'pen';
rollback;
select item, qty from orders where item = 'pen';`,
  },
  {
    mode: 'sql',
    title: 'Look around: tables, columns, schemas',
    text: `show tables;
describe table orders;
show schemas;`,
  },

  // ---- JSON (documents) --------------------------------------------------
  {
    mode: 'json',
    title: 'Insert documents',
    text: `use shop
db.products.insertMany([
  {_id: 1, name: 'pen', price: 1.5, tags: ['office', 'writing'], stock: {warehouse: 120, store: 8}},
  {_id: 2, name: 'ink', price: 7.25, tags: ['writing'], stock: {warehouse: 30, store: 0}},
  {_id: 3, name: 'pad', price: 2.0, tags: ['office', 'paper'], stock: {warehouse: 0, store: 14}},
  {_id: 4, name: 'stapler', price: 9.99, tags: ['office'], stock: {warehouse: 5, store: 2}}
])`,
  },
  {
    mode: 'json',
    title: 'Find with operators, projection, sort',
    text: `db.products.find({price: {$lt: 5}, tags: 'office'}, {name: 1, price: 1, _id: 0})
  .sort({price: -1})`,
  },
  {
    mode: 'json',
    title: 'Nested fields, arrays, regex',
    text: `db.products.find({'stock.store': {$gt: 0}}, {name: 1})
db.products.find({tags: {$all: ['office', 'writing']}})
db.products.find({name: {$regex: '^p'}}).count()`,
  },
  {
    mode: 'json',
    title: 'Update operators',
    text: `db.products.updateOne({_id: 2}, {$inc: {'stock.store': 25}, $push: {tags: 'refill'}})
db.products.updateMany({price: {$gt: 5}}, {$set: {premium: true}})
db.products.find({premium: true}, {name: 1, tags: 1, stock: 1})`,
  },
  {
    mode: 'json',
    title: 'Upsert and findOneAndUpdate',
    text: `db.products.updateOne({_id: 5}, {$set: {name: 'eraser', price: 0.5}}, {upsert: true})
db.products.findOneAndUpdate({_id: 5}, {$inc: {price: 0.25}}, {returnDocument: 'after'})`,
  },
  {
    mode: 'json',
    title: 'Aggregation pipeline',
    text: `db.products.aggregate([
  {$unwind: '$tags'},
  {$group: {_id: '$tags', items: {$sum: 1}, avgPrice: {$avg: '$price'}}},
  {$sort: {items: -1, _id: 1}}
])`,
  },
  {
    mode: 'json',
    title: 'Join collections with $lookup',
    text: `db.orders.insertMany([{_id: 100, product: 1, qty: 3}, {_id: 101, product: 3, qty: 2}])
db.orders.aggregate([
  {$lookup: {from: 'products', localField: 'product', foreignField: '_id', as: 'p'}},
  {$unwind: '$p'},
  {$project: {_id: 0, order: '$_id', name: '$p.name', total: {$multiply: ['$qty', '$p.price']}}}
])`,
  },
  {
    mode: 'json',
    title: 'Index a field and check the plan',
    text: `db.products.createIndex({price: 1})
db.products.find({price: {$gte: 2}}).explain()
db.products.getIndexes()`,
  },
  {
    mode: 'json',
    title: 'Multi-document transaction',
    text: `begin
db.products.updateOne({_id: 1}, {$inc: {'stock.store': -1}})
db.orders.insertOne({_id: 102, product: 1, qty: 1})
commit
db.orders.countDocuments({})`,
  },
  {
    mode: 'json',
    title: 'Look around: databases and collections',
    text: `show dbs
show collections
db.products.distinct('tags')`,
  },
];
