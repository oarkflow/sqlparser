# Examples

This folder contains runnable end-to-end SQL samples.

## Run all examples

```bash
go run ./examples
```

The runner will:
- Parse every `.sql` file in `examples/sql/`
- Print parse status and statement count
- Print developer-friendly analysis findings (problem + recommendation)
- Print dialect-aware findings for MySQL/Postgres/SQLite
- Print optimization actions and whether SQL was converted for each dialect
- Convert each sample to MySQL, PostgreSQL, and SQLite SQL

### Comprehensive sample

`examples/sql/10_complete_everything.sql` contains an end-to-end SQL script covering:
- Database/table/index/view DDL
- CTEs, joins, subqueries, unions/intersections
- JSONB field usage + operators
- Insert/upsert (`ON DUPLICATE KEY`, `ON CONFLICT`)
- Update/delete with conditions and limits
- Transactions and `CALL`
- Routine/trigger fallback DDL
- Drop/cleanup statements

## Add your own examples

1. Add a new `*.sql` file under `examples/sql/`
2. Run `go run ./examples`
