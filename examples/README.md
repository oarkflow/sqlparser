# Examples

This folder contains runnable end-to-end SQL samples.

## Run all examples

```bash
go run ./examples
```

The runner will:
- Parse every `.sql` file in `examples/sql/`
- Print parse status and statement count
- Convert each sample to MySQL, PostgreSQL, and SQLite SQL

## Add your own examples

1. Add a new `*.sql` file under `examples/sql/`
2. Run `go run ./examples`

