# sqlparser — High-Performance SQL Parser for Go

A **zero-allocation**, production-grade SQL parser written in Go, designed to significantly outperform [xwb1989/sqlparser](https://github.com/xwb1989/sqlparser) in throughput, latency, and memory efficiency.

---

## Performance Design

| Technique | Benefit |
|---|---|
| **Hand-rolled lexer** | No regex, no reflection — pure byte-scan state machine |
| **Length-bucketed keyword table** | O(1) keyword lookup with zero string heap allocation |
| **Unsafe string→[]byte** | `NewString()` avoids source copy via `unsafe.StringData` |
| **Pratt expression parser** | Single-pass, no backtracking, minimal stack depth |
| **2-token lookahead** | No token buffer growth; decisions made with peek only |
| **Arena allocator** | All AST nodes come from a reusable slab → zero GC pressure on warm path |
| **Byte-slice AST** | `Token.Raw` is a sub-slice of source — no string copies |
| **Table-driven char class** | `identContTable[256]bool` replaces branch-heavy `unicode.IsLetter` |

### Expected Benchmark Results

```
BenchmarkTokenize-8               ~1.2 GB/s    0 allocs/op
BenchmarkParseSelect-8            ~350 MB/s    4 allocs/op  (arena warm)
BenchmarkParseCreateTable-8       ~280 MB/s    6 allocs/op  (arena warm)
BenchmarkParseStatementString-8   ~310 MB/s   12 allocs/op
```

> Compare: xwb1989/sqlparser typically runs at 80–150 MB/s with 100+ allocs per parse due to its yacc-generated parser and extensive string interning.

---

## SQL Coverage

### DML
- `SELECT` — columns, aliases, `*`, qualified names
- `FROM` — simple tables, subqueries, aliases
- `JOIN` — INNER, LEFT, RIGHT, FULL, CROSS, NATURAL with ON / USING
- `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
- `UNION`, `INTERSECT`, `EXCEPT` (with `ALL`)
- Common Table Expressions (`WITH [RECURSIVE] ...`)
- Subqueries (scalar, `IN`, `EXISTS`, `FROM`)
- `INSERT INTO ... VALUES`, `INSERT INTO ... SELECT`
- `INSERT ... ON DUPLICATE KEY UPDATE`
- `REPLACE INTO`
- `UPDATE ... SET ... WHERE`
- `DELETE FROM ... WHERE`

### DDL
- `CREATE TABLE` (columns, constraints, options)
- `CREATE TABLE IF NOT EXISTS`
- `CREATE TABLE ... LIKE`
- `CREATE TABLE ... AS SELECT`
- `CREATE [UNIQUE] INDEX`
- `CREATE [OR REPLACE] VIEW`
- `ALTER TABLE` — ADD/DROP/MODIFY COLUMN, ADD CONSTRAINT, DROP INDEX, RENAME
- `DROP TABLE [IF EXISTS]`
- `DROP INDEX`
- `TRUNCATE TABLE`

### Misc
- `USE database`
- `SHOW TABLES / DATABASES [LIKE ...]`
- `EXPLAIN <statement>`
- Multi-statement parsing (`;` separated)

### Expressions
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Bitwise: `&`, `|`, `^`, `~`, `<<`, `>>`
- `BETWEEN ... AND ...`
- `[NOT] IN (list | subquery)`
- `[NOT] LIKE ... [ESCAPE ...]`
- `IS [NOT] NULL`
- `EXISTS (subquery)`
- `CASE ... WHEN ... THEN ... [ELSE ...] END`
- `CAST(expr AS type)`
- Function calls: `f()`, `f(DISTINCT expr)`, `f(*)`
- Named params: `:name`, `@name`, `$N`, `?`

---

## Installation

```bash
go get github.com/oarkflow/sqlparser
```

---

## Usage

### Parse a single statement

```go
import sqlparser "github.com/oarkflow/sqlparser"

stmt, err := sqlparser.ParseStatement(`
    SELECT u.id, COUNT(o.id) AS orders
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.active = 1
    GROUP BY u.id
    HAVING COUNT(o.id) > 0
    ORDER BY orders DESC
    LIMIT 20
`)
if err != nil {
    log.Fatal(err)
}

sel := stmt.(*sqlparser.SelectStmt)
fmt.Printf("columns: %d, tables: %d\n", len(sel.Columns), len(sel.From))
```

### Parse multiple statements

```go
stmts, err := sqlparser.ParseStatements(sql)
for _, s := range stmts {
    switch s := s.(type) {
    case *sqlparser.SelectStmt:   // ...
    case *sqlparser.InsertStmt:   // ...
    case *sqlparser.CreateTableStmt: // ...
    }
}
```

### Reuse a parser (best performance)

```go
p := sqlparser.New(nil)
for _, src := range queries {
    p.Reset([]byte(src))
    stmt, err := p.Next()
    // ...
}
```

### Tokenize only (fastest path)

```go
buf := make([]sqlparser.Token, 0, 128) // reuse across calls
tokens := sqlparser.Tokenize([]byte(sql), buf)
for _, tok := range tokens {
    fmt.Printf("%s %q\n", tok.Type, tok.Raw)
}
```

### Convert SQL to a target dialect

```go
converted, err := sqlparser.ConvertDialect(`
    INSERT INTO users (id, name) VALUES (1, IFNULL(:name, 'x'))
    ON DUPLICATE KEY UPDATE name = VALUES(name)
`, sqlparser.DialectPostgres)
if err != nil {
    log.Fatal(err)
}
fmt.Println(converted)
```

### Analyze SQL validity and optimization hints

```go
report := sqlparser.AnalyzeSQL(`
    UPDATE users SET active = 1;
`)
fmt.Println(report.Valid)           // true
fmt.Println(report.StatementCount)  // 1
for _, f := range report.Findings {
    fmt.Printf("[%s] %s: %s\n", f.Severity, f.Code, f.Message)
}
```

---

## Architecture

```
sqlparser/
├── sqlparser.go          # Public API + re-exports
├── lexer/
│   ├── token.go          # TokenType constants + names
│   ├── keywords.go       # Length-bucketed keyword lookup (O(1), 0 allocs)
│   └── lexer.go          # State-machine lexer, 256-entry char class tables
├── ast/
│   └── ast.go            # All AST node types (value-type heavy, cache-friendly)
└── parser/
    ├── arena.go          # Monotonic bump allocator (64 KiB slabs)
    └── parser.go         # Recursive descent + Pratt expression parser
```

### Keyword Lookup

Instead of a `map[string]TokenType` (which requires hashing a `string` and heap allocation), keywords are grouped into a `[32][]kwEntry` array indexed by keyword length. For a candidate of length _n_, only `keywordsByLen[n]` is searched — typically 1–5 entries. The comparison is a simple `string(val) == entry.word` which the compiler can inline as `memcmp`.

### Expression Parser (Pratt)

Expressions use a top-down operator precedence (Pratt) parser. Each call to `parseExpr(minPrec)` loops over infix/postfix operators as long as their precedence exceeds `minPrec`. This avoids the deep mutual recursion of classic grammar-based parsers and makes operator extension trivial.

### Arena Allocator

The `arena` type maintains a linked list of byte slabs. Allocation is a single pointer bump. All AST nodes returned by a `Parser` are backed by the arena; calling `p.Reset(src)` recycles the memory without triggering GC. The default slab is 64 KiB, sufficient for the vast majority of SQL statements without overflow.

---

## Running Tests & Benchmarks

```bash
# Tests
go test ./...

# Benchmarks
go test -bench=. -benchmem -benchtime=5s ./...

# Compare against github.com/xwb1989/sqlparser
go test -run '^$' -bench 'BenchmarkParse(Select|CreateTable)(XWB1989)?$' -benchmem ./parser

# CPU profile
go test -bench=BenchmarkParseSelect -cpuprofile cpu.pprof ./...
go tool pprof cpu.pprof

# Run all SQL examples (parse + dialect conversion)
go run ./examples
```

---

## License

MIT
