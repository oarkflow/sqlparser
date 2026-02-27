package parser_test

import (
	"testing"

	sqlparser "github.com/oarkflow/sqlparser"
	"github.com/oarkflow/sqlparser/ast"
)

// ---- helpers ----

func mustParse(t *testing.T, sql string) sqlparser.Statement {
	t.Helper()
	stmt, err := sqlparser.ParseStatement(sql)
	if err != nil {
		t.Fatalf("parse error: %v\nSQL: %s", err, sql)
	}
	return stmt
}

func mustParseAll(t *testing.T, sql string) []sqlparser.Statement {
	t.Helper()
	stmts, err := sqlparser.ParseStatements(sql)
	if err != nil {
		t.Fatalf("parse error: %v\nSQL: %s", err, sql)
	}
	return stmts
}

// ---- SELECT tests ----

func TestSelectSimple(t *testing.T) {
	stmt := mustParse(t, "SELECT 1")
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
}

func TestSelectStar(t *testing.T) {
	mustParse(t, "SELECT * FROM users")
}

func TestSelectMultiCol(t *testing.T) {
	stmt := mustParse(t, "SELECT id, name, email FROM users")
	sel := stmt.(*ast.SelectStmt)
	if len(sel.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(sel.Columns))
	}
}

func TestSelectWhere(t *testing.T) {
	mustParse(t, "SELECT * FROM users WHERE id = 42 AND active = true")
}

func TestSelectJoin(t *testing.T) {
	mustParse(t, `
		SELECT u.id, o.total
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		WHERE o.total > 100
		ORDER BY o.total DESC
		LIMIT 10`)
}

func TestSelectSubquery(t *testing.T) {
	mustParse(t, `
		SELECT * FROM (
			SELECT id, name FROM users WHERE active = 1
		) sub WHERE sub.name LIKE 'A%'`)
}

func TestSelectCTE(t *testing.T) {
	mustParse(t, `
		WITH active_users AS (
			SELECT id, name FROM users WHERE active = 1
		),
		recent_orders AS (
			SELECT user_id, COUNT(*) AS cnt FROM orders GROUP BY user_id
		)
		SELECT u.name, r.cnt
		FROM active_users u
		JOIN recent_orders r ON u.id = r.user_id`)
}

func TestSelectCase(t *testing.T) {
	mustParse(t, `
		SELECT id,
		       CASE status
		           WHEN 1 THEN 'active'
		           WHEN 0 THEN 'inactive'
		           ELSE 'unknown'
		       END AS label
		FROM users`)
}

func TestSelectAggregates(t *testing.T) {
	mustParse(t, `
		SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal, MAX(salary)
		FROM employees
		GROUP BY dept
		HAVING COUNT(*) > 5
		ORDER BY avg_sal DESC`)
}

func TestSelectDistinct(t *testing.T) {
	mustParse(t, "SELECT DISTINCT dept, role FROM employees")
}

func TestSelectUnion(t *testing.T) {
	mustParse(t, `
		SELECT id, name FROM users
		UNION ALL
		SELECT id, name FROM archived_users`)
}

func TestSelectSetOpChain(t *testing.T) {
	stmt := mustParse(t, `
		SELECT id FROM a
		UNION ALL
		SELECT id FROM b
		INTERSECT
		SELECT id FROM c`)
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if sel.SetOp == nil || sel.SetOp.Right == nil || sel.SetOp.Right.SetOp == nil {
		t.Fatalf("expected chained set operations")
	}
}

func TestSelectIn(t *testing.T) {
	mustParse(t, "SELECT * FROM t WHERE id IN (1, 2, 3)")
	mustParse(t, "SELECT * FROM t WHERE id NOT IN (SELECT id FROM blacklist)")
}

func TestSelectBetween(t *testing.T) {
	mustParse(t, "SELECT * FROM t WHERE age BETWEEN 18 AND 65")
}

func TestSelectLike(t *testing.T) {
	mustParse(t, "SELECT * FROM t WHERE name LIKE '%smith%' ESCAPE '\\'")
}

func TestSelectExists(t *testing.T) {
	mustParse(t, "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other WHERE other.id = t.id)")
}

func TestSelectCast(t *testing.T) {
	mustParse(t, "SELECT CAST(price AS DECIMAL(10,2)) FROM products")
}

func TestSelectMultipleJoins(t *testing.T) {
	mustParse(t, `
		SELECT a.id, b.name, c.total
		FROM a
		LEFT JOIN b ON a.b_id = b.id
		RIGHT JOIN c ON b.c_id = c.id
		CROSS JOIN d`)
}

func TestSelectOffset(t *testing.T) {
	mustParse(t, "SELECT * FROM t LIMIT 20 OFFSET 40")
	mustParse(t, "SELECT * FROM t LIMIT 40, 20")
}

func TestSelectFunctionCalls(t *testing.T) {
	mustParse(t, `SELECT NOW(), COALESCE(a, b, 0), IFNULL(x, 'default') FROM t`)
}

func TestSelectJSONBOperators(t *testing.T) {
	mustParse(t, `SELECT payload->'user' FROM events`)
	mustParse(t, `SELECT payload->>'user' FROM events`)
	mustParse(t, `SELECT payload#>'{user,name}' FROM events`)
	mustParse(t, `SELECT payload#>>'{user,name}' FROM events`)
	mustParse(t, `SELECT * FROM events WHERE payload @> '{"active":true}'`)
	mustParse(t, `SELECT * FROM events WHERE payload <@ payload`)
	mustParse(t, `SELECT * FROM events WHERE payload ? 'user'`)
	mustParse(t, `SELECT * FROM events WHERE payload ?| '{user,email}'`)
	mustParse(t, `SELECT * FROM events WHERE payload ?& '{user,email}'`)
}

// ---- INSERT tests ----

func TestInsertValues(t *testing.T) {
	mustParse(t, "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
}

func TestInsertMultiRow(t *testing.T) {
	mustParse(t, `INSERT INTO users (name, age) VALUES ('A', 1), ('B', 2), ('C', 3)`)
}

func TestInsertSelect(t *testing.T) {
	mustParse(t, "INSERT INTO archive SELECT * FROM users WHERE created_at < '2020-01-01'")
}

func TestInsertOnDuplicate(t *testing.T) {
	mustParse(t, `
		INSERT INTO counters (id, val) VALUES (1, 1)
		ON DUPLICATE KEY UPDATE val = val + 1`)
}

func TestInsertOnConflict(t *testing.T) {
	mustParse(t, `
		INSERT INTO counters (id, val) VALUES (1, 1)
		ON CONFLICT (id) DO UPDATE SET val = 2`)
	mustParse(t, `
		INSERT INTO counters (id, val) VALUES (1, 1)
		ON CONFLICT DO NOTHING`)
}

func TestInsertWithCTE(t *testing.T) {
	stmt := mustParse(t, `
		WITH recent AS (SELECT id, name FROM users WHERE active = 1)
		INSERT INTO archive_users (id, name)
		SELECT id, name FROM recent`)
	ins, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}
	if ins.With == nil {
		t.Fatalf("expected WITH clause on INSERT")
	}
}

func TestReplaceWithCTE(t *testing.T) {
	stmt := mustParse(t, `
		WITH src AS (SELECT 1 AS id, 'A' AS name)
		REPLACE INTO users (id, name)
		SELECT id, name FROM src`)
	ins, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}
	if ins.With == nil || !ins.Replace {
		t.Fatalf("expected WITH clause on REPLACE")
	}
}

func TestReplace(t *testing.T) {
	mustParse(t, "REPLACE INTO users (id, name) VALUES (1, 'Bob')")
}

// ---- UPDATE tests ----

func TestUpdateSimple(t *testing.T) {
	mustParse(t, "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1")
}

func TestUpdateLimit(t *testing.T) {
	mustParse(t, "UPDATE users SET active = 0 ORDER BY last_login ASC LIMIT 100")
}

func TestUpdateWithCTE(t *testing.T) {
	stmt := mustParse(t, `
		WITH targets AS (SELECT id FROM users WHERE active = 0)
		UPDATE users SET active = 1
		WHERE id IN (SELECT id FROM targets)`)
	upd, ok := stmt.(*ast.UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}
	if upd.With == nil {
		t.Fatalf("expected WITH clause on UPDATE")
	}
}

func TestUpdateJSONB(t *testing.T) {
	mustParse(t, `UPDATE events SET payload = payload || '{"processed":true}' WHERE payload ? 'user'`)
}

// ---- DELETE tests ----

func TestDeleteSimple(t *testing.T) {
	mustParse(t, "DELETE FROM users WHERE id = 42")
}

func TestDeleteLimit(t *testing.T) {
	mustParse(t, "DELETE FROM logs WHERE ts < NOW() ORDER BY ts ASC LIMIT 1000")
}

func TestDeleteWithCTE(t *testing.T) {
	stmt := mustParse(t, `
		WITH old_rows AS (SELECT id FROM logs WHERE ts < '2020-01-01')
		DELETE FROM logs WHERE id IN (SELECT id FROM old_rows)`)
	del, ok := stmt.(*ast.DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", stmt)
	}
	if del.With == nil {
		t.Fatalf("expected WITH clause on DELETE")
	}
}

func TestDeleteJSONB(t *testing.T) {
	mustParse(t, `DELETE FROM events WHERE payload @> '{"deleted":true}'`)
}

// ---- DDL tests ----

func TestCreateTable(t *testing.T) {
	mustParse(t, `
		CREATE TABLE users (
			id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			username   VARCHAR(64) NOT NULL,
			email      VARCHAR(255) NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (id),
			UNIQUE KEY uq_email (email),
			INDEX idx_username (username)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
}

func TestCreateTableJSONB(t *testing.T) {
	mustParse(t, `CREATE TABLE events (id BIGINT, payload JSONB, meta JSON)`)
}

func TestCreateTableForeignKey(t *testing.T) {
	mustParse(t, `
		CREATE TABLE orders (
			id      INT NOT NULL AUTO_INCREMENT,
			user_id INT NOT NULL,
			total   DECIMAL(10,2) NOT NULL,
			PRIMARY KEY (id),
			CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users (id)
			    ON DELETE CASCADE ON UPDATE RESTRICT
		) ENGINE=InnoDB`)
}

func TestCreateTableIfNotExists(t *testing.T) {
	mustParse(t, `CREATE TABLE IF NOT EXISTS config (k VARCHAR(64) PRIMARY KEY, v TEXT)`)
}

func TestCreateTableAsSelect(t *testing.T) {
	mustParse(t, `CREATE TABLE summary AS SELECT dept, COUNT(*) AS n FROM employees GROUP BY dept`)
}

func TestCreateIndex(t *testing.T) {
	mustParse(t, "CREATE UNIQUE INDEX idx_email ON users (email)")
	mustParse(t, "CREATE INDEX idx_multi ON t (a ASC, b DESC, c(10))")
}

func TestCreateView(t *testing.T) {
	mustParse(t, `
		CREATE OR REPLACE VIEW active_users AS
		SELECT id, name, email FROM users WHERE active = 1`)
}

func TestCreateDatabase(t *testing.T) {
	mustParse(t, "CREATE DATABASE IF NOT EXISTS appdb")
	mustParse(t, "CREATE SCHEMA analytics")
}

func TestAlterTable(t *testing.T) {
	mustParse(t, "ALTER TABLE users ADD COLUMN phone VARCHAR(20) AFTER email")
	mustParse(t, "ALTER TABLE users DROP COLUMN phone")
	mustParse(t, "ALTER TABLE users RENAME TO members")
	mustParse(t, "ALTER TABLE users ADD INDEX idx_phone (phone)")
}

func TestAlterDatabase(t *testing.T) {
	mustParse(t, "ALTER DATABASE appdb CHARACTER SET utf8mb4")
}

func TestDropTable(t *testing.T) {
	mustParse(t, "DROP TABLE IF EXISTS users, orders, products")
}

func TestDropDatabase(t *testing.T) {
	mustParse(t, "DROP DATABASE IF EXISTS appdb")
	mustParse(t, "DROP SCHEMA analytics")
}

func TestDropIndex(t *testing.T) {
	mustParse(t, "DROP INDEX idx_email ON users")
}

func TestTruncate(t *testing.T) {
	mustParse(t, "TRUNCATE TABLE logs")
}

func TestUse(t *testing.T) {
	mustParse(t, "USE mydb")
}

func TestShow(t *testing.T) {
	mustParse(t, "SHOW TABLES")
	mustParse(t, "SHOW TABLES LIKE 'user%'")
}

func TestExplain(t *testing.T) {
	mustParse(t, "EXPLAIN SELECT * FROM users WHERE id = 1")
}

func TestCallStatement(t *testing.T) {
	stmt := mustParse(t, "CALL refresh_cache(42, 'full')")
	if _, ok := stmt.(*ast.CallStmt); !ok {
		t.Fatalf("expected *CallStmt, got %T", stmt)
	}
}

func TestTransactionStatements(t *testing.T) {
	mustParse(t, "BEGIN")
	mustParse(t, "BEGIN TRANSACTION")
	mustParse(t, "COMMIT")
	mustParse(t, "ROLLBACK")
	mustParse(t, "ROLLBACK TO SAVEPOINT sp1")
	mustParse(t, "START TRANSACTION READ WRITE")
	mustParse(t, "SAVEPOINT sp1")
	mustParse(t, "RELEASE SAVEPOINT sp1")
	mustParse(t, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
}

func TestGenericRoutineDDL(t *testing.T) {
	stmt := mustParse(t, "CREATE FUNCTION f")
	if _, ok := stmt.(*ast.GenericDDLStmt); !ok {
		t.Fatalf("expected *GenericDDLStmt for CREATE FUNCTION, got %T", stmt)
	}
	stmt = mustParse(t, "DROP TRIGGER trg_before_insert")
	if _, ok := stmt.(*ast.GenericDDLStmt); !ok {
		t.Fatalf("expected *GenericDDLStmt for DROP TRIGGER, got %T", stmt)
	}
}

// ---- Multiple statements ----

func TestMultipleStatements(t *testing.T) {
	stmts := mustParseAll(t, `
		CREATE TABLE t (id INT);
		INSERT INTO t VALUES (1), (2);
		SELECT * FROM t WHERE id > 0;
		DROP TABLE t;
	`)
	if len(stmts) != 4 {
		t.Fatalf("expected 4 statements, got %d", len(stmts))
	}
}

// ---- Tokenizer tests ----

func TestTokenize(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE id = 1"
	buf := make([]sqlparser.Token, 0, 32)
	toks := sqlparser.Tokenize([]byte(sql), buf)
	if len(toks) == 0 {
		t.Fatal("expected tokens")
	}
}

// ---- Benchmark suite ----

var benchSQL = `
SELECT
    u.id,
    u.username,
    u.email,
    COUNT(o.id) AS order_count,
    SUM(o.total) AS total_spent,
    MAX(o.created_at) AS last_order
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.active = 1
  AND u.created_at BETWEEN '2023-01-01' AND '2024-01-01'
  AND u.country IN ('US', 'CA', 'GB')
GROUP BY u.id, u.username, u.email
HAVING COUNT(o.id) > 0
ORDER BY total_spent DESC
LIMIT 100 OFFSET 0`

var benchDDL = `
CREATE TABLE orders (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id     BIGINT UNSIGNED NOT NULL,
    state       TINYINT(1) NOT NULL DEFAULT 0,
    total       DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    created_at  DATETIME NOT NULL,
    PRIMARY KEY (id),
    KEY idx_user (user_id),
    KEY idx_state (state),
    KEY idx_created (created_at)
) ENGINE=InnoDB`

func BenchmarkParseSelect(b *testing.B) {
	src := []byte(benchSQL)
	p := sqlparser.New(src)
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Reset(src)
		_, err := p.Next()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseCreateTable(b *testing.B) {
	src := []byte(benchDDL)
	p := sqlparser.New(src)
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Reset(src)
		_, err := p.Next()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTokenize(b *testing.B) {
	src := []byte(benchSQL)
	buf := make([]sqlparser.Token, 0, 128)
	b.SetBytes(int64(len(src)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlparser.Tokenize(src, buf)
	}
}

func BenchmarkParseStatementString(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := sqlparser.ParseStatement(benchSQL)
		if err != nil {
			b.Fatal(err)
		}
	}
}
