package parser_test

import (
	"testing"

	sqlparser "github.com/oarkflow/sqlparser"
)

// FuzzParser tests the parser with random input to ensure it never panics.
// Parse errors are expected and OK; panics are not.
func FuzzParser(f *testing.F) {
	seeds := []string{
		"SELECT * FROM users",
		"SELECT id, name FROM t WHERE id = 1",
		"INSERT INTO t (a) VALUES (1)",
		"UPDATE t SET x = 1",
		"DELETE FROM t WHERE id = 1",
		"CREATE TABLE t (id INT)",
		"DROP TABLE t",
		"ALTER TABLE t ADD COLUMN x INT",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"",
		"SELECT",
		"SELECT FROM",
		"INSERT",
		"CREATE",
		";;;",
		"SELECT 'unterminated",
		"SELECT 1 + 2 * 3 - 4 / 5",
		"SELECT * FROM t WHERE x IN (1,2,3) AND y BETWEEN 1 AND 10",
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// We don't care about errors, only panics
		sqlparser.ParseStatements(sql)
	})
}
