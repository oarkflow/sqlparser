package sqlparser_test

import (
	"testing"

	sqlparser "github.com/oarkflow/sqlparser"
)

func TestAnalyzeSQLParseError(t *testing.T) {
	report := sqlparser.AnalyzeSQL("SELECT FROM")
	if report.Valid {
		t.Fatalf("expected invalid SQL")
	}
	if len(report.Findings) == 0 || report.Findings[0].Code != "PARSE_ERROR" {
		t.Fatalf("expected PARSE_ERROR finding, got %#v", report.Findings)
	}
}

func TestAnalyzeSQLRiskyPatterns(t *testing.T) {
	sql := `SELECT * FROM users WHERE name LIKE '%abc'; UPDATE users SET active = 1; DELETE FROM logs;`
	report := sqlparser.AnalyzeSQL(sql)
	if !report.Valid {
		t.Fatalf("expected valid SQL, got parse error: %#v", report.Findings)
	}
	codes := map[string]bool{}
	for _, f := range report.Findings {
		codes[f.Code] = true
	}
	for _, code := range []string{"SELECT_STAR", "LIKE_LEADING_WILDCARD", "UPDATE_WITHOUT_WHERE", "DELETE_WITHOUT_WHERE"} {
		if !codes[code] {
			t.Fatalf("expected finding %s, findings=%#v", code, report.Findings)
		}
	}
}

func TestAnalyzeSQLJSONBHint(t *testing.T) {
	report := sqlparser.AnalyzeSQL(`CREATE TABLE events (payload JSONB)`)
	if !report.Valid {
		t.Fatalf("expected valid SQL")
	}
	found := false
	for _, f := range report.Findings {
		if f.Code == "JSONB_DIALECT_NOTE" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected JSONB_DIALECT_NOTE finding")
	}
}
