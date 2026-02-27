package sqlparser

import (
	"fmt"
	"strings"

	"github.com/oarkflow/sqlparser/ast"
)

type FindingSeverity string

const (
	SeverityInfo     FindingSeverity = "info"
	SeverityWarning  FindingSeverity = "warning"
	SeverityCritical FindingSeverity = "critical"
)

type AnalysisFinding struct {
	Severity       FindingSeverity
	Code           string
	Message        string
	StatementIndex int
}

type AnalysisReport struct {
	Valid          bool
	StatementCount int
	Findings       []AnalysisFinding
}

func AnalyzeSQL(sql string) AnalysisReport {
	report := AnalysisReport{}
	stmts, err := ParseStatements(sql)
	if err != nil {
		report.Valid = false
		report.Findings = append(report.Findings, AnalysisFinding{
			Severity:       SeverityCritical,
			Code:           "PARSE_ERROR",
			Message:        err.Error(),
			StatementIndex: -1,
		})
		return report
	}
	report.Valid = true
	report.StatementCount = len(stmts)

	for i, stmt := range stmts {
		analyzeStatement(stmt, i, &report)
	}
	return report
}

func analyzeStatement(stmt Statement, idx int, report *AnalysisReport) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		if hasSelectStar(s.Columns) {
			addFinding(report, SeverityWarning, "SELECT_STAR", "SELECT * can increase IO and break projection stability", idx)
		}
		if s.SetOp != nil {
			for cur := s.SetOp; cur != nil; cur = cur.Right.SetOp {
				if cur.Op == ast.Union && !cur.All {
					addFinding(report, SeverityInfo, "UNION_DISTINCT_COST", "UNION (without ALL) requires de-dup sort/hash; use UNION ALL if possible", idx)
				}
			}
		}
		for _, tr := range s.From {
			if jt, ok := tr.(*ast.JoinTable); ok && jt.Kind == ast.CrossJoin {
				addFinding(report, SeverityWarning, "CROSS_JOIN", "CROSS JOIN may cause large cartesian products", idx)
			}
		}
		analyzeExpr(s.Where, idx, report)
		analyzeExpr(s.Having, idx, report)
		for _, c := range s.Columns {
			analyzeExpr(c.Expr, idx, report)
		}
	case *ast.InsertStmt:
		if len(s.Values) > 1000 {
			addFinding(report, SeverityInfo, "BULK_INSERT_SIZE", "Large VALUES list; consider batch/chunk strategy", idx)
		}
		if len(s.OnDupKey) > 0 || len(s.OnConflictUpdate) > 0 {
			addFinding(report, SeverityInfo, "UPSERT_PRESENT", "Upsert clause detected; ensure target unique index exists", idx)
		}
		if s.Select != nil {
			for _, c := range s.Select.Columns {
				analyzeExpr(c.Expr, idx, report)
			}
		}
	case *ast.UpdateStmt:
		if s.Where == nil {
			addFinding(report, SeverityCritical, "UPDATE_WITHOUT_WHERE", "UPDATE without WHERE affects all rows", idx)
		}
		if s.Limit != nil && len(s.Order) == 0 {
			addFinding(report, SeverityWarning, "UPDATE_LIMIT_NO_ORDER", "UPDATE with LIMIT but no ORDER BY may be nondeterministic", idx)
		}
		analyzeExpr(s.Where, idx, report)
		for _, a := range s.Set {
			analyzeExpr(a.Value, idx, report)
		}
	case *ast.DeleteStmt:
		if s.Where == nil {
			addFinding(report, SeverityCritical, "DELETE_WITHOUT_WHERE", "DELETE without WHERE affects all rows", idx)
		}
		if s.Limit != nil && len(s.Order) == 0 {
			addFinding(report, SeverityWarning, "DELETE_LIMIT_NO_ORDER", "DELETE with LIMIT but no ORDER BY may be nondeterministic", idx)
		}
		analyzeExpr(s.Where, idx, report)
	case *ast.CreateTableStmt:
		for _, c := range s.Columns {
			if c.Type != nil && strings.EqualFold(string(c.Type.Name), "jsonb") {
				addFinding(report, SeverityInfo, "JSONB_DIALECT_NOTE", "JSONB column detected; MySQL/SQLite conversion rewrites type semantics", idx)
			}
		}
	case *ast.GenericDDLStmt:
		addFinding(report, SeverityWarning, "GENERIC_DDL", "Statement parsed via generic DDL fallback; semantic fidelity may be limited", idx)
	}
}

func analyzeExpr(e Expr, idx int, report *AnalysisReport) {
	if e == nil {
		return
	}
	switch ex := e.(type) {
	case *ast.LikeExpr:
		if lit, ok := ex.Pattern.(*ast.Literal); ok {
			raw := string(lit.Raw)
			if strings.HasPrefix(raw, "'%") || strings.HasPrefix(raw, "\"%") {
				addFinding(report, SeverityInfo, "LIKE_LEADING_WILDCARD", "Leading wildcard LIKE pattern may prevent index usage", idx)
			}
		}
		analyzeExpr(ex.Expr, idx, report)
		analyzeExpr(ex.Pattern, idx, report)
		analyzeExpr(ex.Escape, idx, report)
	case *ast.BinaryExpr:
		if strings.EqualFold(ex.Op.String(), "OR") {
			addFinding(report, SeverityInfo, "OR_PREDICATE", "OR predicates can reduce index selectivity and complicate plans", idx)
		}
		analyzeExpr(ex.Left, idx, report)
		analyzeExpr(ex.Right, idx, report)
	case *ast.UnaryExpr:
		analyzeExpr(ex.Expr, idx, report)
	case *ast.FuncCall:
		for _, a := range ex.Args {
			analyzeExpr(a, idx, report)
		}
	case *ast.CaseExpr:
		analyzeExpr(ex.Operand, idx, report)
		analyzeExpr(ex.Else, idx, report)
		for _, w := range ex.Whens {
			analyzeExpr(w.Cond, idx, report)
			analyzeExpr(w.Result, idx, report)
		}
	case *ast.BetweenExpr:
		analyzeExpr(ex.Expr, idx, report)
		analyzeExpr(ex.Lo, idx, report)
		analyzeExpr(ex.Hi, idx, report)
	case *ast.InExpr:
		analyzeExpr(ex.Expr, idx, report)
		for _, v := range ex.List {
			analyzeExpr(v, idx, report)
		}
		if ex.Subq != nil {
			for _, c := range ex.Subq.Columns {
				analyzeExpr(c.Expr, idx, report)
			}
			analyzeExpr(ex.Subq.Where, idx, report)
		}
	case *ast.IsNullExpr:
		analyzeExpr(ex.Expr, idx, report)
	case *ast.ExistsExpr:
		if ex.Subq != nil {
			for _, c := range ex.Subq.Columns {
				analyzeExpr(c.Expr, idx, report)
			}
			analyzeExpr(ex.Subq.Where, idx, report)
		}
	case *ast.SubqueryExpr:
		if ex.Subq != nil {
			for _, c := range ex.Subq.Columns {
				analyzeExpr(c.Expr, idx, report)
			}
			analyzeExpr(ex.Subq.Where, idx, report)
		}
	case *ast.CastExpr:
		analyzeExpr(ex.Expr, idx, report)
	}
}

func hasSelectStar(cols []ast.SelectColumn) bool {
	for _, c := range cols {
		if c.Star {
			return true
		}
	}
	return false
}

func addFinding(report *AnalysisReport, sev FindingSeverity, code, msg string, idx int) {
	report.Findings = append(report.Findings, AnalysisFinding{
		Severity:       sev,
		Code:           code,
		Message:        msg,
		StatementIndex: idx,
	})
}

func (r AnalysisReport) String() string {
	if !r.Valid {
		if len(r.Findings) == 0 {
			return "invalid SQL"
		}
		return fmt.Sprintf("invalid SQL: %s", r.Findings[0].Message)
	}
	if len(r.Findings) == 0 {
		return fmt.Sprintf("valid SQL (%d statements), no findings", r.StatementCount)
	}
	return fmt.Sprintf("valid SQL (%d statements), %d finding(s)", r.StatementCount, len(r.Findings))
}
