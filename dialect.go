package sqlparser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/oarkflow/sqlparser/ast"
	"github.com/oarkflow/sqlparser/lexer"
)

type Dialect string

const (
	DialectMySQL    Dialect = "mysql"
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type ConvertOptions struct {
	Target Dialect
	Strict bool
}

func ConvertDialect(sql string, target Dialect) (string, error) {
	return ConvertDialectWithOptions(sql, ConvertOptions{Target: target})
}

func ConvertDialectWithOptions(sql string, opts ConvertOptions) (string, error) {
	stmts, err := ParseStatements(sql)
	if err != nil {
		return "", err
	}
	r := &dialectRenderer{
		target: opts.Target,
		strict: opts.Strict,
	}
	return r.renderStatements(stmts)
}

type dialectRenderer struct {
	target     Dialect
	strict     bool
	paramIndex int
}

func (r *dialectRenderer) renderStatements(stmts []Statement) (string, error) {
	var b strings.Builder
	for i, stmt := range stmts {
		if i > 0 {
			b.WriteString("; ")
		}
		s, err := r.renderStatement(stmt)
		if err != nil {
			return "", err
		}
		b.WriteString(s)
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderStatement(stmt Statement) (string, error) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		return r.renderSelect(s)
	case *ast.InsertStmt:
		return r.renderInsert(s)
	case *ast.UpdateStmt:
		return r.renderUpdate(s)
	case *ast.DeleteStmt:
		return r.renderDelete(s)
	case *ast.CreateTableStmt:
		return r.renderCreateTable(s)
	case *ast.AlterTableStmt:
		return r.renderAlterTable(s)
	case *ast.DropTableStmt:
		return r.renderDropTable(s)
	case *ast.CreateIndexStmt:
		return r.renderCreateIndex(s)
	case *ast.DropIndexStmt:
		return r.renderDropIndex(s)
	case *ast.CreateViewStmt:
		return r.renderCreateView(s)
	case *ast.CreateDatabaseStmt:
		return r.renderCreateDatabase(s)
	case *ast.AlterDatabaseStmt:
		return r.renderAlterDatabase(s)
	case *ast.DropDatabaseStmt:
		return r.renderDropDatabase(s)
	case *ast.TruncateStmt:
		return "TRUNCATE TABLE " + r.renderQualifiedIdent(s.Table), nil
	case *ast.UseStmt:
		return "USE " + r.renderIdent(s.Database), nil
	case *ast.ShowStmt:
		return r.renderShow(s)
	case *ast.ExplainStmt:
		inner, err := r.renderStatement(s.Stmt)
		if err != nil {
			return "", err
		}
		return "EXPLAIN " + inner, nil
	case *ast.CallStmt:
		return r.renderCall(s)
	case *ast.TransactionStmt:
		return r.renderTx(s), nil
	case *ast.GenericDDLStmt:
		return r.renderGenericDDL(s), nil
	default:
		if r.strict {
			return "", fmt.Errorf("unsupported statement type %T", s)
		}
		return "", nil
	}
}

func (r *dialectRenderer) renderWith(w *ast.WithClause) string {
	if w == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString("WITH ")
	if w.Recursive {
		b.WriteString("RECURSIVE ")
	}
	for i, cte := range w.CTEs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderIdent(cte.Name))
		if len(cte.Columns) > 0 {
			b.WriteString(" (")
			for j, col := range cte.Columns {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(r.renderIdent(col))
			}
			b.WriteString(")")
		}
		sub, _ := r.renderSelect(cte.Subq)
		b.WriteString(" AS (")
		b.WriteString(sub)
		b.WriteByte(')')
	}
	b.WriteByte(' ')
	return b.String()
}

func (r *dialectRenderer) renderSelect(s *ast.SelectStmt) (string, error) {
	var b strings.Builder
	b.WriteString(r.renderWith(s.With))
	b.WriteString("SELECT ")
	if s.Distinct {
		b.WriteString("DISTINCT ")
	}
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		if c.Star {
			b.WriteByte('*')
		} else {
			b.WriteString(r.renderExpr(c.Expr))
		}
		if c.Alias != nil {
			b.WriteString(" AS ")
			b.WriteString(r.renderIdent(c.Alias))
		}
	}
	if len(s.From) > 0 {
		b.WriteString(" FROM ")
		for i, tr := range s.From {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderTableRef(tr))
		}
	}
	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(r.renderExpr(s.Where))
	}
	if len(s.GroupBy) > 0 {
		b.WriteString(" GROUP BY ")
		for i, e := range s.GroupBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderExpr(e))
		}
	}
	if s.Having != nil {
		b.WriteString(" HAVING ")
		b.WriteString(r.renderExpr(s.Having))
	}
	if len(s.OrderBy) > 0 {
		b.WriteString(" ORDER BY ")
		for i, it := range s.OrderBy {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderExpr(it.Expr))
			if it.Desc {
				b.WriteString(" DESC")
			} else {
				b.WriteString(" ASC")
			}
		}
	}
	if s.Limit != nil {
		b.WriteString(" LIMIT ")
		b.WriteString(r.renderExpr(s.Limit.Count))
		if s.Limit.Offset != nil {
			b.WriteString(" OFFSET ")
			b.WriteString(r.renderExpr(s.Limit.Offset))
		}
	}
	if s.SetOp != nil {
		cur := s.SetOp
		for cur != nil {
			b.WriteByte(' ')
			switch cur.Op {
			case ast.Union:
				b.WriteString("UNION")
			case ast.Intersect:
				b.WriteString("INTERSECT")
			case ast.Except:
				b.WriteString("EXCEPT")
			}
			if cur.All {
				b.WriteString(" ALL")
			}
			right, err := r.renderSelect(cur.Right)
			if err != nil {
				return "", err
			}
			b.WriteByte(' ')
			b.WriteString(right)
			cur = cur.Right.SetOp
		}
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderInsert(s *ast.InsertStmt) (string, error) {
	var b strings.Builder
	b.WriteString(r.renderWith(s.With))
	if s.Replace {
		b.WriteString("REPLACE INTO ")
	} else {
		b.WriteString("INSERT ")
		if s.Ignore && r.target == DialectMySQL {
			b.WriteString("IGNORE ")
		}
		b.WriteString("INTO ")
	}
	b.WriteString(r.renderQualifiedIdent(s.Table))
	if len(s.Columns) > 0 {
		b.WriteString(" (")
		for i, col := range s.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderIdent(col))
		}
		b.WriteString(")")
	}
	if len(s.Values) > 0 {
		b.WriteString(" VALUES ")
		for i, row := range s.Values {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteByte('(')
			for j, e := range row {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteString(r.renderExpr(e))
			}
			b.WriteByte(')')
		}
	} else if s.Select != nil {
		sel, err := r.renderSelect(s.Select)
		if err != nil {
			return "", err
		}
		b.WriteByte(' ')
		b.WriteString(sel)
	}
	switch r.target {
	case DialectMySQL:
		assign := s.OnDupKey
		if len(assign) == 0 {
			assign = s.OnConflictUpdate
		}
		if len(assign) > 0 {
			b.WriteString(" ON DUPLICATE KEY UPDATE ")
			for i, a := range assign {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(r.renderIdent(a.Column))
				b.WriteString(" = ")
				b.WriteString(r.renderExpr(a.Value))
			}
		}
	case DialectPostgres, DialectSQLite:
		target := s.OnConflictTarget
		assign := s.OnConflictUpdate
		doNothing := s.OnConflictDoNothing
		if len(assign) == 0 && len(s.OnDupKey) > 0 {
			assign = s.OnDupKey
		}
		if len(assign) > 0 || doNothing {
			if len(target) == 0 && len(assign) > 0 {
				if len(s.Columns) > 0 {
					target = []*ast.Ident{s.Columns[0]}
				} else if r.strict {
					return "", fmt.Errorf("cannot rewrite ON DUPLICATE KEY without conflict target")
				}
			}
			b.WriteString(" ON CONFLICT")
			if len(target) > 0 {
				b.WriteString(" (")
				for i, c := range target {
					if i > 0 {
						b.WriteString(", ")
					}
					b.WriteString(r.renderIdent(c))
				}
				b.WriteByte(')')
			}
			if doNothing && len(assign) == 0 {
				b.WriteString(" DO NOTHING")
			} else {
				b.WriteString(" DO UPDATE SET ")
				for i, a := range assign {
					if i > 0 {
						b.WriteString(", ")
					}
					b.WriteString(r.renderIdent(a.Column))
					b.WriteString(" = ")
					b.WriteString(r.renderExpr(a.Value))
				}
			}
		}
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderUpdate(s *ast.UpdateStmt) (string, error) {
	var b strings.Builder
	b.WriteString(r.renderWith(s.With))
	b.WriteString("UPDATE ")
	for i, tr := range s.Tables {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderTableRef(tr))
	}
	b.WriteString(" SET ")
	for i, a := range s.Set {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderIdent(a.Column))
		b.WriteString(" = ")
		b.WriteString(r.renderExpr(a.Value))
	}
	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(r.renderExpr(s.Where))
	}
	if len(s.Order) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.Order {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderExpr(o.Expr))
			if o.Desc {
				b.WriteString(" DESC")
			}
		}
	}
	if s.Limit != nil {
		b.WriteString(" LIMIT ")
		b.WriteString(r.renderExpr(s.Limit.Count))
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderDelete(s *ast.DeleteStmt) (string, error) {
	var b strings.Builder
	b.WriteString(r.renderWith(s.With))
	b.WriteString("DELETE FROM ")
	for i, tr := range s.From {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderTableRef(tr))
	}
	if s.Where != nil {
		b.WriteString(" WHERE ")
		b.WriteString(r.renderExpr(s.Where))
	}
	if len(s.Order) > 0 {
		b.WriteString(" ORDER BY ")
		for i, o := range s.Order {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderExpr(o.Expr))
			if o.Desc {
				b.WriteString(" DESC")
			}
		}
	}
	if s.Limit != nil {
		b.WriteString(" LIMIT ")
		b.WriteString(r.renderExpr(s.Limit.Count))
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderCreateTable(s *ast.CreateTableStmt) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	if s.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(r.renderQualifiedIdent(s.Table))
	if s.Like != nil {
		b.WriteString(" LIKE ")
		b.WriteString(r.renderQualifiedIdent(s.Like))
		return b.String(), nil
	}
	if len(s.Columns) > 0 || len(s.Constraints) > 0 {
		b.WriteString(" (")
		wrote := false
		for _, col := range s.Columns {
			if wrote {
				b.WriteString(", ")
			}
			wrote = true
			b.WriteString(r.renderColumnDef(col))
		}
		for _, c := range s.Constraints {
			if wrote {
				b.WriteString(", ")
			}
			wrote = true
			b.WriteString(r.renderConstraint(c))
		}
		b.WriteByte(')')
	}
	for _, opt := range s.Options {
		b.WriteByte(' ')
		b.WriteString(string(opt.Key))
		if len(opt.Value) > 0 {
			b.WriteByte('=')
			b.WriteString(string(opt.Value))
		}
	}
	if s.Select != nil {
		sel, err := r.renderSelect(s.Select)
		if err != nil {
			return "", err
		}
		b.WriteString(" AS ")
		b.WriteString(sel)
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderAlterTable(s *ast.AlterTableStmt) (string, error) {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(r.renderQualifiedIdent(s.Table))
	for i, cmd := range s.Cmds {
		if i == 0 {
			b.WriteByte(' ')
		} else {
			b.WriteString(", ")
		}
		b.WriteString(r.renderAlterCmd(cmd))
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderDropTable(s *ast.DropTableStmt) (string, error) {
	var b strings.Builder
	b.WriteString("DROP TABLE ")
	if s.IfExists {
		b.WriteString("IF EXISTS ")
	}
	for i, t := range s.Tables {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderQualifiedIdent(t))
	}
	if s.Cascade {
		b.WriteString(" CASCADE")
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderCreateIndex(s *ast.CreateIndexStmt) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE ")
	if s.Type == ast.UniqueConstraint {
		b.WriteString("UNIQUE ")
	}
	b.WriteString("INDEX ")
	b.WriteString(r.renderIdent(s.Name))
	b.WriteString(" ON ")
	b.WriteString(r.renderQualifiedIdent(s.Table))
	b.WriteString(" (")
	for i, c := range s.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderIdent(c.Name))
		if c.Length != nil {
			b.WriteByte('(')
			b.WriteString(strconv.Itoa(*c.Length))
			b.WriteByte(')')
		}
		if c.Desc {
			b.WriteString(" DESC")
		}
	}
	b.WriteByte(')')
	return b.String(), nil
}

func (r *dialectRenderer) renderDropIndex(s *ast.DropIndexStmt) (string, error) {
	if r.target == DialectPostgres || r.target == DialectSQLite {
		out := "DROP INDEX "
		if s.IfExists {
			out += "IF EXISTS "
		}
		return out + r.renderIdent(s.Name), nil
	}
	out := "DROP INDEX " + r.renderIdent(s.Name)
	if s.Table != nil {
		out += " ON " + r.renderQualifiedIdent(s.Table)
	}
	return out, nil
}

func (r *dialectRenderer) renderCreateView(s *ast.CreateViewStmt) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE ")
	if s.OrReplace {
		b.WriteString("OR REPLACE ")
	}
	b.WriteString("VIEW ")
	b.WriteString(r.renderQualifiedIdent(s.Name))
	if len(s.Columns) > 0 {
		b.WriteString(" (")
		for i, c := range s.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderIdent(c))
		}
		b.WriteByte(')')
	}
	sel, err := r.renderSelect(s.Select)
	if err != nil {
		return "", err
	}
	b.WriteString(" AS ")
	b.WriteString(sel)
	return b.String(), nil
}

func (r *dialectRenderer) renderCreateDatabase(s *ast.CreateDatabaseStmt) (string, error) {
	var b strings.Builder
	b.WriteString("CREATE DATABASE ")
	if s.IfNotExists {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(r.renderIdent(s.Name))
	for _, opt := range s.Options {
		b.WriteByte(' ')
		b.WriteString(string(opt.Key))
		if len(opt.Value) > 0 {
			b.WriteByte('=')
			b.WriteString(string(opt.Value))
		}
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderAlterDatabase(s *ast.AlterDatabaseStmt) (string, error) {
	var b strings.Builder
	b.WriteString("ALTER DATABASE ")
	b.WriteString(r.renderIdent(s.Name))
	for _, opt := range s.Options {
		b.WriteByte(' ')
		b.WriteString(string(opt.Key))
		if len(opt.Value) > 0 {
			b.WriteByte('=')
			b.WriteString(string(opt.Value))
		}
	}
	return b.String(), nil
}

func (r *dialectRenderer) renderDropDatabase(s *ast.DropDatabaseStmt) (string, error) {
	out := "DROP DATABASE "
	if s.IfExists {
		out += "IF EXISTS "
	}
	return out + r.renderIdent(s.Name), nil
}

func (r *dialectRenderer) renderShow(s *ast.ShowStmt) (string, error) {
	out := "SHOW " + string(s.What)
	if s.Like != nil {
		out += " LIKE " + r.renderExpr(s.Like)
	}
	if s.Where != nil {
		out += " WHERE " + r.renderExpr(s.Where)
	}
	return out, nil
}

func (r *dialectRenderer) renderCall(s *ast.CallStmt) (string, error) {
	var b strings.Builder
	b.WriteString("CALL ")
	b.WriteString(r.renderQualifiedIdent(s.Name))
	b.WriteByte('(')
	for i, a := range s.Args {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(r.renderExpr(a))
	}
	b.WriteByte(')')
	return b.String(), nil
}

func (r *dialectRenderer) renderTx(s *ast.TransactionStmt) string {
	switch string(s.Action) {
	case "begin":
		return "BEGIN"
	case "commit":
		return "COMMIT"
	case "rollback":
		if s.Savepoint == nil {
			return "ROLLBACK"
		}
		return "ROLLBACK TO SAVEPOINT " + r.renderIdent(s.Savepoint)
	case "start_transaction":
		out := "START TRANSACTION"
		for _, o := range s.Options {
			out += " " + string(o)
		}
		return out
	case "savepoint":
		return "SAVEPOINT " + r.renderIdent(s.Savepoint)
	case "release_savepoint":
		return "RELEASE SAVEPOINT " + r.renderIdent(s.Savepoint)
	case "set_transaction":
		out := "SET TRANSACTION"
		for _, o := range s.Options {
			out += " " + string(o)
		}
		return out
	default:
		return strings.ToUpper(string(s.Action))
	}
}

func (r *dialectRenderer) renderGenericDDL(s *ast.GenericDDLStmt) string {
	out := strings.ToUpper(string(s.Verb)) + " " + strings.ToUpper(string(s.Object))
	if s.Name != nil {
		out += " " + r.renderIdent(s.Name)
	}
	return out
}

func (r *dialectRenderer) renderColumnDef(c *ast.ColumnDef) string {
	var b strings.Builder
	b.WriteString(r.renderIdent(c.Name))
	if c.Type != nil {
		b.WriteByte(' ')
		b.WriteString(r.renderDataType(c.Type))
	}
	if c.NotNull {
		b.WriteString(" NOT NULL")
	}
	if c.Default != nil {
		b.WriteString(" DEFAULT ")
		b.WriteString(r.renderExpr(c.Default))
	}
	if c.AutoIncrement {
		if r.target == DialectPostgres {
			// keep conservative and dialect-safe without mutating type inference
			b.WriteString(" GENERATED BY DEFAULT AS IDENTITY")
		} else {
			b.WriteString(" AUTO_INCREMENT")
		}
	}
	if c.PrimaryKey {
		b.WriteString(" PRIMARY KEY")
	}
	if c.Unique {
		b.WriteString(" UNIQUE")
	}
	if c.Comment != nil {
		b.WriteString(" COMMENT ")
		b.WriteString(r.renderExpr(c.Comment))
	}
	return b.String()
}

func (r *dialectRenderer) renderDataType(dt *ast.DataType) string {
	name := string(dt.Name)
	switch {
	case strings.EqualFold(name, "jsonb"):
		if r.target == DialectMySQL {
			name = "JSON"
		}
		if r.target == DialectSQLite {
			name = "TEXT"
		}
	case strings.EqualFold(name, "json"):
		if r.target == DialectSQLite {
			name = "TEXT"
		}
	}
	var b strings.Builder
	b.WriteString(name)
	if dt.Precision > 0 {
		b.WriteByte('(')
		b.WriteString(strconv.Itoa(dt.Precision))
		if dt.Scale > 0 {
			b.WriteByte(',')
			b.WriteString(strconv.Itoa(dt.Scale))
		}
		b.WriteByte(')')
	}
	if dt.Unsigned && r.target == DialectMySQL {
		b.WriteString(" UNSIGNED")
	}
	if dt.Zerofill && r.target == DialectMySQL {
		b.WriteString(" ZEROFILL")
	}
	return b.String()
}

func (r *dialectRenderer) renderConstraint(c *ast.TableConstraint) string {
	var b strings.Builder
	if c.Name != nil {
		b.WriteString("CONSTRAINT ")
		b.WriteString(r.renderIdent(c.Name))
		b.WriteByte(' ')
	}
	switch c.Type {
	case ast.PrimaryKeyConstraint:
		b.WriteString("PRIMARY KEY")
	case ast.UniqueConstraint:
		b.WriteString("UNIQUE")
	case ast.IndexConstraint:
		b.WriteString("INDEX")
	case ast.ForeignKeyConstraint:
		b.WriteString("FOREIGN KEY")
	case ast.CheckConstraint:
		b.WriteString("CHECK")
	}
	if len(c.Columns) > 0 {
		b.WriteString(" (")
		for i, col := range c.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(r.renderIdent(col.Name))
		}
		b.WriteByte(')')
	}
	if c.RefTable != nil {
		b.WriteString(" REFERENCES ")
		b.WriteString(r.renderQualifiedIdent(c.RefTable))
		if len(c.RefCols) > 0 {
			b.WriteString(" (")
			for i, col := range c.RefCols {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(r.renderIdent(col))
			}
			b.WriteByte(')')
		}
	}
	return b.String()
}

func (r *dialectRenderer) renderAlterCmd(cmd ast.AlterCmd) string {
	switch c := cmd.(type) {
	case *ast.AddColumnCmd:
		out := "ADD COLUMN " + r.renderColumnDef(c.Col)
		if c.First {
			out += " FIRST"
		}
		if c.After != nil {
			out += " AFTER " + r.renderIdent(c.After)
		}
		return out
	case *ast.DropColumnCmd:
		return "DROP COLUMN " + r.renderIdent(c.Name)
	case *ast.ModifyColumnCmd:
		out := "MODIFY COLUMN " + r.renderColumnDef(c.Col)
		if c.First {
			out += " FIRST"
		}
		if c.After != nil {
			out += " AFTER " + r.renderIdent(c.After)
		}
		return out
	case *ast.AddConstraintCmd:
		return "ADD " + r.renderConstraint(c.Constraint)
	case *ast.DropIndexCmd:
		return "DROP INDEX " + r.renderIdent(c.Name)
	case *ast.RenameTableCmd:
		return "RENAME TO " + r.renderQualifiedIdent(c.NewName)
	default:
		return ""
	}
}

func (r *dialectRenderer) renderTableRef(tr ast.TableRef) string {
	switch t := tr.(type) {
	case *ast.SimpleTable:
		out := r.renderQualifiedIdent(t.Name)
		if t.Alias != nil {
			out += " " + r.renderIdent(t.Alias)
		}
		return out
	case *ast.SubqueryTable:
		sub, _ := r.renderSelect(t.Subq)
		out := "(" + sub + ")"
		if t.Alias != nil {
			out += " " + r.renderIdent(t.Alias)
		}
		return out
	case *ast.JoinTable:
		out := r.renderTableRef(t.Left) + " "
		switch t.Kind {
		case ast.InnerJoin:
			out += "JOIN "
		case ast.LeftJoin:
			out += "LEFT JOIN "
		case ast.RightJoin:
			out += "RIGHT JOIN "
		case ast.FullJoin:
			out += "FULL JOIN "
		case ast.CrossJoin:
			out += "CROSS JOIN "
		case ast.NaturalJoin:
			out += "NATURAL JOIN "
		}
		out += r.renderTableRef(t.Right)
		if t.On != nil {
			out += " ON " + r.renderExpr(t.On)
		}
		if len(t.Using) > 0 {
			out += " USING ("
			for i, id := range t.Using {
				if i > 0 {
					out += ", "
				}
				out += r.renderIdent(id)
			}
			out += ")"
		}
		return out
	default:
		return ""
	}
}

func (r *dialectRenderer) renderExpr(expr Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return r.renderIdent(e)
	case *ast.QualifiedIdent:
		return r.renderQualifiedIdent(e)
	case *ast.StarExpr:
		return "*"
	case *ast.Literal:
		return string(e.Raw)
	case *ast.NullLit:
		return "NULL"
	case *ast.Param:
		return r.renderParam(e.Raw)
	case *ast.BinaryExpr:
		return "(" + r.renderExpr(e.Left) + " " + r.opString(e.Op) + " " + r.renderExpr(e.Right) + ")"
	case *ast.UnaryExpr:
		return "(" + r.opString(e.Op) + " " + r.renderExpr(e.Expr) + ")"
	case *ast.FuncCall:
		var b strings.Builder
		b.WriteString(r.renderFunctionName(e.Name))
		b.WriteByte('(')
		if e.Star {
			b.WriteByte('*')
		} else {
			if e.Distinct {
				b.WriteString("DISTINCT ")
			}
			for i, a := range e.Args {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(r.renderExpr(a))
			}
		}
		b.WriteByte(')')
		return b.String()
	case *ast.CaseExpr:
		var b strings.Builder
		b.WriteString("CASE")
		if e.Operand != nil {
			b.WriteByte(' ')
			b.WriteString(r.renderExpr(e.Operand))
		}
		for _, w := range e.Whens {
			b.WriteString(" WHEN ")
			b.WriteString(r.renderExpr(w.Cond))
			b.WriteString(" THEN ")
			b.WriteString(r.renderExpr(w.Result))
		}
		if e.Else != nil {
			b.WriteString(" ELSE ")
			b.WriteString(r.renderExpr(e.Else))
		}
		b.WriteString(" END")
		return b.String()
	case *ast.BetweenExpr:
		out := r.renderExpr(e.Expr)
		if e.Not {
			out += " NOT"
		}
		out += " BETWEEN " + r.renderExpr(e.Lo) + " AND " + r.renderExpr(e.Hi)
		return out
	case *ast.InExpr:
		out := r.renderExpr(e.Expr)
		if e.Not {
			out += " NOT"
		}
		out += " IN ("
		if e.Subq != nil {
			sub, _ := r.renderSelect(e.Subq)
			out += sub
		} else {
			for i, it := range e.List {
				if i > 0 {
					out += ", "
				}
				out += r.renderExpr(it)
			}
		}
		out += ")"
		return out
	case *ast.LikeExpr:
		out := r.renderExpr(e.Expr)
		if e.Not {
			out += " NOT"
		}
		out += " LIKE " + r.renderExpr(e.Pattern)
		if e.Escape != nil {
			out += " ESCAPE " + r.renderExpr(e.Escape)
		}
		return out
	case *ast.IsNullExpr:
		out := r.renderExpr(e.Expr) + " IS "
		if e.Not {
			out += "NOT "
		}
		return out + "NULL"
	case *ast.ExistsExpr:
		sub, _ := r.renderSelect(e.Subq)
		pfx := ""
		if e.Not {
			pfx = "NOT "
		}
		return pfx + "EXISTS (" + sub + ")"
	case *ast.SubqueryExpr:
		sub, _ := r.renderSelect(e.Subq)
		return "(" + sub + ")"
	case *ast.CastExpr:
		return "CAST(" + r.renderExpr(e.Expr) + " AS " + r.renderDataType(e.Type) + ")"
	case *ast.SelectStmt:
		s, _ := r.renderSelect(e)
		return "(" + s + ")"
	default:
		return ""
	}
}

func (r *dialectRenderer) renderFunctionName(name *ast.QualifiedIdent) string {
	if name == nil || len(name.Parts) == 0 {
		return ""
	}
	if len(name.Parts) == 1 {
		fn := strings.ToUpper(name.Parts[0].Unquoted)
		switch r.target {
		case DialectPostgres, DialectSQLite:
			if fn == "IFNULL" {
				return "COALESCE"
			}
		case DialectMySQL:
			if fn == "COALESCE" {
				return "IFNULL"
			}
		}
		return fn
	}
	return r.renderQualifiedIdent(name)
}

func (r *dialectRenderer) renderParam(raw []byte) string {
	if r.target == DialectPostgres {
		r.paramIndex++
		return "$" + strconv.Itoa(r.paramIndex)
	}
	return "?"
}

func (r *dialectRenderer) opString(op lexer.TokenType) string {
	switch op {
	case lexer.PLUS:
		return "+"
	case lexer.MINUS:
		return "-"
	case lexer.STAR:
		return "*"
	case lexer.SLASH:
		return "/"
	case lexer.PERCENT:
		return "%"
	case lexer.AND, lexer.DAMP:
		return "AND"
	case lexer.OR:
		return "OR"
	case lexer.NOT:
		return "NOT"
	case lexer.EQ:
		return "="
	case lexer.NEQ:
		return "!="
	case lexer.LT:
		return "<"
	case lexer.GT:
		return ">"
	case lexer.LTE:
		return "<="
	case lexer.GTE:
		return ">="
	case lexer.LSHIFT:
		return "<<"
	case lexer.RSHIFT:
		return ">>"
	case lexer.DBAR:
		return "||"
	case lexer.PIPE:
		return "|"
	case lexer.CARET:
		return "^"
	case lexer.AMPERSAND:
		return "&"
	case lexer.ARROW:
		return "->"
	case lexer.DARROW2:
		return "->>"
	case lexer.HASHARROW:
		return "#>"
	case lexer.HASHDARROW:
		return "#>>"
	case lexer.ATGT:
		return "@>"
	case lexer.LTAT:
		return "<@"
	case lexer.QUESTION:
		return "?"
	case lexer.QMARKPIPE:
		return "?|"
	case lexer.QMARKAMP:
		return "?&"
	default:
		return string(op.String())
	}
}

func (r *dialectRenderer) renderQualifiedIdent(q *ast.QualifiedIdent) string {
	if q == nil {
		return ""
	}
	var b strings.Builder
	for i, p := range q.Parts {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(r.renderIdent(p))
	}
	return b.String()
}

func (r *dialectRenderer) renderIdent(id *ast.Ident) string {
	if id == nil {
		return ""
	}
	name := id.Unquoted
	if name == "*" {
		return "*"
	}
	switch r.target {
	case DialectMySQL:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}
