package lexer

// keywords maps lowercase SQL keywords to their token types.
// Uses a two-level lookup: first by length bucket, then by FNV hash
// for O(1) average-case performance with zero allocations.

// kwEntry is a keyword table entry.
type kwEntry struct {
	word string
	tok  TokenType
}

// Keywords organized by string length for fast dispatch.
// The lexer lowercases the candidate before lookup.
var keywordsByLen [32][]kwEntry

func init() {
	words := []kwEntry{
		{"add", ADD},
		{"after", AFTER},
		{"all", ALL},
		{"alter", ALTER},
		{"analyze", ANALYZE},
		{"and", AND},
		{"as", AS},
		{"asc", ASC},
		{"auto_increment", AUTO_INCREMENT},
		{"between", BETWEEN},
		{"bigint", BIGINT},
		{"binary", BINARY},
		{"blob", BLOB},
		{"boolean", BOOLEAN},
		{"by", BY},
		{"cascade", CASCADE},
		{"case", CASE},
		{"cast", CAST},
		{"change", CHANGE},
		{"char", CHAR},
		{"character", CHARACTER},
		{"check", CHECK},
		{"collate", COLLATE},
		{"column", COLUMN},
		{"comment", COMMENT_KW},
		{"constraint", CONSTRAINT},
		{"create", CREATE},
		{"cross", CROSS},
		{"database", DATABASE},
		{"date", DATE},
		{"datetime", DATETIME},
		{"decimal", DECIMAL},
		{"default", DEFAULT},
		{"deferrable", DEFERRABLE},
		{"deferred", DEFERRED},
		{"delete", DELETE},
		{"desc", DESC},
		{"distinct", DISTINCT},
		{"double", DOUBLE},
		{"drop", DROP},
		{"else", ELSE},
		{"end", END},
		{"engine", ENGINE},
		{"enum", ENUM},
		{"escape", ESCAPE},
		{"except", EXCEPT},
		{"exists", EXISTS},
		{"explain", EXPLAIN},
		{"false", FALSE_KW},
		{"first", FIRST},
		{"float", FLOAT_KW},
		{"for", FOR},
		{"foreign", FOREIGN},
		{"from", FROM},
		{"full", FULL},
		{"function", FUNCTION},
		{"group", GROUP},
		{"having", HAVING},
		{"if", IF},
		{"ignore", IGNORE},
		{"in", IN},
		{"index", INDEX},
		{"inner", INNER},
		{"insert", INSERT},
		{"int", INT_KW},
		{"integer", INTEGER},
		{"intersect", INTERSECT},
		{"into", INTO},
		{"is", IS},
		{"join", JOIN},
		{"json", JSON},
		{"jsonb", JSONB},
		{"key", KEY},
		{"last", LAST},
		{"left", LEFT},
		{"like", LIKE},
		{"limit", LIMIT},
		{"longblob", LONGBLOB},
		{"longtext", LONGTEXT},
		{"match", MATCH},
		{"mediumblob", MEDIUMBLOB},
		{"mediumint", MEDIUMINT},
		{"mediumtext", MEDIUMTEXT},
		{"natural", NATURAL},
		{"nchar", NCHAR},
		{"no", NO},
		{"not", NOT},
		{"null", NULL_KW},
		{"numeric", NUMERIC},
		{"offset", OFFSET},
		{"on", ON},
		{"or", OR},
		{"order", ORDER},
		{"outer", OUTER},
		{"partition", PARTITION},
		{"primary", PRIMARY},
		{"procedure", PROCEDURE},
		{"real", REAL},
		{"recursive", RECURSIVE},
		{"references", REFERENCES},
		{"rename", RENAME},
		{"replace", REPLACE},
		{"restrict", RESTRICT},
		{"right", RIGHT},
		{"rollback", ROLLBACK},
		{"select", SELECT},
		{"set", SET},
		{"show", SHOW},
		{"smallint", SMALLINT},
		{"table", TABLE},
		{"tables", TABLES},
		{"text", TEXT},
		{"then", THEN},
		{"time", TIME},
		{"timestamp", TIMESTAMP},
		{"tinyblob", TINYBLOB},
		{"tinyint", TINYINT},
		{"tinytext", TINYTEXT},
		{"to", TO},
		{"transaction", TRANSACTION},
		{"trigger", TRIGGER},
		{"true", TRUE_KW},
		{"truncate", TRUNCATE},
		{"union", UNION},
		{"unique", UNIQUE},
		{"update", UPDATE},
		{"use", USE},
		{"using", USING},
		{"values", VALUES},
		{"varbinary", VARBINARY},
		{"varchar", VARCHAR},
		{"view", VIEW},
		{"when", WHEN},
		{"where", WHERE},
		{"with", WITH},
		{"without", WITHOUT},
		{"year", YEAR},
	}
	for _, e := range words {
		l := len(e.word)
		if l < len(keywordsByLen) {
			keywordsByLen[l] = append(keywordsByLen[l], e)
		}
	}
}

// lookupKeyword returns the token for a keyword, or IDENT if not found.
// val must be lowercase. This function performs zero allocations.
func lookupKeyword(val []byte) TokenType {
	l := len(val)
	if l == 0 || l >= len(keywordsByLen) {
		return IDENT
	}
	bucket := keywordsByLen[l]
	for i := range bucket {
		if bytesEqualString(val, bucket[i].word) {
			return bucket[i].tok
		}
	}
	return IDENT
}

func bytesEqualString(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != s[i] {
			return false
		}
	}
	return true
}
