// Package sql provides SQL parsing and execution for XxSql.
package sql

// TokenType represents the type of a token.
type TokenType int

const (
	// Special tokens
	TokEOF TokenType = iota
	TokError

	// Keywords - DDL
	TokCreate
	TokTable
	TokDrop
	TokIndex
	TokPrimary
	TokKey
	TokUnique
	TokConstraint
	TokForeign
	TokReferences
	TokAlter
	TokAdd
	TokColumn
	TokRename
	TokModify
	TokDatabase
	TokSchema

	// Keywords - DML
	TokSelect
	TokFrom
	TokWhere
	TokInsert
	TokInto
	TokValues
	TokUpdate
	TokSet
	TokDelete

	// Keywords - Clauses
	TokAnd
	TokOr
	TokNot
	TokIn
	TokLike
	TokBetween
	TokIs
	TokNull
	TokDefault
	TokAs
	TokOn
	TokUsing
	TokDistinct

	// Keywords - JOIN
	TokJoin
	TokInner
	TokLeft
	TokRight
	TokCross
	TokOuter
	TokFull
	TokNatural

	// Keywords - Set operations
	TokUnion
	TokAll
	TokIntersect
	TokExcept

	// Keywords - GROUP BY / HAVING / ORDER BY
	TokGroup
	TokBy
	TokHaving
	TokOrder
	TokAsc
	TokDesc
	TokLimit
	TokOffset

	// Keywords - Data types
	TokSeq      // Auto-increment integer
	TokInt
	TokInteger
	TokBigInt
	TokSmallInt
	TokTinyInt
	TokFloat
	TokDouble
	TokDecimal
	TokNumeric
	TokChar
	TokVarchar
	TokText
	TokDate
	TokTime
	TokDateTime
	TokTimestamp
	TokBoolean
	TokBool
	TokBlob

	// Keywords - Functions
	TokCount
	TokSum
	TokAvg
	TokMin
	TokMax
	TokCoalesce
	TokNullIf
	TokCast
	TokCase
	TokWhen
	TokThen
	TokElse
	TokEnd

	// Keywords - Other
	TokIf
	TokExists
	TokAutoIncrement
	TokUnsigned
	TokZerofill
	TokCollate
	TokEngine
	TokCharset
	TokComment

	// Keywords - Privileges
	TokGrant
	TokRevoke
	TokPrivileges
	TokTo
	TokUse
	TokShow
	TokTruncate
	TokUser
	TokPassword
	TokIdentified
	TokRole
	TokGrants
	TokOption
	TokWith
	TokAt
	TokFor

	// Identifiers and literals
	TokIdent   // identifier
	TokNumber  // numeric literal
	TokString  // string literal
	TokBoolLit // boolean literal (TRUE/FALSE)

	// Operators
	TokEq      // =
	TokNe      // != or <>
	TokLt      // <
	TokLe      // <=
	TokGt      // >
	TokGe      // >=
	TokPlus    // +
	TokMinus   // -
	TokStar    // *
	TokSlash   // /
	TokPercent // %
	TokCaret   // ^
	TokConcat  // ||

	// Punctuation
	TokLParen    // (
	TokRParen    // )
	TokLBracket  // [
	TokRBracket  // ]
	TokLBrace    // {
	TokRBrace    // }
	TokComma     // ,
	TokSemi      // ;
	TokDot       // .
	TokColon     // :
	TokArrow     // ->
	TokDoubleCol // ::

	// Special
	TokParameter // ? or $1 for prepared statements
	TokAssign    // :=
)

// Token represents a lexical token.
type Token struct {
	Type   TokenType
	Value  string
	Line   int // 1-based line number
	Column int // 1-based column number
}

// String returns a string representation of the token type.
func (t TokenType) String() string {
	switch t {
	case TokEOF:
		return "EOF"
	case TokError:
		return "ERROR"
	case TokCreate:
		return "CREATE"
	case TokTable:
		return "TABLE"
	case TokDrop:
		return "DROP"
	case TokIndex:
		return "INDEX"
	case TokSelect:
		return "SELECT"
	case TokFrom:
		return "FROM"
	case TokWhere:
		return "WHERE"
	case TokInsert:
		return "INSERT"
	case TokInto:
		return "INTO"
	case TokValues:
		return "VALUES"
	case TokUpdate:
		return "UPDATE"
	case TokSet:
		return "SET"
	case TokDelete:
		return "DELETE"
	case TokAnd:
		return "AND"
	case TokOr:
		return "OR"
	case TokNot:
		return "NOT"
	case TokNull:
		return "NULL"
	case TokJoin:
		return "JOIN"
	case TokInner:
		return "INNER"
	case TokLeft:
		return "LEFT"
	case TokRight:
		return "RIGHT"
	case TokCross:
		return "CROSS"
	case TokOn:
		return "ON"
	case TokAs:
		return "AS"
	case TokUnion:
		return "UNION"
	case TokAll:
		return "ALL"
	case TokGroup:
		return "GROUP"
	case TokBy:
		return "BY"
	case TokHaving:
		return "HAVING"
	case TokOrder:
		return "ORDER"
	case TokAsc:
		return "ASC"
	case TokDesc:
		return "DESC"
	case TokLimit:
		return "LIMIT"
	case TokOffset:
		return "OFFSET"
	case TokSeq:
		return "SEQ"
	case TokInt, TokInteger:
		return "INT"
	case TokBigInt:
		return "BIGINT"
	case TokFloat:
		return "FLOAT"
	case TokDouble:
		return "DOUBLE"
	case TokChar:
		return "CHAR"
	case TokVarchar:
		return "VARCHAR"
	case TokText:
		return "TEXT"
	case TokDate:
		return "DATE"
	case TokTime:
		return "TIME"
	case TokDateTime, TokTimestamp:
		return "DATETIME"
	case TokBoolean, TokBool:
		return "BOOLEAN"
	case TokIdent:
		return "IDENT"
	case TokNumber:
		return "NUMBER"
	case TokString:
		return "STRING"
	case TokBoolLit:
		return "BOOL"
	case TokEq:
		return "="
	case TokNe:
		return "!="
	case TokLt:
		return "<"
	case TokLe:
		return "<="
	case TokGt:
		return ">"
	case TokGe:
		return ">="
	case TokPlus:
		return "+"
	case TokMinus:
		return "-"
	case TokStar:
		return "*"
	case TokSlash:
		return "/"
	case TokLParen:
		return "("
	case TokRParen:
		return ")"
	case TokComma:
		return ","
	case TokSemi:
		return ";"
	case TokDot:
		return "."
	case TokPrimary:
		return "PRIMARY"
	case TokKey:
		return "KEY"
	case TokUnique:
		return "UNIQUE"
	case TokDefault:
		return "DEFAULT"
	case TokIn:
		return "IN"
	case TokLike:
		return "LIKE"
	case TokBetween:
		return "BETWEEN"
	case TokIs:
		return "IS"
	case TokDistinct:
		return "DISTINCT"
	case TokCount:
		return "COUNT"
	case TokSum:
		return "SUM"
	case TokAvg:
		return "AVG"
	case TokMin:
		return "MIN"
	case TokMax:
		return "MAX"
	case TokCase:
		return "CASE"
	case TokWhen:
		return "WHEN"
	case TokThen:
		return "THEN"
	case TokElse:
		return "ELSE"
	case TokEnd:
		return "END"
	case TokCast:
		return "CAST"
	case TokCoalesce:
		return "COALESCE"
	case TokNullIf:
		return "NULLIF"
	case TokForeign:
		return "FOREIGN"
	case TokReferences:
		return "REFERENCES"
	case TokConstraint:
		return "CONSTRAINT"
	case TokAutoIncrement:
		return "AUTO_INCREMENT"
	case TokTruncate:
		return "TRUNCATE"
	case TokModify:
		return "MODIFY"
	case TokGrant:
		return "GRANT"
	case TokRevoke:
		return "REVOKE"
	case TokUser:
		return "USER"
	case TokPassword:
		return "PASSWORD"
	case TokIdentified:
		return "IDENTIFIED"
	case TokRole:
		return "ROLE"
	case TokGrants:
		return "GRANTS"
	case TokOption:
		return "OPTION"
	case TokWith:
		return "WITH"
	case TokAt:
		return "@"
	case TokFor:
		return "FOR"
	default:
		return "UNKNOWN"
	}
}

// keywords maps keyword strings to their token types.
var keywords = map[string]TokenType{
	"SELECT":       TokSelect,
	"FROM":         TokFrom,
	"WHERE":        TokWhere,
	"INSERT":       TokInsert,
	"INTO":         TokInto,
	"VALUES":       TokValues,
	"UPDATE":       TokUpdate,
	"SET":          TokSet,
	"DELETE":       TokDelete,
	"CREATE":       TokCreate,
	"TABLE":        TokTable,
	"DROP":         TokDrop,
	"INDEX":        TokIndex,
	"PRIMARY":      TokPrimary,
	"KEY":          TokKey,
	"UNIQUE":       TokUnique,
	"CONSTRAINT":   TokConstraint,
	"FOREIGN":      TokForeign,
	"REFERENCES":   TokReferences,
	"ALTER":        TokAlter,
	"ADD":          TokAdd,
	"COLUMN":       TokColumn,
	"RENAME":       TokRename,
	"MODIFY":       TokModify,
	"DATABASE":     TokDatabase,
	"SCHEMA":       TokSchema,
	"AND":          TokAnd,
	"OR":           TokOr,
	"NOT":          TokNot,
	"IN":           TokIn,
	"LIKE":         TokLike,
	"BETWEEN":      TokBetween,
	"IS":           TokIs,
	"NULL":         TokNull,
	"DEFAULT":      TokDefault,
	"AS":           TokAs,
	"ON":           TokOn,
	"USING":        TokUsing,
	"DISTINCT":     TokDistinct,
	"JOIN":         TokJoin,
	"INNER":        TokInner,
	"LEFT":         TokLeft,
	"RIGHT":        TokRight,
	"CROSS":        TokCross,
	"OUTER":        TokOuter,
	"FULL":         TokFull,
	"NATURAL":      TokNatural,
	"UNION":        TokUnion,
	"ALL":          TokAll,
	"INTERSECT":    TokIntersect,
	"EXCEPT":       TokExcept,
	"GROUP":        TokGroup,
	"BY":           TokBy,
	"HAVING":       TokHaving,
	"ORDER":        TokOrder,
	"ASC":          TokAsc,
	"DESC":         TokDesc,
	"LIMIT":        TokLimit,
	"OFFSET":       TokOffset,
	"SEQ":          TokSeq,
	"INT":          TokInt,
	"INTEGER":      TokInteger,
	"BIGINT":       TokBigInt,
	"SMALLINT":     TokSmallInt,
	"TINYINT":      TokTinyInt,
	"FLOAT":        TokFloat,
	"DOUBLE":       TokDouble,
	"DECIMAL":      TokDecimal,
	"NUMERIC":      TokNumeric,
	"CHAR":         TokChar,
	"VARCHAR":      TokVarchar,
	"TEXT":         TokText,
	"DATE":         TokDate,
	"TIME":         TokTime,
	"DATETIME":     TokDateTime,
	"TIMESTAMP":    TokTimestamp,
	"BOOLEAN":      TokBoolean,
	"BOOL":         TokBool,
	"BLOB":         TokBlob,
	"COUNT":        TokCount,
	"SUM":          TokSum,
	"AVG":          TokAvg,
	"MIN":          TokMin,
	"MAX":          TokMax,
	"COALESCE":     TokCoalesce,
	"NULLIF":       TokNullIf,
	"CAST":         TokCast,
	"CASE":         TokCase,
	"WHEN":         TokWhen,
	"THEN":         TokThen,
	"ELSE":         TokElse,
	"END":          TokEnd,
	"IF":           TokIf,
	"EXISTS":       TokExists,
	"AUTO_INCREMENT": TokAutoIncrement,
	"UNSIGNED":     TokUnsigned,
	"ZEROFILL":     TokZerofill,
	"COLLATE":      TokCollate,
	"ENGINE":       TokEngine,
	"CHARSET":      TokCharset,
	"COMMENT":      TokComment,
	"GRANT":        TokGrant,
	"REVOKE":       TokRevoke,
	"PRIVILEGES":   TokPrivileges,
	"TO":           TokTo,
	"USE":          TokUse,
	"SHOW":         TokShow,
	"TRUNCATE":     TokTruncate,
	"USER":         TokUser,
	"PASSWORD":     TokPassword,
	"IDENTIFIED":   TokIdentified,
	"ROLE":         TokRole,
	"GRANTS":       TokGrants,
	"OPTION":       TokOption,
	"WITH":         TokWith,
	"FOR":          TokFor,
	"TRUE":         TokBoolLit,
	"FALSE":        TokBoolLit,
}

// LookupKeyword looks up a keyword and returns its token type.
// Returns TokIdent if not a keyword.
func LookupKeyword(s string) TokenType {
	if t, ok := keywords[s]; ok {
		return t
	}
	return TokIdent
}