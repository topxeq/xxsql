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
	TokView
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

	// Keywords - Bulk Import/Export
	TokCopy
	TokLoad
	TokData
	TokInfile
	TokFields
	TokTerminated
	TokLines
	TokEnclosed
	TokEscaped
	TokOptionally

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
	TokGlob
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
	TokOf
	TokNulls    // NULLS FIRST/LAST
	TokFirst    // NULLS FIRST
	TokLast     // NULLS LAST
	TokFilter   // FILTER (WHERE ...)

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

	// Keywords - JSON Functions
	TokJsonExtract
	TokJsonArray
	TokJsonObject
	TokJsonType
	TokJsonValid
	TokJsonQuote
	TokJsonUnquote
	TokJsonContains
	TokJsonKeys
	TokJsonLength

	// Keywords - Other
	TokIf
	TokExists
	TokAny
	TokAutoIncrement
	TokUnsigned
	TokZerofill
	TokCollate
	TokEngine
	TokCharset
	TokComment
	TokCheck
	TokCascade
	TokRestrict
	TokAction
	TokDescribe
	TokBackup
	TokRestore

	// Keywords - Privileges
	TokGrant
	TokRevoke
	TokPrivileges
	TokTo
	TokUse
	TokShow
	TokTruncate
	TokVacuum
	TokUser
	TokPassword
	TokIdentified
	TokRole
	TokGrants
	TokOption
	TokWith
	TokRecursive
	TokAt
	TokFor

	// Keywords - Window Functions
	TokOver
	TokPartition
	TokWindow
	TokRows
	TokRange
	TokPreceding
	TokFollowing
	TokCurrent
	TokLead       // LEAD window function
	TokLag        // LAG window function
	TokNtile      // NTILE window function
	TokFirstValue // FIRST_VALUE window function
	TokLastValue  // LAST_VALUE window function
	TokNthValue   // NTH_VALUE window function
	TokPercentRank // PERCENT_RANK window function
	TokCumeDist   // CUME_DIST window function
	TokIgnore     // IGNORE NULLS
	TokRespect    // RESPECT NULLS
	TokUnbounded  // UNBOUNDED PRECEDING/FOLLOWING
	TokFromFirst  // FROM FIRST (for NTH_VALUE)
	TokFromLast   // FROM LAST (for NTH_VALUE)

	// Keywords - LATERAL
	TokLateral // LATERAL for correlated subqueries

	// Keywords - UDF
	TokFunction
	TokReturns
	TokReturn
	TokReplace
	TokLet
	TokBegin

	// Keywords - Transaction
	TokCommit
	TokRollback
	TokTransaction
	TokSavepoint
	TokRelease
	TokWork // optional keyword in COMMIT/ROLLBACK

	// Keywords - Trigger
	TokTrigger
	TokBefore
	TokAfter
	TokInstead
	TokEach
	TokRow
	TokStatement

	// Keywords - UPSERT and RETURNING
	TokConflict
	TokDo
	TokNothing
	TokReturning

	// Keywords - EXPLAIN
	TokExplain
	TokQuery
	TokPlan

	// Keywords - Generated Columns
	TokGenerated
	TokAlways
	TokVirtual
	TokStored

	// Keywords - Full-Text Search
	TokMatch     // MATCH
	TokFts       // FTS
	TokRank      // RANK
	TokTokenizer // TOKENIZER

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
	case TokView:
		return "VIEW"
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
	case TokCopy:
		return "COPY"
	case TokLoad:
		return "LOAD"
	case TokData:
		return "DATA"
	case TokInfile:
		return "INFILE"
	case TokFields:
		return "FIELDS"
	case TokTerminated:
		return "TERMINATED"
	case TokLines:
		return "LINES"
	case TokEnclosed:
		return "ENCLOSED"
	case TokEscaped:
		return "ESCAPED"
	case TokOptionally:
		return "OPTIONALLY"
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
	case TokOf:
		return "OF"
	case TokNulls:
		return "NULLS"
	case TokFirst:
		return "FIRST"
	case TokLast:
		return "LAST"
	case TokFilter:
		return "FILTER"
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
	case TokBlob:
		return "BLOB"
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
	case TokGlob:
		return "GLOB"
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
	case TokJsonExtract:
		return "JSON_EXTRACT"
	case TokJsonArray:
		return "JSON_ARRAY"
	case TokJsonObject:
		return "JSON_OBJECT"
	case TokJsonType:
		return "JSON_TYPE"
	case TokJsonValid:
		return "JSON_VALID"
	case TokJsonQuote:
		return "JSON_QUOTE"
	case TokJsonUnquote:
		return "JSON_UNQUOTE"
	case TokJsonContains:
		return "JSON_CONTAINS"
	case TokJsonKeys:
		return "JSON_KEYS"
	case TokJsonLength:
		return "JSON_LENGTH"
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
	case TokVacuum:
		return "VACUUM"
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
	case TokRecursive:
		return "RECURSIVE"
	case TokAt:
		return "@"
	case TokFor:
		return "FOR"
	case TokOver:
		return "OVER"
	case TokPartition:
		return "PARTITION"
	case TokWindow:
		return "WINDOW"
	case TokRows:
		return "ROWS"
	case TokRange:
		return "RANGE"
	case TokPreceding:
		return "PRECEDING"
	case TokFollowing:
		return "FOLLOWING"
	case TokCurrent:
		return "CURRENT"
	case TokLead:
		return "LEAD"
	case TokLag:
		return "LAG"
	case TokNtile:
		return "NTILE"
	case TokFirstValue:
		return "FIRST_VALUE"
	case TokLastValue:
		return "LAST_VALUE"
	case TokIgnore:
		return "IGNORE"
	case TokRespect:
		return "RESPECT"
	case TokUnbounded:
		return "UNBOUNDED"
	case TokNthValue:
		return "NTH_VALUE"
	case TokPercentRank:
		return "PERCENT_RANK"
	case TokCumeDist:
		return "CUME_DIST"
	case TokFromFirst:
		return "FROM FIRST"
	case TokFromLast:
		return "FROM LAST"
	case TokLateral:
		return "LATERAL"
	case TokFunction:
		return "FUNCTION"
	case TokReturns:
		return "RETURNS"
	case TokReturn:
		return "RETURN"
	case TokReplace:
		return "REPLACE"
	case TokLet:
		return "LET"
	case TokBegin:
		return "BEGIN"
	case TokCommit:
		return "COMMIT"
	case TokRollback:
		return "ROLLBACK"
	case TokTransaction:
		return "TRANSACTION"
	case TokSavepoint:
		return "SAVEPOINT"
	case TokRelease:
		return "RELEASE"
	case TokWork:
		return "WORK"
	case TokTrigger:
		return "TRIGGER"
	case TokBefore:
		return "BEFORE"
	case TokAfter:
		return "AFTER"
	case TokInstead:
		return "INSTEAD"
	case TokEach:
		return "EACH"
	case TokRow:
		return "ROW"
	case TokStatement:
		return "STATEMENT"
	case TokConflict:
		return "CONFLICT"
	case TokDo:
		return "DO"
	case TokNothing:
		return "NOTHING"
	case TokReturning:
		return "RETURNING"
	case TokExplain:
		return "EXPLAIN"
	case TokQuery:
		return "QUERY"
	case TokPlan:
		return "PLAN"
	case TokGenerated:
		return "GENERATED"
	case TokAlways:
		return "ALWAYS"
	case TokVirtual:
		return "VIRTUAL"
	case TokStored:
		return "STORED"
	case TokMatch:
		return "MATCH"
	case TokFts:
		return "FTS"
	case TokRank:
		return "RANK"
	case TokTokenizer:
		return "TOKENIZER"
	case TokDescribe:
		return "DESCRIBE"
	case TokBackup:
		return "BACKUP"
	case TokRestore:
		return "RESTORE"
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
	"VIEW":         TokView,
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
	"COPY":         TokCopy,
	"LOAD":         TokLoad,
	"DATA":         TokData,
	"INFILE":       TokInfile,
	"FIELDS":       TokFields,
	"TERMINATED":   TokTerminated,
	"LINES":        TokLines,
	"ENCLOSED":     TokEnclosed,
	"ESCAPED":      TokEscaped,
	"OPTIONALLY":   TokOptionally,
	"AND":          TokAnd,
	"OR":           TokOr,
	"NOT":          TokNot,
	"IN":           TokIn,
	"LIKE":         TokLike,
	"GLOB":         TokGlob,
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
	"OF":           TokOf,
	"NULLS":        TokNulls,
	"FIRST":        TokFirst,
	"LAST":         TokLast,
	"FILTER":       TokFilter,
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
	"JSON_EXTRACT":  TokJsonExtract,
	"JSON_ARRAY":    TokJsonArray,
	"JSON_OBJECT":   TokJsonObject,
	"JSON_TYPE":     TokJsonType,
	"JSON_VALID":    TokJsonValid,
	"JSON_QUOTE":    TokJsonQuote,
	"JSON_UNQUOTE":  TokJsonUnquote,
	"JSON_CONTAINS": TokJsonContains,
	"JSON_KEYS":     TokJsonKeys,
	"JSON_LENGTH":   TokJsonLength,
	"IF":           TokIf,
	"EXISTS":       TokExists,
	"ANY":          TokAny,
	"AUTO_INCREMENT": TokAutoIncrement,
	"UNSIGNED":     TokUnsigned,
	"ZEROFILL":     TokZerofill,
	"COLLATE":      TokCollate,
	"ENGINE":       TokEngine,
	"CHARSET":      TokCharset,
	"COMMENT":      TokComment,
	"CHECK":        TokCheck,
	"CASCADE":      TokCascade,
	"RESTRICT":     TokRestrict,
	"ACTION":       TokAction,
	"DESCRIBE":     TokDescribe,
	"BACKUP":       TokBackup,
	"RESTORE":      TokRestore,
	"GRANT":        TokGrant,
	"REVOKE":       TokRevoke,
	"PRIVILEGES":   TokPrivileges,
	"TO":           TokTo,
	"USE":          TokUse,
	"SHOW":         TokShow,
	"TRUNCATE":     TokTruncate,
	"VACUUM":       TokVacuum,
	"USER":         TokUser,
	"PASSWORD":     TokPassword,
	"IDENTIFIED":   TokIdentified,
	"ROLE":         TokRole,
	"GRANTS":       TokGrants,
	"OPTION":       TokOption,
	"WITH":         TokWith,
	"RECURSIVE":    TokRecursive,
	"FOR":          TokFor,
	"OVER":         TokOver,
	"PARTITION":    TokPartition,
	"WINDOW":       TokWindow,
	"ROWS":         TokRows,
	"RANGE":        TokRange,
	"PRECEDING":    TokPreceding,
	"FOLLOWING":    TokFollowing,
	"CURRENT":      TokCurrent,
	"LEAD":         TokLead,
	"LAG":          TokLag,
	"NTILE":        TokNtile,
	"FIRST_VALUE":  TokFirstValue,
	"LAST_VALUE":   TokLastValue,
	"IGNORE":       TokIgnore,
	"RESPECT":      TokRespect,
	"UNBOUNDED":    TokUnbounded,
	"NTH_VALUE":    TokNthValue,
	"PERCENT_RANK": TokPercentRank,
	"CUME_DIST":    TokCumeDist,
	"LATERAL":      TokLateral,
	"FUNCTION":     TokFunction,
	"RETURNS":      TokReturns,
	"RETURN":       TokReturn,
	"REPLACE":      TokReplace,
	"LET":          TokLet,
	"BEGIN":        TokBegin,
	"COMMIT":       TokCommit,
	"ROLLBACK":     TokRollback,
	"TRANSACTION":  TokTransaction,
	"SAVEPOINT":    TokSavepoint,
	"RELEASE":      TokRelease,
	"WORK":         TokWork,
	"TRIGGER":      TokTrigger,
	"BEFORE":       TokBefore,
	"AFTER":        TokAfter,
	"INSTEAD":      TokInstead,
	"EACH":         TokEach,
	"ROW":          TokRow,
	"STATEMENT":    TokStatement,
	"CONFLICT":     TokConflict,
	"DO":           TokDo,
	"NOTHING":      TokNothing,
	"RETURNING":    TokReturning,
	"EXPLAIN":      TokExplain,
	"QUERY":        TokQuery,
	"PLAN":         TokPlan,
	"GENERATED":    TokGenerated,
	"ALWAYS":       TokAlways,
	"VIRTUAL":      TokVirtual,
	"STORED":       TokStored,
	"MATCH":        TokMatch,
	"FTS":          TokFts,
	"RANK":         TokRank,
	"TOKENIZER":    TokTokenizer,
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