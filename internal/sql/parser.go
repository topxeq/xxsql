package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses SQL statements.
type Parser struct {
	lexer     *Lexer
	currTok   Token
	peekTok   Token
	err       error
	errLine   int
	errColumn int
}

// NewParser creates a new parser for the given input.
func NewParser(input string) *Parser {
	l := NewLexer(input)
	p := &Parser{
		lexer: l,
	}
	// Prime the parser
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses the input and returns a statement.
func (p *Parser) Parse() (Statement, error) {
	stmt := p.parseStatement()
	if p.err != nil {
		return nil, fmt.Errorf("parse error at line %d, column %d: %v", p.errLine, p.errColumn, p.err)
	}
	return stmt, nil
}

// ParseExpression parses a single expression and returns it.
// This is useful for parsing generated column expressions.
func (p *Parser) ParseExpression() Expression {
	return p.parseExpression()
}

// Error returns any parse error that occurred.
func (p *Parser) Error() error {
	return p.err
}

// ParseAll parses all statements in the input.
func ParseAll(input string) ([]Statement, error) {
	p := NewParser(input)
	var stmts []Statement

	for p.currTok.Type != TokEOF {
		stmt := p.parseStatement()
		if p.err != nil {
			return nil, fmt.Errorf("parse error at line %d, column %d: %v", p.errLine, p.errColumn, p.err)
		}
		if stmt != nil {
			stmts = append(stmts, stmt)
		}
		// Skip optional semicolon
		if p.currTok.Type == TokSemi {
			p.nextToken()
		}
	}

	return stmts, nil
}

// Parse parses a single SQL statement from the input string.
func Parse(input string) (Statement, error) {
	p := NewParser(input)
	return p.Parse()
}

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	p.currTok = p.peekTok
	p.peekTok = p.lexer.NextToken()
}

// expect checks that the current token is of the expected type.
func (p *Parser) expect(t TokenType) bool {
	if p.currTok.Type == t {
		p.nextToken()
		return true
	}
	p.error("expected %s, got %s", t, p.currTok.Type)
	return false
}

// expectPeek checks that the peek token is of the expected type.
func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTok.Type == t {
		p.nextToken()
		return true
	}
	p.error("expected %s, got %s", t, p.peekTok.Type)
	return false
}

// curTokenIs checks if the current token is of the given type.
func (p *Parser) curTokenIs(t TokenType) bool {
	return p.currTok.Type == t
}

// isKeywordAsIdent checks if the current token is a keyword that can be used as an identifier.
// Many SQL keywords can be used as column names or table names.
func (p *Parser) isKeywordAsIdent() bool {
	// List of keywords that can be used as identifiers (column names, table names)
	switch p.currTok.Type {
	case TokAction, TokCascade, TokRestrict, TokCheck, TokComment,
		TokEngine, TokCharset, TokCollate, TokDefault, TokNull,
		TokPrimary, TokKey, TokUnique, TokForeign, TokReferences,
		TokConstraint, TokAutoIncrement, TokUnsigned, TokZerofill,
		TokIndex, TokTable, TokView, TokTrigger, TokFunction,
		TokUser, TokPassword, TokIdentified, TokRole, TokGrants,
		TokOption, TokWith, TokRecursive, TokFor, TokAt,
		TokBackup, TokRestore, TokDescribe, TokBegin, TokEnd,
		TokWhen, TokThen, TokElse, TokCase, TokIf, TokExists,
		TokAny, TokAll, TokDistinct, TokUsing, TokFrom, TokWhere,
		TokGroup, TokBy, TokHaving, TokOrder, TokAsc, TokDesc,
		TokLimit, TokOffset, TokInto, TokValues, TokSet,
		TokSelect, TokInsert, TokUpdate, TokDelete, TokDrop,
		TokCreate, TokAlter, TokAdd, TokColumn, TokRename,
		TokModify, TokDatabase, TokSchema, TokUse, TokShow,
		TokGrant, TokRevoke, TokPrivileges, TokTo,
		TokTruncate, TokOn, TokAs, TokAnd, TokOr, TokNot,
		TokIn, TokLike, TokGlob, TokBetween, TokIs,
		TokJoin, TokInner, TokLeft, TokRight, TokCross, TokOuter,
		TokFull, TokNatural, TokUnion, TokIntersect, TokExcept,
		TokOver, TokPartition, TokWindow, TokRows, TokRange,
		TokPreceding, TokFollowing, TokCurrent,
		TokReturns, TokReturn, TokReplace, TokLet,
		TokBefore, TokAfter, TokInstead, TokEach, TokRow, TokStatement,
		TokOf,
		// Bulk import/export keywords that can be used as identifiers
		TokData, TokLoad, TokCopy, TokInfile, TokFields, TokLines,
		TokTerminated, TokEnclosed, TokEscaped, TokOptionally:
		return true
	default:
		return false
	}
}

// isIdentOrKeyword checks if current token is an identifier or a keyword that can be used as identifier.
func (p *Parser) isIdentOrKeyword() bool {
	return p.curTokenIs(TokIdent) || p.isKeywordAsIdent()
}

// peekTokenIs checks if the peek token is of the given type.
func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekTok.Type == t
}

// error records a parse error.
func (p *Parser) error(format string, args ...interface{}) {
	if p.err == nil {
		p.err = fmt.Errorf(format, args...)
		p.errLine = p.currTok.Line
		p.errColumn = p.currTok.Column
	}
}

// ============================================================================
// Statement Parsing
// ============================================================================

func (p *Parser) parseStatement() Statement {
	// Check for WITH clause before DML statements
	var withClause *WithClause
	if p.currTok.Type == TokWith {
		withClause = p.parseWithClause()
		if withClause == nil {
			return nil
		}
	}

	switch p.currTok.Type {
	case TokSelect:
		if withClause != nil {
			return &WithStmt{CTEs: withClause.CTEs, MainQuery: p.parseSelect()}
		}
		return p.parseSelect()
	case TokInsert:
		stmt := p.parseInsert()
		if stmt != nil && withClause != nil {
			stmt.WithClause = withClause
		}
		return stmt
	case TokUpdate:
		stmt := p.parseUpdate()
		if stmt != nil && withClause != nil {
			stmt.WithClause = withClause
		}
		return stmt
	case TokDelete:
		stmt := p.parseDelete()
		if stmt != nil && withClause != nil {
			stmt.WithClause = withClause
		}
		return stmt
	case TokCreate:
		return p.parseCreate()
	case TokDrop:
		return p.parseDrop()
	case TokAlter:
		return p.parseAlter()
	case TokTruncate:
		return p.parseTruncate()
	case TokUse:
		return p.parseUse()
	case TokShow:
		return p.parseShow()
	case TokDescribe, TokDesc:
		return p.parseDescribe()
	case TokGrant:
		return p.parseGrant()
	case TokRevoke:
		return p.parseRevoke()
	case TokSet:
		return p.parseSet()
	case TokBackup:
		return p.parseBackup()
	case TokRestore:
		return p.parseRestore()
	case TokVacuum:
		return p.parseVacuum()
	case TokPragma:
		return p.parsePragma()
	case TokExplain:
		return p.parseExplain()
	case TokBegin:
		return p.parseBegin()
	case TokCommit:
		return p.parseCommit()
	case TokRollback:
		return p.parseRollback()
	case TokSavepoint:
		return p.parseSavepoint()
	case TokRelease:
		return p.parseReleaseSavepoint()
	case TokCopy:
		return p.parseCopy()
	case TokLoad:
		return p.parseLoadData()
	case TokLParen:
		// Could be a parenthesized SELECT
		if withClause != nil {
			return &WithStmt{CTEs: withClause.CTEs, MainQuery: p.parseSelect()}
		}
		return p.parseSelect()
	default:
		p.error("unexpected token: %s", p.currTok.Type)
		return nil
	}
}

// parseWithClause parses a WITH clause (CTE) and returns a WithClause.
// Syntax: WITH [RECURSIVE] cte_name [(col1, col2, ...)] AS (query) [, ...]
func (p *Parser) parseWithClause() *WithClause {
	p.nextToken() // consume WITH

	withClause := &WithClause{}

	// Check for RECURSIVE keyword (applies to all CTEs in the WITH clause)
	if p.curTokenIs(TokRecursive) {
		withClause.Recursive = true
		p.nextToken()
	}

	// Parse CTE definitions
	for {
		cte := CTEDefinition{Recursive: withClause.Recursive}

		// Parse CTE name
		if !p.curTokenIs(TokIdent) {
			p.error("expected CTE name, got %s", p.currTok.Type)
			return nil
		}
		cte.Name = p.currTok.Value
		p.nextToken()

		// Parse optional column list: (col1, col2, ...)
		if p.curTokenIs(TokLParen) {
			p.nextToken()
			for {
				if !p.isIdentOrKeyword() {
					p.error("expected column name, got %s", p.currTok.Type)
					return nil
				}
				cte.Columns = append(cte.Columns, p.currTok.Value)
				p.nextToken()

				if !p.curTokenIs(TokComma) {
					break
				}
				p.nextToken()
			}
			if !p.expect(TokRParen) {
				return nil
			}
		}

		// Parse AS keyword
		if !p.expect(TokAs) {
			return nil
		}

		// Parse the subquery in parentheses
		if !p.expect(TokLParen) {
			return nil
		}
		cte.Query = p.parseStatement()
		if cte.Query == nil {
			return nil
		}
		if !p.expect(TokRParen) {
			return nil
		}

		withClause.CTEs = append(withClause.CTEs, cte)

		// Check for more CTEs
		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return withClause
}

// parseWith parses a WITH clause (CTE) statement for SELECT.
// Syntax: WITH [RECURSIVE] cte_name [(col1, col2, ...)] AS (query) [, ...] main_query
func (p *Parser) parseWith() Statement {
	withClause := p.parseWithClause()
	if withClause == nil {
		return nil
	}

	// Parse the main query
	mainQuery := p.parseStatement()
	if mainQuery == nil {
		return nil
	}

	return &WithStmt{CTEs: withClause.CTEs, MainQuery: mainQuery}
}

// parseSelect parses a SELECT statement.
func (p *Parser) parseSelect() Statement {
	// Handle parenthesized SELECT
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		stmt := p.parseSelect()
		if !p.expect(TokRParen) {
			return nil
		}
		// Check for UNION
		if p.curTokenIs(TokUnion) {
			return p.parseUnion(stmt)
		}
		return stmt
	}

	p.nextToken() // consume SELECT

	stmt := &SelectStmt{}

	// DISTINCT
	if p.curTokenIs(TokDistinct) {
		stmt.Distinct = true
		p.nextToken()
	}

	// Columns
	stmt.Columns = p.parseSelectColumns()

	// FROM
	if p.curTokenIs(TokFrom) {
		stmt.From = p.parseFromClause()
	}

	// WHERE
	if p.curTokenIs(TokWhere) {
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	// GROUP BY
	if p.curTokenIs(TokGroup) {
		p.nextToken()
		if !p.expect(TokBy) {
			return nil
		}
		stmt.GroupBy = p.parseExpressionList()
	}

	// HAVING
	if p.curTokenIs(TokHaving) {
		p.nextToken()
		stmt.Having = p.parseExpression()
	}

	// ORDER BY
	if p.curTokenIs(TokOrder) {
		stmt.OrderBy = p.parseOrderBy()
	}

	// LIMIT
	if p.curTokenIs(TokLimit) {
		stmt.Limit = p.parseLimit()
	}

	// OFFSET
	if p.curTokenIs(TokOffset) {
		p.nextToken()
		val, err := strconv.Atoi(p.currTok.Value)
		if err != nil {
			p.error("invalid OFFSET value")
			return nil
		}
		p.nextToken()
		stmt.Offset = &val
	}

	// UNION
	if p.curTokenIs(TokUnion) {
		return p.parseSetOperation(stmt, SetUnion)
	}

	// INTERSECT
	if p.curTokenIs(TokIntersect) {
		return p.parseSetOperation(stmt, SetIntersect)
	}

	// EXCEPT
	if p.curTokenIs(TokExcept) {
		return p.parseSetOperation(stmt, SetExcept)
	}

	return stmt
}

// parseSelectColumns parses SELECT columns.
func (p *Parser) parseSelectColumns() []Expression {
	var columns []Expression

	for {
		col := p.parseSelectColumn()
		if col == nil {
			break
		}
		columns = append(columns, col)

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return columns
}

// parseSelectColumn parses a single SELECT column.
func (p *Parser) parseSelectColumn() Expression {
	// Check for *
	if p.curTokenIs(TokStar) {
		p.nextToken()
		return &StarExpr{}
	}

	// Could be table.* or expression
	expr := p.parseExpression()
	if expr == nil {
		return nil
	}

	// Check for alias
	if p.curTokenIs(TokAs) {
		p.nextToken()
		// Accept identifiers and keywords as alias names
		if p.curTokenIs(TokIdent) || isKeywordAsIdent(p.currTok.Type) {
			setAlias(expr, p.currTok.Value)
			p.nextToken()
		}
	} else if p.curTokenIs(TokIdent) || isKeywordAsIdent(p.currTok.Type) {
		// Implicit alias (without AS) - accept identifiers and keywords
		setAlias(expr, p.currTok.Value)
		p.nextToken()
	}

	return expr
}

// isKeywordAsIdent returns true if the token type can be used as an identifier (e.g., for aliases)
func isKeywordAsIdent(t TokenType) bool {
	switch t {
	case TokCumeDist, TokPercentRank, TokNthValue, TokNtile, TokLead, TokLag,
		TokFirstValue, TokLastValue, TokCount, TokSum, TokRank,
		TokAvg, TokMin, TokMax, TokCoalesce, TokNullIf, TokCast, TokCase,
		TokWhen, TokThen, TokElse, TokEnd, TokIf, TokExists, TokAny,
		TokOver, TokPartition, TokWindow, TokRows, TokRange, TokPreceding,
		TokFollowing, TokCurrent, TokIgnore, TokRespect, TokUnbounded:
		return true
	default:
		return false
	}
}

// setAlias sets the alias on an expression if supported.
func setAlias(expr Expression, alias string) {
	switch e := expr.(type) {
	case *ColumnRef:
		e.Alias = alias
	case *Literal:
		e.Alias = alias
	case *BinaryExpr:
		e.Alias = alias
	case *WindowFuncCall:
		e.Alias = alias
	}
}

// parseFromClause parses a FROM clause.
func (p *Parser) parseFromClause() *FromClause {
	p.nextToken() // consume FROM

	fc := &FromClause{
		Table: p.parseTableRef(),
	}

	// JOINs
	for isJoinToken(p.currTok.Type) {
		join := p.parseJoin()
		if join == nil {
			break
		}
		fc.Joins = append(fc.Joins, join)
	}

	return fc
}

// parseJoin parses a JOIN clause.
func (p *Parser) parseJoin() *JoinClause {
	jc := &JoinClause{}

	// Determine join type
	switch p.currTok.Type {
	case TokInner:
		jc.Type = JoinInner
		p.nextToken()
		p.expect(TokJoin)
	case TokLeft:
		jc.Type = JoinLeft
		p.nextToken()
		if p.curTokenIs(TokOuter) {
			p.nextToken()
		}
		p.expect(TokJoin)
	case TokRight:
		jc.Type = JoinRight
		p.nextToken()
		if p.curTokenIs(TokOuter) {
			p.nextToken()
		}
		p.expect(TokJoin)
	case TokCross:
		jc.Type = JoinCross
		p.nextToken()
		p.expect(TokJoin)
	case TokFull:
		jc.Type = JoinFull
		p.nextToken()
		if p.curTokenIs(TokOuter) {
			p.nextToken()
		}
		p.expect(TokJoin)
	case TokJoin:
		jc.Type = JoinInner
		p.nextToken()
	default:
		return nil
	}

	// Table reference
	jc.Table = p.parseTableRef()

	// ON or USING
	if p.curTokenIs(TokOn) {
		p.nextToken()
		jc.On = p.parseExpression()
	} else if p.curTokenIs(TokUsing) {
		p.nextToken()
		if !p.expect(TokLParen) {
			return nil
		}
		jc.Using = p.parseIdentifierList()
		if !p.expect(TokRParen) {
			return nil
		}
	}

	return jc
}

// parseTableRef parses a table reference (table name or subquery).
func (p *Parser) parseTableRef() *TableRef {
	tr := &TableRef{}

	// Check for LATERAL keyword
	if p.curTokenIs(TokLateral) {
		tr.Lateral = true
		p.nextToken() // consume LATERAL
	}

	// Check for VALUES table constructor
	if p.curTokenIs(TokValues) {
		tr.Values = p.parseValuesExpr()
		if tr.Values == nil {
			return nil
		}
		// Parse optional alias
		if p.curTokenIs(TokAs) {
			p.nextToken()
		}
		if p.curTokenIs(TokIdent) {
			tr.Alias = p.currTok.Value
			p.nextToken()
			// Check for column aliases: AS t(col1, col2)
			if p.curTokenIs(TokLParen) {
				p.nextToken()
				tr.Values.Columns = p.parseIdentifierList()
				if !p.expect(TokRParen) {
					return nil
				}
			}
		}
		return tr
	}

	// Check for subquery: (SELECT ...) AS alias or (VALUES ...) AS alias
	if p.curTokenIs(TokLParen) {
		p.nextToken() // consume (
		if p.curTokenIs(TokSelect) {
			// Parse subquery
			stmt := p.parseSelect()
			if stmt == nil {
				return nil
			}
			if !p.expect(TokRParen) {
				return nil
			}
			tr.Subquery = &SubqueryExpr{Select: stmt.(*SelectStmt)}

			// Require alias for derived table
			if p.curTokenIs(TokAs) {
				p.nextToken()
			}
			if p.curTokenIs(TokIdent) {
				tr.Alias = p.currTok.Value
				p.nextToken()
			}
			return tr
		}
		// Check for VALUES inside parentheses
		if p.curTokenIs(TokValues) {
			tr.Values = p.parseValuesExpr()
			if tr.Values == nil {
				return nil
			}
			if !p.expect(TokRParen) {
				return nil
			}
			// Parse optional alias
			if p.curTokenIs(TokAs) {
				p.nextToken()
			}
			if p.curTokenIs(TokIdent) {
				tr.Alias = p.currTok.Value
				p.nextToken()
				// Check for column aliases: AS t(col1, col2)
				if p.curTokenIs(TokLParen) {
					p.nextToken()
					tr.Values.Columns = p.parseIdentifierList()
					if !p.expect(TokRParen) {
						return nil
					}
				}
			}
			return tr
		}
		p.error("expected SELECT, VALUES or table name")
		return nil
	}

	// Regular table name
	if !p.isIdentOrKeyword() {
		p.error("expected table name")
		return nil
	}

	tr.Name = p.currTok.Value
	p.nextToken()

	// Alias
	if p.curTokenIs(TokAs) {
		p.nextToken()
		if !p.curTokenIs(TokIdent) {
			p.error("expected alias name")
			return nil
		}
		tr.Alias = p.currTok.Value
		p.nextToken()
	} else if p.curTokenIs(TokIdent) {
		tr.Alias = p.currTok.Value
		p.nextToken()
	}

	return tr
}

// parseValuesExpr parses a VALUES table constructor.
// Syntax: VALUES (expr1, expr2, ...), (expr3, expr4, ...)
func (p *Parser) parseValuesExpr() *ValuesExpr {
	ve := &ValuesExpr{}
	p.nextToken() // consume VALUES

	for {
		if !p.expect(TokLParen) {
			return nil
		}

		row := p.parseExpressionList()
		if row == nil {
			return nil
		}
		ve.Rows = append(ve.Rows, row)

		if !p.expect(TokRParen) {
			return nil
		}

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken() // consume ,
	}

	return ve
}

// parseOrderBy parses an ORDER BY clause.
func (p *Parser) parseOrderBy() []*OrderByItem {
	p.nextToken() // consume ORDER
	if !p.expect(TokBy) {
		return nil
	}
	return p.parseOrderByItems()
}

// parseOrderByItems parses the items after ORDER BY.
func (p *Parser) parseOrderByItems() []*OrderByItem {
	var items []*OrderByItem

	for {
		item := &OrderByItem{
			Expr:      p.parseExpression(),
			Ascending: true,
		}

		// Handle COLLATE
		if p.curTokenIs(TokCollate) {
			p.nextToken()
			if !p.curTokenIs(TokIdent) {
				p.error("expected collation name")
				return nil
			}
			item.Collate = p.currTok.Value
			p.nextToken()
		}

		if p.curTokenIs(TokAsc) {
			p.nextToken()
		} else if p.curTokenIs(TokDesc) {
			item.Ascending = false
			p.nextToken()
		}

		// Handle NULLS FIRST or NULLS LAST
		if p.curTokenIs(TokNulls) {
			p.nextToken()
			if p.curTokenIs(TokFirst) {
				item.NullsFirst = true
				p.nextToken()
			} else if p.curTokenIs(TokLast) {
				item.NullsLast = true
				p.nextToken()
			} else {
				p.error("expected FIRST or LAST after NULLS")
				return nil
			}
		}

		items = append(items, item)

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return items
}

// parseLimit parses a LIMIT clause.
func (p *Parser) parseLimit() *int {
	p.nextToken() // consume LIMIT

	val, err := strconv.Atoi(p.currTok.Value)
	if err != nil {
		p.error("invalid LIMIT value")
		return nil
	}
	p.nextToken()

	return &val
}

// parseUnion parses a UNION statement.
func (p *Parser) parseSetOperation(left Statement, op SetOperation) *UnionStmt {
	p.nextToken() // consume UNION/INTERSECT/EXCEPT

	all := false
	// Support ALL for all set operations (UNION ALL, INTERSECT ALL, EXCEPT ALL)
	if p.curTokenIs(TokAll) {
		all = true
		p.nextToken()
	}

	right := p.parseSelect()
	if right == nil {
		return nil
	}

	return &UnionStmt{
		Left:  left,
		Right: right,
		All:   all,
		Op:    op,
	}
}

// parseUnion parses a UNION statement (deprecated, use parseSetOperation)
func (p *Parser) parseUnion(left Statement) *UnionStmt {
	return p.parseSetOperation(left, SetUnion)
}

// parseInsert parses an INSERT statement.
func (p *Parser) parseInsert() *InsertStmt {
	p.nextToken() // consume INSERT
	if !p.expect(TokInto) {
		return nil
	}

	stmt := &InsertStmt{
		Table: p.currTok.Value,
	}
	p.nextToken()

	// Columns
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		stmt.Columns = p.parseIdentifierList()
		if !p.expect(TokRParen) {
			return nil
		}
	}

	// VALUES
	if !p.expect(TokValues) {
		return nil
	}

	// Value lists
	for {
		if !p.expect(TokLParen) {
			return nil
		}

		values := p.parseExpressionList()
		stmt.Values = append(stmt.Values, values)

		if !p.expect(TokRParen) {
			return nil
		}

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	// ON CONFLICT clause (SQLite-style UPSERT)
	if p.curTokenIs(TokOn) {
		p.nextToken()
		if p.curTokenIs(TokConflict) {
			stmt.OnConflict = p.parseOnConflict()
		} else if p.curTokenIs(TokIdent) && strings.ToUpper(p.currTok.Value) == "DUPLICATE" {
			// MySQL-style ON DUPLICATE KEY UPDATE
			p.nextToken()
			if p.expect(TokKey) && p.expect(TokUpdate) {
				stmt.OnDuplicateKeyUpdate = p.parseAssignments()
			}
		}
	}

	// RETURNING clause
	if p.curTokenIs(TokReturning) {
		stmt.Returning = p.parseReturning()
	}

	return stmt
}

// parseOnConflict parses an ON CONFLICT clause.
func (p *Parser) parseOnConflict() *UpsertClause {
	p.nextToken() // consume CONFLICT

	upsert := &UpsertClause{}

	// Optional conflict columns: ON CONFLICT(column1, column2)
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		upsert.ConflictColumns = p.parseIdentifierList()
		if !p.expect(TokRParen) {
			return nil
		}
	}

	// DO NOTHING or DO UPDATE
	if !p.expect(TokDo) {
		return nil
	}

	if p.curTokenIs(TokNothing) {
		upsert.DoNothing = true
		p.nextToken()
	} else if p.curTokenIs(TokUpdate) {
		upsert.DoUpdate = true
		p.nextToken()

		if !p.expect(TokSet) {
			return nil
		}

		upsert.Assignments = p.parseAssignments()
		if len(upsert.Assignments) == 0 {
			p.error("expected at least one assignment in DO UPDATE SET")
			return nil
		}

		// Optional WHERE clause
		if p.curTokenIs(TokWhere) {
			p.nextToken()
			upsert.Where = p.parseExpression()
		}
	}

	return upsert
}

// parseReturning parses a RETURNING clause.
func (p *Parser) parseReturning() *ReturningClause {
	p.nextToken() // consume RETURNING

	ret := &ReturningClause{}

	// Check for RETURNING *
	if p.curTokenIs(TokStar) {
		ret.All = true
		p.nextToken()
		return ret
	}

	// Parse column list
	ret.Columns = p.parseSelectColumns()
	return ret
}

// parseUpdate parses an UPDATE statement.
func (p *Parser) parseUpdate() *UpdateStmt {
	p.nextToken() // consume UPDATE

	stmt := &UpdateStmt{
		Table: p.currTok.Value,
	}
	p.nextToken()

	// SET
	if !p.expect(TokSet) {
		return nil
	}

	// Assignments
	stmt.Assignments = p.parseAssignments()
	if len(stmt.Assignments) == 0 {
		p.error("expected at least one assignment after SET")
		return nil
	}

	// WHERE
	if p.curTokenIs(TokWhere) {
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	// RETURNING clause
	if p.curTokenIs(TokReturning) {
		stmt.Returning = p.parseReturning()
	}

	return stmt
}

// parseAssignments parses SET assignments.
func (p *Parser) parseAssignments() []*Assignment {
	var assignments []*Assignment

	for {
		if !p.curTokenIs(TokIdent) {
			break
		}

		a := &Assignment{
			Column: p.currTok.Value,
		}
		p.nextToken()

		if !p.expect(TokEq) {
			return nil
		}

		a.Value = p.parseExpression()
		assignments = append(assignments, a)

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return assignments
}

// parseDelete parses a DELETE statement.
func (p *Parser) parseDelete() *DeleteStmt {
	p.nextToken() // consume DELETE
	if !p.expect(TokFrom) {
		return nil
	}

	stmt := &DeleteStmt{
		Table: p.currTok.Value,
	}
	p.nextToken()

	// WHERE
	if p.curTokenIs(TokWhere) {
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	// RETURNING clause
	if p.curTokenIs(TokReturning) {
		stmt.Returning = p.parseReturning()
	}

	return stmt
}

// parseCreate parses a CREATE statement.
func (p *Parser) parseCreate() Statement {
	p.nextToken() // consume CREATE

	// Check for TEMP/TEMPORARY keyword
	if p.curTokenIs(TokTemp) {
		p.nextToken()
		if p.curTokenIs(TokTable) {
			stmt := p.parseCreateTable()
			if stmt != nil {
				stmt.Temp = true
			}
			return stmt
		}
		// Other TEMP objects could be added here (TEMP VIEW, etc.)
		p.error("expected TABLE after TEMP")
		return nil
	}

	switch p.currTok.Type {
	case TokTable:
		return p.parseCreateTable()
	case TokIndex:
		return p.parseCreateIndex()
	case TokUnique:
		p.nextToken()
		if p.curTokenIs(TokIndex) {
			stmt := p.parseCreateIndex()
			if stmt != nil {
				stmt.Unique = true
			}
			return stmt
		}
		p.error("expected INDEX after UNIQUE")
		return nil
	case TokUser:
		return p.parseCreateUser()
	case TokView:
		return p.parseCreateView(false)
	case TokOr:
		// CREATE OR REPLACE FUNCTION or VIEW
		p.nextToken()
		if !p.expect(TokReplace) {
			return nil
		}
		if p.curTokenIs(TokFunction) {
			p.nextToken()
			// FUNCTION already consumed, parse rest
			stmt := &CreateFunctionStmt{
				Name:    p.currTok.Value,
				Replace: true,
			}
			p.nextToken() // consume function name

			// Parse parameters and rest
			if !p.expect(TokLParen) {
				return nil
			}

			for !p.curTokenIs(TokRParen) && p.err == nil {
				param := &FunctionParameter{
					Name: p.currTok.Value,
				}
				p.nextToken()

				param.Type = p.parseDataType()
				if param.Type == nil {
					p.error("expected data type for parameter %s", param.Name)
					return nil
				}

				stmt.Parameters = append(stmt.Parameters, param)

				if p.curTokenIs(TokComma) {
					p.nextToken()
				}
			}

			if !p.expect(TokRParen) {
				return nil
			}

			if !p.expect(TokReturns) {
				return nil
			}

			stmt.ReturnType = p.parseDataType()
			if stmt.ReturnType == nil {
				p.error("expected return type")
				return nil
			}

			if !p.expect(TokReturn) {
				return nil
			}

			stmt.Body = p.parseExpression()
			if stmt.Body == nil {
				p.error("expected function body expression")
				return nil
			}

			return stmt
		} else if p.curTokenIs(TokView) {
			return p.parseCreateView(true)
		}
		p.error("expected FUNCTION or VIEW after CREATE OR REPLACE")
		return nil
	case TokFunction:
		return p.parseCreateFunction()
	case TokTrigger:
		return p.parseCreateTrigger()
	case TokFts:
		return p.parseCreateFTS()
	default:
		p.error("expected TABLE, INDEX, FTS, VIEW, USER, FUNCTION or TRIGGER after CREATE")
		return nil
	}
}

// parseCreateTable parses a CREATE TABLE statement.
func (p *Parser) parseCreateTable() *CreateTableStmt {
	p.nextToken() // consume TABLE

	stmt := &CreateTableStmt{
		Options: make(map[string]string),
	}

	// IF NOT EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokNot) {
			return nil
		}
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfNotExists = true
	}

	// Table name
	stmt.TableName = p.currTok.Value
	p.nextToken()

	// Column definitions
	if !p.expect(TokLParen) {
		return nil
	}

	for !p.curTokenIs(TokRParen) && p.err == nil {
		// Check for constraint
		if p.curTokenIs(TokPrimary) || p.curTokenIs(TokUnique) || p.curTokenIs(TokForeign) || p.curTokenIs(TokConstraint) || p.curTokenIs(TokCheck) {
			c := p.parseTableConstraint()
			if c != nil {
				stmt.Constraints = append(stmt.Constraints, c)
			}
		} else {
			col := p.parseColumnDef()
			if col != nil {
				stmt.Columns = append(stmt.Columns, col)
			}
		}

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	if !p.expect(TokRParen) {
		return nil
	}

	return stmt
}

// parseColumnDef parses a column definition.
func (p *Parser) parseColumnDef() *ColumnDef {
	cd := &ColumnDef{
		Nullable: true,
	}

	// Column name - can be an identifier or a keyword used as identifier
	if !p.curTokenIs(TokIdent) && !p.isKeywordAsIdent() {
		p.error("expected column name")
		return nil
	}
	cd.Name = p.currTok.Value
	p.nextToken()

	// Data type
	cd.Type = p.parseDataType()

	// Modifiers
	for {
		switch p.currTok.Type {
		case TokNot:
			p.nextToken()
			if !p.expect(TokNull) {
				return nil
			}
			cd.Nullable = false
		case TokNull:
			p.nextToken()
		case TokDefault:
			p.nextToken()
			cd.Default = p.parseExpression()
		case TokAutoIncrement:
			cd.AutoIncr = true
			p.nextToken()
		case TokPrimary:
			p.nextToken()
			if !p.expect(TokKey) {
				return nil
			}
			cd.PrimaryKey = true
		case TokUnique:
			cd.Unique = true
			p.nextToken()
		case TokComment:
			p.nextToken()
			if p.curTokenIs(TokString) {
				cd.Comment = p.currTok.Value
				p.nextToken()
			}
		case TokGenerated:
			p.nextToken()
			if !p.expect(TokAlways) {
				return nil
			}
			if !p.expect(TokAs) {
				return nil
			}
			if !p.expect(TokLParen) {
				return nil
			}
			cd.GeneratedExpr = p.parseExpression()
			if !p.expect(TokRParen) {
				return nil
			}
			// Check for STORED or VIRTUAL
			if p.curTokenIs(TokStored) {
				cd.GeneratedStored = true
				p.nextToken()
			} else if p.curTokenIs(TokVirtual) {
				cd.GeneratedStored = false
				p.nextToken()
			}
		case TokCollate:
			p.nextToken()
			if !p.curTokenIs(TokIdent) {
				p.error("expected collation name")
				return nil
			}
			cd.Collate = p.currTok.Value
			p.nextToken()
		default:
			return cd
		}
	}
}

// parseDataType parses a data type.
func (p *Parser) parseDataType() *DataType {
	dt := &DataType{
		Name: p.currTok.Value,
	}
	p.nextToken()

	// Size/Precision in parentheses
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		size, err := strconv.Atoi(p.currTok.Value)
		if err != nil {
			p.error("invalid data type size")
			return nil
		}
		dt.Size = size
		p.nextToken()

		// Check for scale (DECIMAL(10,2))
		if p.curTokenIs(TokComma) {
			p.nextToken()
			scale, err := strconv.Atoi(p.currTok.Value)
			if err != nil {
				p.error("invalid data type scale")
				return nil
			}
			dt.Precision = dt.Size
			dt.Scale = scale
			dt.Size = 0
			p.nextToken()
		}

		if !p.expect(TokRParen) {
			return nil
		}
	}

	// UNSIGNED
	if p.curTokenIs(TokUnsigned) {
		dt.Unsigned = true
		p.nextToken()
	}

	return dt
}

// parseTableConstraint parses a table constraint.
func (p *Parser) parseTableConstraint() *TableConstraint {
	tc := &TableConstraint{}

	// Constraint name
	if p.curTokenIs(TokConstraint) {
		p.nextToken()
		tc.Name = p.currTok.Value
		p.nextToken()
	}

	// Constraint type
	switch p.currTok.Type {
	case TokPrimary:
		p.nextToken()
		if !p.expect(TokKey) {
			return nil
		}
		tc.Type = ConstraintPrimaryKey
	case TokUnique:
		p.nextToken()
		tc.Type = ConstraintUnique
	case TokForeign:
		p.nextToken()
		if !p.expect(TokKey) {
			return nil
		}
		tc.Type = ConstraintForeignKey
	case TokCheck:
		p.nextToken()
		tc.Type = ConstraintCheck
		// Parse CHECK (expression)
		if !p.expect(TokLParen) {
			return nil
		}
		tc.CheckExpr = p.parseExpression()
		if !p.expect(TokRParen) {
			return nil
		}
		return tc
	}

	// Columns (for PRIMARY KEY, UNIQUE, FOREIGN KEY)
	if !p.expect(TokLParen) {
		return nil
	}
	tc.Columns = p.parseIdentifierList()
	if !p.expect(TokRParen) {
		return nil
	}

	// REFERENCES for foreign key
	if tc.Type == ConstraintForeignKey && p.curTokenIs(TokReferences) {
		p.nextToken()
		tc.RefTable = p.currTok.Value
		p.nextToken()

		if p.curTokenIs(TokLParen) {
			p.nextToken()
			tc.RefColumns = p.parseIdentifierList()
			if !p.expect(TokRParen) {
				return nil
			}
		}

		// ON DELETE / ON UPDATE
		if p.curTokenIs(TokOn) {
			p.nextToken()
			if p.curTokenIs(TokDelete) {
				p.nextToken()
				tc.OnDelete = p.parseFKAction()
			} else if p.curTokenIs(TokUpdate) {
				p.nextToken()
				tc.OnUpdate = p.parseFKAction()
			}
		}

		if p.curTokenIs(TokOn) {
			p.nextToken()
			if p.curTokenIs(TokDelete) {
				p.nextToken()
				tc.OnDelete = p.parseFKAction()
			} else if p.curTokenIs(TokUpdate) {
				p.nextToken()
				tc.OnUpdate = p.parseFKAction()
			}
		}
	}

	return tc
}

// parseFKAction parses a foreign key action (CASCADE, SET NULL, RESTRICT, NO ACTION).
func (p *Parser) parseFKAction() string {
	if p.curTokenIs(TokCascade) {
		p.nextToken()
		return "CASCADE"
	}
	if p.curTokenIs(TokRestrict) {
		p.nextToken()
		return "RESTRICT"
	}
	if p.curTokenIs(TokIdent) && p.currTok.Value == "NO" {
		p.nextToken()
		if p.curTokenIs(TokAction) {
			p.nextToken()
			return "NO ACTION"
		}
		p.error("expected ACTION after NO")
		return ""
	}
	if p.curTokenIs(TokSet) {
		p.nextToken()
		if p.curTokenIs(TokNull) {
			p.nextToken()
			return "SET NULL"
		}
		p.error("expected NULL after SET")
		return ""
	}
	// Default is RESTRICT
	return "RESTRICT"
}

// parseCreateIndex parses a CREATE INDEX statement.
func (p *Parser) parseCreateIndex() *CreateIndexStmt {
	p.nextToken() // consume INDEX

	stmt := &CreateIndexStmt{}

	// IF NOT EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokNot) {
			return nil
		}
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfNotExists = true
	}

	// Index name
	stmt.IndexName = p.currTok.Value
	p.nextToken()

	// ON
	if !p.expect(TokOn) {
		return nil
	}

	// Table name
	stmt.TableName = p.currTok.Value
	p.nextToken()

	// Columns
	if !p.expect(TokLParen) {
		return nil
	}
	stmt.Columns = p.parseIdentifierList()
	if !p.expect(TokRParen) {
		return nil
	}

	return stmt
}

// parseCreateFTS parses a CREATE FTS INDEX statement.
// Syntax: CREATE FTS INDEX [IF NOT EXISTS] name ON table(column1, column2, ...) [WITH TOKENIZER tokenizer]
func (p *Parser) parseCreateFTS() *CreateFTSStmt {
	p.nextToken() // consume FTS

	// Expect INDEX
	if !p.expect(TokIndex) {
		return nil
	}

	stmt := &CreateFTSStmt{}

	// IF NOT EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokNot) {
			return nil
		}
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfNotExists = true
	}

	// Index name
	if !p.isIdentOrKeyword() {
		p.error("expected FTS index name")
		return nil
	}
	stmt.IndexName = p.currTok.Value
	p.nextToken()

	// ON
	if !p.expect(TokOn) {
		return nil
	}

	// Table name
	if !p.isIdentOrKeyword() {
		p.error("expected table name")
		return nil
	}
	stmt.TableName = p.currTok.Value
	p.nextToken()

	// Columns
	if !p.expect(TokLParen) {
		return nil
	}
	stmt.Columns = p.parseIdentifierList()
	if !p.expect(TokRParen) {
		return nil
	}

	// Optional WITH TOKENIZER clause
	if p.curTokenIs(TokWith) {
		p.nextToken()
		if !p.expect(TokTokenizer) {
			return nil
		}
		if !p.isIdentOrKeyword() {
			p.error("expected tokenizer name")
			return nil
		}
		stmt.Tokenizer = strings.ToLower(p.currTok.Value)
		p.nextToken()
	}

	return stmt
}

// parseCreateView parses a CREATE VIEW or CREATE OR REPLACE VIEW statement.
func (p *Parser) parseCreateView(orReplace bool) *CreateViewStmt {
	p.nextToken() // consume VIEW

	stmt := &CreateViewStmt{
		OrReplace: orReplace,
	}

	// View name
	stmt.ViewName = p.currTok.Value
	p.nextToken()

	// Optional column list
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		for !p.curTokenIs(TokRParen) && p.err == nil {
			stmt.Columns = append(stmt.Columns, p.currTok.Value)
			p.nextToken()
			if p.curTokenIs(TokComma) {
				p.nextToken()
			}
		}
		if !p.expect(TokRParen) {
			return nil
		}
	}

	// AS keyword
	if !p.expect(TokAs) {
		return nil
	}

	// Parse the SELECT statement
	stmt.SelectStmt = p.parseSelect()
	if stmt.SelectStmt == nil {
		return nil
	}

	// Optional WITH [CASCADED | LOCAL] CHECK OPTION
	if p.curTokenIs(TokWith) {
		p.nextToken()
		// Check for CASCADED or LOCAL (default is CASCADED)
		if p.curTokenIs(TokCascaded) {
			stmt.CheckOption = "CASCADED"
			p.nextToken()
		} else if p.curTokenIs(TokLocal) {
			stmt.CheckOption = "LOCAL"
			p.nextToken()
		} else {
			// Default to CASCADED if just WITH CHECK OPTION
			stmt.CheckOption = "CASCADED"
		}

		// Expect CHECK OPTION
		if !p.expect(TokCheck) {
			return nil
		}
		if !p.expect(TokOption) {
			return nil
		}
	}

	return stmt
}

// parseDrop parses a DROP statement.
func (p *Parser) parseDrop() Statement {
	p.nextToken() // consume DROP

	switch p.currTok.Type {
	case TokTable:
		return p.parseDropTable()
	case TokIndex:
		return p.parseDropIndex()
	case TokView:
		return p.parseDropView()
	case TokUser:
		return p.parseDropUser()
	case TokFunction:
		return p.parseDropFunction()
	case TokTrigger:
		return p.parseDropTrigger()
	case TokFts:
		return p.parseDropFTS()
	default:
		p.error("expected TABLE, INDEX, FTS, VIEW, USER, FUNCTION or TRIGGER after DROP")
		return nil
	}
}

// parseDropTable parses a DROP TABLE statement.
func (p *Parser) parseDropTable() *DropTableStmt {
	p.nextToken() // consume TABLE

	stmt := &DropTableStmt{}

	// IF EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfExists = true
	}

	// Table name - check if present
	if !p.isIdentOrKeyword() {
		p.error("expected table name")
		return nil
	}
	stmt.TableName = p.currTok.Value
	p.nextToken()

	return stmt
}

// parseDropIndex parses a DROP INDEX statement.
func (p *Parser) parseDropIndex() *DropIndexStmt {
	p.nextToken() // consume INDEX

	stmt := &DropIndexStmt{}

	// Index name
	stmt.IndexName = p.currTok.Value
	p.nextToken()

	// ON table
	if p.curTokenIs(TokOn) {
		p.nextToken()
		stmt.TableName = p.currTok.Value
		p.nextToken()
	}

	return stmt
}

// parseDropView parses a DROP VIEW statement.
func (p *Parser) parseDropView() *DropViewStmt {
	p.nextToken() // consume VIEW

	stmt := &DropViewStmt{}

	// IF EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfExists = true
	}

	// View name
	stmt.ViewName = p.currTok.Value
	p.nextToken()

	return stmt
}

// parseUse parses a USE statement.
func (p *Parser) parseUse() *UseStmt {
	p.nextToken() // consume USE

	stmt := &UseStmt{
		Database: p.currTok.Value,
	}
	p.nextToken()

	return stmt
}

// parseShow parses a SHOW statement.
func (p *Parser) parseShow() Statement {
	p.nextToken() // consume SHOW

	// Handle SHOW GRANTS
	if p.curTokenIs(TokGrants) {
		return p.parseShowGrants()
	}

	// Handle SHOW CREATE TABLE
	if p.curTokenIs(TokCreate) {
		p.nextToken() // consume CREATE
		if !p.expect(TokTable) {
			return nil
		}
		tableName := p.currTok.Value
		p.nextToken()
		return &ShowCreateTableStmt{TableName: tableName}
	}

	stmt := &ShowStmt{
		Type: p.currTok.Value,
	}
	p.nextToken()

	// FROM
	if p.curTokenIs(TokFrom) {
		p.nextToken()
		stmt.From = p.currTok.Value
		p.nextToken()
	}

	// LIKE
	if p.curTokenIs(TokLike) {
		p.nextToken()
		stmt.Like = p.currTok.Value
		p.nextToken()
	}

	return stmt
}

// parseDescribe parses a DESCRIBE/DESC statement.
func (p *Parser) parseDescribe() Statement {
	p.nextToken() // consume DESCRIBE/DESC

	tableName := p.currTok.Value
	p.nextToken()

	return &DescribeStmt{TableName: tableName}
}

// parseExplain parses an EXPLAIN statement.
func (p *Parser) parseExplain() Statement {
	p.nextToken() // consume EXPLAIN

	stmt := &ExplainStmt{}

	// Check for QUERY PLAN
	if p.curTokenIs(TokQuery) {
		p.nextToken()
		if p.curTokenIs(TokPlan) {
			p.nextToken()
			stmt.QueryPlan = true
		}
	}

	// Parse the statement to explain
	innerStmt := p.parseStatement()
	if innerStmt == nil {
		return nil
	}
	stmt.Statement = innerStmt

	return stmt
}

// parseBegin parses a BEGIN [TRANSACTION] statement.
func (p *Parser) parseBegin() Statement {
	p.nextToken() // consume BEGIN

	stmt := &BeginStmt{}

	// Optional transaction type: DEFERRED, IMMEDIATE, EXCLUSIVE
	switch p.currTok.Type {
	case TokDeferred:
		stmt.TransactionType = "DEFERRED"
		p.nextToken()
	case TokImmediate:
		stmt.TransactionType = "IMMEDIATE"
		p.nextToken()
	case TokExclusive:
		stmt.TransactionType = "EXCLUSIVE"
		p.nextToken()
	}

	// Optional TRANSACTION or WORK keyword
	if p.curTokenIs(TokTransaction) || p.curTokenIs(TokWork) {
		p.nextToken()
	}

	return stmt
}

// parseCommit parses a COMMIT [TRANSACTION] statement.
func (p *Parser) parseCommit() Statement {
	p.nextToken() // consume COMMIT

	// Optional TRANSACTION or WORK keyword
	if p.curTokenIs(TokTransaction) || p.curTokenIs(TokWork) {
		p.nextToken()
	}

	return &CommitStmt{}
}

// parseRollback parses a ROLLBACK [TRANSACTION] [TO SAVEPOINT name] statement.
func (p *Parser) parseRollback() Statement {
	p.nextToken() // consume ROLLBACK

	// Optional TRANSACTION or WORK keyword
	if p.curTokenIs(TokTransaction) || p.curTokenIs(TokWork) {
		p.nextToken()
	}

	// Check for TO SAVEPOINT
	if p.curTokenIs(TokTo) {
		p.nextToken()
		if !p.expect(TokSavepoint) {
			return nil
		}
		if !p.curTokenIs(TokIdent) {
			p.error("expected savepoint name")
			return nil
		}
		name := p.currTok.Value
		p.nextToken()
		return &RollbackStmt{ToSavepoint: name}
	}

	return &RollbackStmt{}
}

// parseSavepoint parses a SAVEPOINT name statement.
func (p *Parser) parseSavepoint() Statement {
	p.nextToken() // consume SAVEPOINT

	if !p.curTokenIs(TokIdent) {
		p.error("expected savepoint name")
		return nil
	}
	name := p.currTok.Value
	p.nextToken()

	return &SavepointStmt{Name: name}
}

// parseReleaseSavepoint parses a RELEASE SAVEPOINT name statement.
func (p *Parser) parseReleaseSavepoint() Statement {
	p.nextToken() // consume RELEASE

	if !p.expect(TokSavepoint) {
		return nil
	}

	if !p.curTokenIs(TokIdent) {
		p.error("expected savepoint name")
		return nil
	}
	name := p.currTok.Value
	p.nextToken()

	return &ReleaseSavepointStmt{Name: name}
}

// parseCopy parses a COPY statement for bulk import/export.
// Syntax:
//   COPY table FROM 'file.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')
//   COPY (SELECT ...) TO 'file.csv' WITH (FORMAT csv, HEADER true)
func (p *Parser) parseCopy() Statement {
	p.nextToken() // consume COPY

	stmt := &CopyStmt{}

	// Check for parenthesized query (COPY (SELECT ...) TO ...)
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		stmt.Query = p.parseSelect()
		if stmt.Query == nil {
			return nil
		}
		if !p.expect(TokRParen) {
			return nil
		}
	} else {
		// Table name
		if !p.isIdentOrKeyword() {
			p.error("expected table name or (SELECT ...)")
			return nil
		}
		stmt.TableName = p.currTok.Value
		p.nextToken()
	}

	// Direction: FROM or TO
	if p.curTokenIs(TokFrom) {
		stmt.Direction = "FROM"
	} else if p.curTokenIs(TokTo) {
		stmt.Direction = "TO"
	} else {
		p.error("expected FROM or TO")
		return nil
	}
	p.nextToken()

	// File name
	if !p.curTokenIs(TokString) {
		p.error("expected file name string")
		return nil
	}
	stmt.FileName = p.currTok.Value
	p.nextToken()

	// Optional WITH clause
	if p.curTokenIs(TokWith) {
		p.nextToken()
		if !p.expect(TokLParen) {
			return nil
		}

		for {
			// Option name can be identifier or keyword (like NULL)
			var option string
			if p.curTokenIs(TokIdent) {
				option = strings.ToUpper(p.currTok.Value)
				p.nextToken()
			} else if p.curTokenIs(TokNull) {
				option = "NULL"
				p.nextToken()
			} else {
				break
			}

			switch option {
				case "FORMAT":
					if !p.curTokenIs(TokIdent) {
						p.error("expected format value")
						return nil
					}
					stmt.Format = strings.ToLower(p.currTok.Value)
					p.nextToken()

				case "HEADER":
					if p.curTokenIs(TokBoolLit) || p.curTokenIs(TokIdent) {
						if p.currTok.Value == "true" || p.currTok.Value == "TRUE" {
							stmt.Header = true
						}
						p.nextToken()
					} else if p.curTokenIs(TokNumber) {
						if p.currTok.Value == "1" {
							stmt.Header = true
						}
						p.nextToken()
					}

				case "DELIMITER":
					if !p.curTokenIs(TokString) {
						p.error("expected delimiter string")
						return nil
					}
					stmt.Delimiter = p.currTok.Value
					p.nextToken()

				case "QUOTE":
					if !p.curTokenIs(TokString) {
						p.error("expected quote string")
						return nil
					}
					stmt.Quote = p.currTok.Value
					p.nextToken()

				case "NULL":
					if !p.curTokenIs(TokString) {
						p.error("expected null string")
						return nil
					}
					stmt.NullString = p.currTok.Value
					p.nextToken()

				case "ENCODING":
					if !p.curTokenIs(TokIdent) {
						p.error("expected encoding value")
						return nil
					}
					stmt.Encoding = strings.ToLower(p.currTok.Value)
					p.nextToken()
				}

			if !p.curTokenIs(TokComma) {
				break
			}
			p.nextToken()
		}

		if !p.expect(TokRParen) {
			return nil
		}
	}

	// Set defaults
	if stmt.Format == "" {
		stmt.Format = "csv"
	}
	if stmt.Delimiter == "" {
		stmt.Delimiter = ","
	}
	if stmt.Quote == "" {
		stmt.Quote = "\""
	}

	return stmt
}

// parseLoadData parses a LOAD DATA INFILE statement (MySQL style).
// Syntax:
//   LOAD DATA INFILE 'file.csv' [IGNORE] INTO TABLE table_name
//     FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
//     LINES TERMINATED BY '\n' STARTING BY ''
//     IGNORE 1 ROWS
//     (col1, col2, ...)
func (p *Parser) parseLoadData() Statement {
	p.nextToken() // consume LOAD

	if !p.expect(TokData) {
		return nil
	}

	if !p.expect(TokInfile) {
		return nil
	}

	stmt := &LoadDataStmt{}

	// File name
	if !p.curTokenIs(TokString) {
		p.error("expected file name string")
		return nil
	}
	stmt.FileName = p.currTok.Value
	p.nextToken()

	// Optional IGNORE (for duplicate key handling, before INTO TABLE)
	// Note: This IGNORE is different from IGNORE n ROWS
	if p.curTokenIs(TokIgnore) {
		p.nextToken()
		// Check if this is IGNORE n ROWS (skip lines)
		if p.curTokenIs(TokNumber) {
			var n int
			fmt.Sscanf(p.currTok.Value, "%d", &n)
			stmt.IgnoreRows = n
			p.nextToken()
			// Optional ROWS keyword
			if p.curTokenIs(TokIdent) && strings.ToUpper(p.currTok.Value) == "ROWS" {
				p.nextToken()
			}
		}
		// Otherwise it's IGNORE for duplicate keys (we just consume it)
	}

	// INTO TABLE
	if p.curTokenIs(TokInto) {
		p.nextToken()
	}
	if !p.expect(TokTable) {
		return nil
	}

	// Table name (can be identifier or reserved word like 'data')
	if !p.isIdentOrKeyword() {
		p.error("expected table name")
		return nil
	}
	stmt.TableName = p.currTok.Value
	p.nextToken()

	// Optional FIELDS clause
	if p.curTokenIs(TokFields) {
		p.nextToken()

		for {
			if p.curTokenIs(TokTerminated) {
				p.nextToken()
				if !p.expect(TokBy) {
					return nil
				}
				if !p.curTokenIs(TokString) {
					p.error("expected string for FIELDS TERMINATED BY")
					return nil
				}
				stmt.FieldsTerminated = p.currTok.Value
				p.nextToken()
			} else if p.curTokenIs(TokEnclosed) {
				p.nextToken()
				if !p.expect(TokBy) {
					return nil
				}
				// Optionally enclosed?
				if p.curTokenIs(TokOptionally) {
					p.nextToken()
				}
				if !p.curTokenIs(TokString) {
					p.error("expected string for FIELDS ENCLOSED BY")
					return nil
				}
				stmt.FieldsEnclosed = p.currTok.Value
				p.nextToken()
			} else if p.curTokenIs(TokEscaped) {
				p.nextToken()
				if !p.expect(TokBy) {
					return nil
				}
				if !p.curTokenIs(TokString) {
					p.error("expected string for FIELDS ESCAPED BY")
					return nil
				}
				stmt.FieldsEscaped = p.currTok.Value
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Optional LINES clause
	if p.curTokenIs(TokLines) {
		p.nextToken()

		for {
			if p.curTokenIs(TokTerminated) {
				p.nextToken()
				if !p.expect(TokBy) {
					return nil
				}
				if !p.curTokenIs(TokString) {
					p.error("expected string for LINES TERMINATED BY")
					return nil
				}
				stmt.LinesTerminated = p.currTok.Value
				p.nextToken()
			} else if p.curTokenIs(TokIdent) && strings.ToUpper(p.currTok.Value) == "STARTING" {
				p.nextToken()
				if !p.expect(TokBy) {
					return nil
				}
				if !p.curTokenIs(TokString) {
					p.error("expected string for LINES STARTING BY")
					return nil
				}
				stmt.LinesStarting = p.currTok.Value
				p.nextToken()
			} else {
				break
			}
		}
	}

	// Optional IGNORE n ROWS
	if p.curTokenIs(TokIgnore) {
		p.nextToken()
		if !p.curTokenIs(TokNumber) {
			p.error("expected number after IGNORE")
			return nil
		}
		// Parse the number
		var n int
		fmt.Sscanf(p.currTok.Value, "%d", &n)
		stmt.IgnoreRows = n
		p.nextToken()

		// Optional ROWS keyword
		if p.curTokenIs(TokIdent) && strings.ToUpper(p.currTok.Value) == "ROWS" {
			p.nextToken()
		}
	}

	// Optional column list
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		for {
			if !p.isIdentOrKeyword() {
				p.error("expected column name")
				return nil
			}
			stmt.ColumnList = append(stmt.ColumnList, p.currTok.Value)
			p.nextToken()

			if !p.curTokenIs(TokComma) {
				break
			}
			p.nextToken()
		}
		if !p.expect(TokRParen) {
			return nil
		}
	}

	// Set defaults
	if stmt.FieldsTerminated == "" {
		stmt.FieldsTerminated = "\t"
	}
	if stmt.LinesTerminated == "" {
		stmt.LinesTerminated = "\n"
	}
	if stmt.FieldsEscaped == "" {
		stmt.FieldsEscaped = "\\"
	}

	return stmt
}

// parseAlter parses an ALTER statement.
func (p *Parser) parseAlter() Statement {
	p.nextToken() // consume ALTER

	// ALTER USER
	if p.curTokenIs(TokUser) {
		return p.parseAlterUser()
	}

	// ALTER TABLE
	if !p.expect(TokTable) {
		return nil
	}

	stmt := &AlterTableStmt{
		TableName: p.currTok.Value,
	}
	p.nextToken()

	// Parse actions
	for {
		var action AlterAction

		switch p.currTok.Type {
		case TokAdd:
			action = p.parseAlterAddColumn()
		case TokDrop:
			action = p.parseAlterDropColumn()
		case TokModify:
			action = p.parseAlterModifyColumn()
		case TokRename:
			action = p.parseAlterRename()
		default:
			p.error("unexpected alter action: %s", p.currTok.Type)
			return nil
		}

		if action == nil {
			return nil
		}
		stmt.Actions = append(stmt.Actions, action)

		// Check for more actions
		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return stmt
}

// parseAlterAddColumn parses ADD COLUMN or ADD CONSTRAINT action.
func (p *Parser) parseAlterAddColumn() AlterAction {
	p.nextToken() // consume ADD

	// Handle ADD CONSTRAINT, ADD PRIMARY KEY, ADD UNIQUE, ADD FOREIGN KEY, ADD CHECK
	if p.curTokenIs(TokConstraint) || p.curTokenIs(TokPrimary) || p.curTokenIs(TokUnique) ||
		p.curTokenIs(TokForeign) || p.curTokenIs(TokCheck) {
		constraint := p.parseTableConstraint()
		if constraint == nil {
			return nil
		}
		return &AddConstraintAction{Constraint: constraint}
	}

	// Optional COLUMN keyword
	if p.curTokenIs(TokColumn) {
		p.nextToken()
	}

	col := p.parseColumnDef()
	if col == nil {
		return nil
	}

	return &AddColumnAction{Column: col}
}

// parseAlterDropColumn parses DROP COLUMN or DROP CONSTRAINT action.
func (p *Parser) parseAlterDropColumn() AlterAction {
	p.nextToken() // consume DROP

	// Handle DROP CONSTRAINT
	if p.curTokenIs(TokConstraint) {
		p.nextToken()
		if !p.curTokenIs(TokIdent) {
			p.error("expected constraint name")
			return nil
		}
		constraintName := p.currTok.Value
		p.nextToken()
		return &DropConstraintAction{ConstraintName: constraintName}
	}

	// Handle DROP PRIMARY KEY
	if p.curTokenIs(TokPrimary) {
		p.nextToken()
		if !p.expect(TokKey) {
			return nil
		}
		return &DropConstraintAction{ConstraintName: "PRIMARY"}
	}

	// Optional COLUMN keyword
	if p.curTokenIs(TokColumn) {
		p.nextToken()
	}

	if !p.isIdentOrKeyword() {
		p.error("expected column name")
		return nil
	}

	colName := p.currTok.Value
	p.nextToken()

	return &DropColumnAction{ColumnName: colName}
}

// parseAlterModifyColumn parses MODIFY COLUMN action.
func (p *Parser) parseAlterModifyColumn() *ModifyColumnAction {
	p.nextToken() // consume MODIFY

	// Optional COLUMN keyword
	if p.curTokenIs(TokColumn) {
		p.nextToken()
	}

	col := p.parseColumnDef()
	if col == nil {
		return nil
	}

	return &ModifyColumnAction{Column: col}
}

// parseAlterRename parses RENAME COLUMN or RENAME TO action.
func (p *Parser) parseAlterRename() AlterAction {
	p.nextToken() // consume RENAME

	if p.curTokenIs(TokColumn) {
		// RENAME COLUMN old TO new
		p.nextToken()

		if !p.isIdentOrKeyword() {
			p.error("expected old column name")
			return nil
		}
		oldName := p.currTok.Value
		p.nextToken()

		if !p.expect(TokTo) {
			return nil
		}

		if !p.isIdentOrKeyword() {
			p.error("expected new column name")
			return nil
		}
		newName := p.currTok.Value
		p.nextToken()

		return &RenameColumnAction{OldName: oldName, NewName: newName}
	}

	if p.curTokenIs(TokTo) {
		// RENAME TO new_table
		p.nextToken()

		if !p.curTokenIs(TokIdent) {
			p.error("expected new table name")
			return nil
		}
		newName := p.currTok.Value
		p.nextToken()

		return &RenameTableAction{NewName: newName}
	}

	p.error("expected COLUMN or TO after RENAME")
	return nil
}

// parseTruncate parses a TRUNCATE TABLE statement.
func (p *Parser) parseTruncate() *TruncateTableStmt {
	p.nextToken() // consume TRUNCATE

	if !p.expect(TokTable) {
		return nil
	}

	stmt := &TruncateTableStmt{
		TableName: p.currTok.Value,
	}
	p.nextToken()

	return stmt
}

// ============================================================================
// Expression Parsing
// ============================================================================

// parseExpression parses an expression using precedence climbing.
func (p *Parser) parseExpression() Expression {
	return p.parseBinaryExpr(0)
}

// parseBinaryExpr parses a binary expression with the given minimum precedence.
func (p *Parser) parseBinaryExpr(minPrec int) Expression {
	left := p.parseUnaryExpr()

	for {
		// Special handling for NOT IN (two-token operator)
		if p.curTokenIs(TokNot) {
			// Look ahead to see if next token is IN
			if p.peekTokenIs(TokIn) {
				p.nextToken() // consume NOT
				p.nextToken() // consume IN
				right := p.parseBinaryExpr(4) // parse the subquery or list
				inExpr := &BinaryExpr{Left: left, Op: OpIn, Right: right}
				left = &UnaryExpr{Op: OpNot, Right: inExpr}
				continue
			}
			// NOT without IN - not a binary operator for us
			break
		}

		// Special handling for IS NULL / IS NOT NULL
		if p.curTokenIs(TokIs) {
			p.nextToken() // consume IS
			notNull := false
			if p.curTokenIs(TokNot) {
				p.nextToken() // consume NOT
				notNull = true
			}
			if !p.curTokenIs(TokNull) {
				p.error("expected NULL after IS")
				return nil
			}
			p.nextToken() // consume NULL
			left = &IsNullExpr{Expr: left, Not: notNull}
			continue
		}

		// Special handling for MATCH (full-text search)
		if p.curTokenIs(TokMatch) {
			p.nextToken() // consume MATCH

			// Expect string literal for the query
			if !p.curTokenIs(TokString) {
				p.error("expected string after MATCH")
				return nil
			}
			query := p.currTok.Value
			p.nextToken()

			// Determine the table name from the left expression
			tableName := ""
			if colRef, ok := left.(*ColumnRef); ok {
				if colRef.Table != "" {
					tableName = colRef.Table
				} else {
					tableName = colRef.Name
				}
			} else if star, ok := left.(*StarExpr); ok {
				tableName = star.Table
			}

			left = &MatchExpr{
				Table: tableName,
				Query: query,
			}
			continue
		}

		// Check if current token is a binary operator
		if !p.isBinaryOpToken() {
			break
		}
		op := p.getBinaryOp()
		if op == OpAnd && minPrec > 1 {
			break
		}
		prec := getPrecedence(op)
		if prec < minPrec {
			break
		}

		p.nextToken()

		// Check for ANY/ALL after comparison operator
		if p.curTokenIs(TokAny) || p.curTokenIs(TokAll) {
			isAny := p.curTokenIs(TokAny)
			p.nextToken() // consume ANY or ALL

			// Expect (
			if !p.expect(TokLParen) {
				return nil
			}

			// Expect SELECT
			if !p.curTokenIs(TokSelect) {
				p.error("expected SELECT after ANY/ALL (")
				return nil
			}

			// Parse subquery
			stmt := p.parseSelect()
			if stmt == nil {
				return nil
			}

			if !p.expect(TokRParen) {
				return nil
			}

			left = &AnyAllExpr{
				Left:     left,
				Op:       op,
				IsAny:    isAny,
				Subquery: &SubqueryExpr{Select: stmt.(*SelectStmt)},
			}
			continue
		}

		right := p.parseBinaryExpr(prec + 1)
		left = &BinaryExpr{Left: left, Op: op, Right: right}
	}

	return left
}

// isBinaryOpToken checks if the current token is a binary operator.
func (p *Parser) isBinaryOpToken() bool {
	switch p.currTok.Type {
	case TokEq, TokNe, TokLt, TokLe, TokGt, TokGe,
		TokPlus, TokMinus, TokStar, TokSlash, TokPercent,
		TokAnd, TokOr, TokLike, TokGlob, TokConcat, TokIn, TokBetween, TokIs:
		return true
	default:
		return false
	}
}

// parseUnaryExpr parses a unary expression.
func (p *Parser) parseUnaryExpr() Expression {
	// NOT
	if p.curTokenIs(TokNot) {
		p.nextToken()
		// Parse the full expression after NOT (including binary operators like IN)
		return &UnaryExpr{Op: OpNot, Right: p.parseBinaryExpr(1)}
	}

	// Unary minus
	if p.curTokenIs(TokMinus) {
		p.nextToken()
		return &UnaryExpr{Op: OpNeg, Right: p.parseUnaryExpr()}
	}

	expr := p.parsePrimaryExpr()

	// Handle COLLATE as postfix operator
	if p.curTokenIs(TokCollate) {
		p.nextToken()
		if !p.curTokenIs(TokIdent) {
			p.error("expected collation name")
			return nil
		}
		expr = &CollateExpr{Expr: expr, Collate: p.currTok.Value}
		p.nextToken()
	}

	return expr
}

// parsePrimaryExpr parses a primary expression.
func (p *Parser) parsePrimaryExpr() Expression {
	switch p.currTok.Type {
	case TokIdent:
		return p.parseIdentOrFunction()
	case TokNumber:
		return p.parseNumber()
	case TokString:
		return p.parseString()
	case TokBoolLit:
		return p.parseBool()
	case TokNull:
		p.nextToken()
		return &Literal{Type: LiteralNull}
	case TokLParen:
		return p.parseParenExpr()
	case TokStar:
		p.nextToken()
		return &StarExpr{}
	case TokCase:
		return p.parseCaseExpr()
	case TokCast:
		return p.parseCastExpr()
	case TokExists:
		return p.parseExistsExpr()
	// Handle function keywords (COUNT, SUM, AVG, MIN, MAX, COALESCE, NULLIF)
	case TokCount, TokSum, TokAvg, TokMin, TokMax, TokCoalesce, TokNullIf:
		return p.parseFunctionKeyword()
	// Window function keywords (LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTILE, NTH_VALUE, PERCENT_RANK, CUME_DIST)
	case TokLead, TokLag, TokNtile, TokFirstValue, TokLastValue, TokNthValue, TokPercentRank, TokCumeDist:
		return p.parseFunctionKeyword()
	// JSON function keywords
	case TokJsonExtract, TokJsonArray, TokJsonObject, TokJsonType, TokJsonValid, TokJsonQuote, TokJsonUnquote, TokJsonContains, TokJsonKeys, TokJsonLength:
		return p.parseFunctionKeyword()
	// UDF expressions
	case TokIf:
		return p.parseIfExpr()
	case TokLet:
		return p.parseLetExpr()
	case TokBegin:
		return p.parseBlockExpr()
	// Handle potential UDF names that are also keywords (data types, etc.)
	// If followed by (, treat as function call
	case TokDouble, TokFloat, TokInt, TokInteger, TokBigInt, TokChar, TokVarchar,
		TokText, TokDate, TokTime, TokDateTime, TokBool, TokBoolean, TokBlob,
		TokDecimal, TokNumeric, TokSmallInt, TokTinyInt, TokSeq,
		TokLeft, TokRight, TokReplace, TokTruncate, TokUser, // LEFT/RIGHT/REPLACE/TRUNCATE/USER can be keywords or function names
		TokData, TokLoad, TokCopy, TokInfile, TokFields, TokLines,
		TokTerminated, TokEnclosed, TokEscaped, TokOptionally: // Bulk import/export keywords can be column names
		name := p.currTok.Value
		p.nextToken()
		if p.curTokenIs(TokLParen) {
			return p.parseFunctionCall(name)
		}
		// Otherwise it's an identifier
		return &ColumnRef{Name: name}
	// FTS RANK expression or window function RANK()
	case TokRank:
		// Check if followed by () and OVER - then it's a window function
		if p.peekTok.Type == TokLParen {
			p.nextToken() // consume RANK
			p.nextToken() // consume (
			if !p.expect(TokRParen) {
				return nil
			}
			// Check for OVER clause
			if p.curTokenIs(TokOver) {
				return p.parseWindowFunction(&FunctionCall{Name: "RANK"}, false, false)
			}
			// RANK() without OVER - return as function call
			return &FunctionCall{Name: "RANK"}
		}
		return p.parseRankExpr()
	default:
		p.error("unexpected token in expression: %s", p.currTok.Type)
		return nil
	}
}

// parseIdentOrFunction parses an identifier or function call.
func (p *Parser) parseIdentOrFunction() Expression {
	name := p.currTok.Value
	p.nextToken()

	// Check for function call
	if p.curTokenIs(TokLParen) {
		return p.parseFunctionCall(name)
	}

	// Check for table.column
	if p.curTokenIs(TokDot) {
		p.nextToken()
		if p.curTokenIs(TokStar) {
			p.nextToken()
			return &StarExpr{Table: name}
		}
		if !p.isIdentOrKeyword() {
			p.error("expected column name after dot")
			return nil
		}
		colName := p.currTok.Value
		p.nextToken()
		return &ColumnRef{Table: name, Name: colName}
	}

	return &ColumnRef{Name: name}
}

// parseFunctionKeyword parses a function call where the function name is a keyword (COUNT, SUM, etc.).
func (p *Parser) parseFunctionKeyword() Expression {
	name := p.currTok.Type.String()
	p.nextToken()

	// Must be followed by ( for function call
	if !p.curTokenIs(TokLParen) {
		p.error("expected ( after %s", name)
		return nil
	}

	return p.parseFunctionCall(name)
}

// parseFunctionCall parses a function call.
func (p *Parser) parseFunctionCall(name string) Expression {
	p.nextToken() // consume (

	fc := &FunctionCall{Name: name}

	// DISTINCT
	if p.curTokenIs(TokDistinct) {
		fc.Distinct = true
		p.nextToken()
	}

	// Arguments (check for * in COUNT(*))
	if !p.curTokenIs(TokRParen) {
		if p.curTokenIs(TokStar) {
			fc.Args = []Expression{&StarExpr{}}
			p.nextToken()
		} else {
			fc.Args = p.parseExpressionList()
		}
	}

	if !p.expect(TokRParen) {
		return nil
	}

	// Check for FILTER clause (for aggregate functions)
	// Syntax: FILTER (WHERE condition)
	if p.curTokenIs(TokFilter) {
		p.nextToken() // consume FILTER
		if !p.expect(TokLParen) {
			return nil
		}
		if !p.expect(TokWhere) {
			return nil
		}
		fc.Filter = p.parseExpression()
		if fc.Filter == nil {
			return nil
		}
		if !p.expect(TokRParen) {
			return nil
		}
	}

	// Check for IGNORE NULLS or RESPECT NULLS (for window functions like LEAD/LAG)
	ignoreNulls := false
	respectNulls := false
	if p.curTokenIs(TokIgnore) {
		p.nextToken() // consume IGNORE
		if !p.expect(TokNull) {
			return nil
		}
		ignoreNulls = true
	} else if p.curTokenIs(TokRespect) {
		p.nextToken() // consume RESPECT
		if !p.expect(TokNull) {
			return nil
		}
		respectNulls = true
	}

	// Check for OVER clause (window function)
	if p.curTokenIs(TokOver) {
		return p.parseWindowFunction(fc, ignoreNulls, respectNulls)
	}

	return fc
}

// parseWindowFunction parses a window function with OVER clause.
func (p *Parser) parseWindowFunction(fc *FunctionCall, ignoreNulls, respectNulls bool) Expression {
	p.nextToken() // consume OVER

	wfc := &WindowFuncCall{
		Func:         fc,
		Window:       &WindowSpec{},
		IgnoreNulls:  ignoreNulls,
		RespectNulls: respectNulls,
	}

	// Check for named window reference or window specification
	if p.curTokenIs(TokIdent) {
		// Named window reference
		wfc.Window.Name = p.currTok.Value
		p.nextToken()
		return wfc
	}

	// Expect ( for window specification
	if !p.expect(TokLParen) {
		return nil
	}

	// Parse PARTITION BY
	if p.curTokenIs(TokPartition) {
		p.nextToken() // consume PARTITION
		if !p.expect(TokBy) {
			return nil
		}
		wfc.Window.PartitionBy = p.parseExpressionList()
	}

	// Parse ORDER BY
	if p.curTokenIs(TokOrder) {
		p.nextToken() // consume ORDER
		if !p.expect(TokBy) {
			return nil
		}
		wfc.Window.OrderBy = p.parseOrderByItems()
	}

	// Parse frame clause (ROWS/RANGE BETWEEN ... AND ...)
	if p.curTokenIs(TokRows) || p.curTokenIs(TokRange) {
		wfc.Window.Frame = p.parseFrameSpec()
		if wfc.Window.Frame == nil {
			return nil
		}
	}

	if !p.expect(TokRParen) {
		return nil
	}

	return wfc
}

// parseFrameSpec parses a window frame clause.
// Syntax: ROWS BETWEEN bound AND bound
//         RANGE BETWEEN bound AND bound
func (p *Parser) parseFrameSpec() *FrameSpec {
	frame := &FrameSpec{}

	// ROWS or RANGE
	if p.curTokenIs(TokRows) {
		frame.Mode = "ROWS"
	} else {
		frame.Mode = "RANGE"
	}
	p.nextToken() // consume ROWS/RANGE

	// Optional BETWEEN keyword (can be omitted for single bound)
	hasBetween := false
	if p.curTokenIs(TokBetween) {
		hasBetween = true
		p.nextToken() // consume BETWEEN
	}

	// Parse start bound
	frame.Start = p.parseFrameBound()
	if frame.Start.Type == "" {
		return nil
	}

	// If BETWEEN was specified, require AND and end bound
	if hasBetween {
		if !p.expect(TokAnd) {
			return nil
		}
		frame.End = p.parseFrameBound()
		if frame.End.Type == "" {
			return nil
		}
	} else {
		// Single bound form: "ROWS 1 PRECEDING" means "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING"
		frame.End = FrameBound{Type: "CURRENT ROW"}
	}

	return frame
}

// parseFrameBound parses a single frame bound.
// Syntax: UNBOUNDED PRECEDING | n PRECEDING | CURRENT ROW | n FOLLOWING | UNBOUNDED FOLLOWING
func (p *Parser) parseFrameBound() FrameBound {
	bound := FrameBound{}

	if p.curTokenIs(TokUnbounded) {
		p.nextToken() // consume UNBOUNDED
		if p.curTokenIs(TokPreceding) {
			bound.Type = "UNBOUNDED PRECEDING"
			p.nextToken()
		} else if p.curTokenIs(TokFollowing) {
			bound.Type = "UNBOUNDED FOLLOWING"
			p.nextToken()
		} else {
			p.error("expected PRECEDING or FOLLOWING after UNBOUNDED")
			return FrameBound{}
		}
	} else if p.curTokenIs(TokCurrent) {
		p.nextToken() // consume CURRENT
		if !p.expect(TokRow) {
			return FrameBound{}
		}
		bound.Type = "CURRENT ROW"
	} else if p.curTokenIs(TokNumber) {
		// Numeric offset
		offset := p.currTok.Value
		p.nextToken()
		offsetInt, err := strconv.Atoi(offset)
		if err != nil {
			p.error("invalid frame offset: %s", offset)
			return FrameBound{}
		}
		if p.curTokenIs(TokPreceding) {
			bound.Type = "PRECEDING"
			bound.Offset = offsetInt
			p.nextToken()
		} else if p.curTokenIs(TokFollowing) {
			bound.Type = "FOLLOWING"
			bound.Offset = offsetInt
			p.nextToken()
		} else {
			p.error("expected PRECEDING or FOLLOWING after offset")
			return FrameBound{}
		}
	} else {
		p.error("expected frame bound (UNBOUNDED, CURRENT, or number)")
		return FrameBound{}
	}

	return bound
}

// parseNumber parses a number literal.
func (p *Parser) parseNumber() *Literal {
	val := p.currTok.Value
	p.nextToken()

	// Check for hexadecimal format (0x...)
	if len(val) >= 2 && val[0] == '0' && (val[1] == 'x' || val[1] == 'X') {
		// Parse as hexadecimal
		hexStr := val[2:]
		if i, err := strconv.ParseInt(hexStr, 16, 64); err == nil {
			return &Literal{Type: LiteralNumber, Value: i}
		}
		p.error("invalid hexadecimal number: %s", val)
		return nil
	}

	// Try to parse as integer first
	if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return &Literal{Type: LiteralNumber, Value: i}
	}

	// Parse as float
	f, err := strconv.ParseFloat(val, 64)
	if err != nil {
		p.error("invalid number: %s", val)
		return nil
	}
	return &Literal{Type: LiteralNumber, Value: f}
}

// parseString parses a string literal.
func (p *Parser) parseString() *Literal {
	val := p.currTok.Value
	p.nextToken()
	return &Literal{Type: LiteralString, Value: val}
}

// parseBool parses a boolean literal.
func (p *Parser) parseBool() *Literal {
	val := p.currTok.Value == "TRUE"
	p.nextToken()
	return &Literal{Type: LiteralBool, Value: val}
}

// parseParenExpr parses a parenthesized expression or subquery.
func (p *Parser) parseParenExpr() Expression {
	p.nextToken() // consume (

	// Check for subquery
	if p.curTokenIs(TokSelect) {
		stmt := p.parseSelect()
		if !p.expect(TokRParen) {
			return nil
		}
		// Return the subquery as a SubqueryExpr (for IN subqueries)
		return &SubqueryExpr{Select: stmt.(*SelectStmt)}
	}

	expr := p.parseExpression()
	if !p.expect(TokRParen) {
		return nil
	}

	return &ParenExpr{Expr: expr}
}

// parseExistsExpr parses an EXISTS expression.
// EXISTS (SELECT ...)
func (p *Parser) parseExistsExpr() *ExistsExpr {
	p.nextToken() // consume EXISTS

	// Expect (
	if !p.expect(TokLParen) {
		return nil
	}

	// Parse the subquery
	if !p.curTokenIs(TokSelect) {
		p.error("expected SELECT after EXISTS (")
		return nil
	}

	stmt := p.parseSelect()
	if stmt == nil {
		return nil
	}

	// Expect )
	if !p.expect(TokRParen) {
		return nil
	}

	return &ExistsExpr{
		Subquery: &SubqueryExpr{Select: stmt.(*SelectStmt)},
	}
}

// parseCaseExpr parses a CASE expression.
func (p *Parser) parseCaseExpr() *CaseExpr {
	p.nextToken() // consume CASE

	ce := &CaseExpr{}

	// Optional operand
	if !p.curTokenIs(TokWhen) {
		ce.Expr = p.parseExpression()
	}

	// WHEN clauses
	for p.curTokenIs(TokWhen) {
		p.nextToken()
		cond := p.parseExpression()
		if !p.expect(TokThen) {
			return nil
		}
		result := p.parseExpression()
		ce.Whens = append(ce.Whens, &CaseWhen{Condition: cond, Result: result})
	}

	// ELSE
	if p.curTokenIs(TokElse) {
		p.nextToken()
		ce.Else = p.parseExpression()
	}

	if !p.expect(TokEnd) {
		return nil
	}

	return ce
}

// parseCastExpr parses a CAST expression.
func (p *Parser) parseCastExpr() *CastExpr {
	p.nextToken() // consume CAST

	if !p.expect(TokLParen) {
		return nil
	}

	expr := p.parseExpression()

	if !p.expect(TokAs) {
		return nil
	}

	dataType := p.parseDataType()

	if !p.expect(TokRParen) {
		return nil
	}

	return &CastExpr{Expr: expr, Type: dataType}
}

// parseExpressionList parses a comma-separated list of expressions.
func (p *Parser) parseExpressionList() []Expression {
	var exprs []Expression

	for {
		expr := p.parseExpression()
		if expr == nil {
			break
		}
		exprs = append(exprs, expr)

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return exprs
}

// parseIdentifierList parses a comma-separated list of identifiers.
func (p *Parser) parseIdentifierList() []string {
	var ids []string

	for {
		if !p.isIdentOrKeyword() {
			break
		}
		ids = append(ids, p.currTok.Value)
		p.nextToken()

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return ids
}

// getBinaryOp returns the binary operator for the current token.
func (p *Parser) getBinaryOp() BinaryOp {
	switch p.currTok.Type {
	case TokEq:
		return OpEq
	case TokNe:
		return OpNe
	case TokLt:
		return OpLt
	case TokLe:
		return OpLe
	case TokGt:
		return OpGt
	case TokGe:
		return OpGe
	case TokPlus:
		return OpAdd
	case TokMinus:
		return OpSub
	case TokStar:
		return OpMul
	case TokSlash:
		return OpDiv
	case TokPercent:
		return OpMod
	case TokAnd:
		return OpAnd
	case TokOr:
		return OpOr
	case TokLike:
		return OpLike
	case TokGlob:
		return OpGlob
	case TokConcat:
		return OpConcat
	case TokIn:
		return OpIn
	case TokBetween:
		// Handled separately
		return OpAnd
	case TokIs:
		// Handled separately
		return OpEq
	}
	return OpEq // default
}

// getPrecedence returns the precedence of a binary operator.
func getPrecedence(op BinaryOp) int {
	switch op {
	case OpOr:
		return 1
	case OpAnd:
		return 2
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpLike, OpGlob, OpIn:
		return 3
	case OpAdd, OpSub, OpConcat:
		return 4
	case OpMul, OpDiv, OpMod:
		return 5
	}
	return 0
}

// isJoinToken checks if the token is a join keyword.
func isJoinToken(t TokenType) bool {
	return t == TokJoin || t == TokInner || t == TokLeft || t == TokRight || t == TokCross || t == TokFull
}

// ============================================================================
// Auth Statement Parsing
// ============================================================================

// parseGrant parses a GRANT statement.
func (p *Parser) parseGrant() *GrantStmt {
	p.nextToken() // consume GRANT

	stmt := &GrantStmt{}

	// Parse privileges
	stmt.Privileges = p.parsePrivileges()

	// ON clause
	if !p.expect(TokOn) {
		return nil
	}

	// Parse object: *.*, db.*, or db.table
	stmt.On, stmt.Database, stmt.Table = p.parseGrantObject()

	// TO clause
	if !p.expect(TokTo) {
		return nil
	}

	// Parse user
	stmt.To, stmt.Host = p.parseUserHost()

	// WITH GRANT OPTION
	if p.curTokenIs(TokWith) {
		p.nextToken()
		if !p.expect(TokGrant) {
			return nil
		}
		if !p.expect(TokOption) {
			return nil
		}
		stmt.WithGrant = true
	}

	return stmt
}

// parseRevoke parses a REVOKE statement.
func (p *Parser) parseRevoke() *RevokeStmt {
	p.nextToken() // consume REVOKE

	stmt := &RevokeStmt{}

	// Parse privileges
	stmt.Privileges = p.parsePrivileges()

	// ON clause
	if !p.expect(TokOn) {
		return nil
	}

	// Parse object
	stmt.On, stmt.Database, stmt.Table = p.parseGrantObject()

	// FROM clause
	if !p.expect(TokFrom) {
		return nil
	}

	// Parse user
	stmt.From, stmt.Host = p.parseUserHost()

	return stmt
}

// parsePrivileges parses a list of privileges.
func (p *Parser) parsePrivileges() []*Privilege {
	var privileges []*Privilege

	// ALL or ALL PRIVILEGES
	if p.curTokenIs(TokAll) {
		p.nextToken()
		if p.curTokenIs(TokPrivileges) {
			p.nextToken()
		}
		privileges = append(privileges, &Privilege{Type: PrivAll})
		return privileges
	}

	// List of specific privileges
	for {
		priv := p.parsePrivilege()
		if priv == nil {
			break
		}
		privileges = append(privileges, priv)

		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	return privileges
}

// parsePrivilege parses a single privilege.
func (p *Parser) parsePrivilege() *Privilege {
	priv := &Privilege{}

	// Map keyword to privilege type
	switch p.currTok.Type {
	case TokSelect:
		priv.Type = PrivSelect
	case TokInsert:
		priv.Type = PrivInsert
	case TokUpdate:
		priv.Type = PrivUpdate
	case TokDelete:
		priv.Type = PrivDelete
	case TokCreate:
		priv.Type = PrivCreate
	case TokDrop:
		priv.Type = PrivDrop
	case TokIndex:
		priv.Type = PrivIndex
	case TokAlter:
		priv.Type = PrivAlter
	case TokUse:
		priv.Type = PrivUsage
	default:
		p.error("expected privilege type, got %s", p.currTok.Type)
		return nil
	}
	p.nextToken()

	// Optional column list
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		priv.Columns = p.parseIdentifierList()
		if !p.expect(TokRParen) {
			return nil
		}
	}

	return priv
}

// parseGrantObject parses the object in GRANT/REVOKE (*.*, db.*, or db.table).
func (p *Parser) parseGrantObject() (GrantOn, string, string) {
	// Check for *.* (global)
	if p.curTokenIs(TokStar) {
		p.nextToken()
		if !p.expect(TokDot) {
			return GrantOnAll, "", ""
		}
		if !p.expect(TokStar) {
			return GrantOnAll, "", ""
		}
		return GrantOnAll, "", ""
	}

	// Database or table name
	name := p.currTok.Value
	p.nextToken()

	if !p.expect(TokDot) {
		// Just a table name (default database)
		return GrantOnTable, "", name
	}

	// Check for db.*
	if p.curTokenIs(TokStar) {
		p.nextToken()
		return GrantOnDatabase, name, ""
	}

	// db.table
	table := p.currTok.Value
	p.nextToken()
	return GrantOnTable, name, table
}

// parseUserHost parses a user@host specification.
func (p *Parser) parseUserHost() (string, string) {
	username := p.currTok.Value
	p.nextToken()

	host := "%"
	if p.curTokenIs(TokAt) {
		p.nextToken()
		host = p.currTok.Value
		p.nextToken()
	}

	return username, host
}

// parseSet parses a SET statement.
func (p *Parser) parseSet() Statement {
	p.nextToken() // consume SET

	// Check for SET PASSWORD
	if p.curTokenIs(TokPassword) {
		return p.parseSetPassword()
	}

	p.error("expected PASSWORD after SET")
	return nil
}

// parseSetPassword parses a SET PASSWORD statement.
func (p *Parser) parseSetPassword() *SetPasswordStmt {
	p.nextToken() // consume PASSWORD

	stmt := &SetPasswordStmt{}

	// FOR user clause
	if p.curTokenIs(TokFor) {
		p.nextToken()
		stmt.ForUser = p.currTok.Value
		p.nextToken()

		if p.curTokenIs(TokAt) {
			p.nextToken()
			stmt.ForHost = p.currTok.Value
			p.nextToken()
		}
	}

	// = 'password'
	if !p.expect(TokEq) {
		return nil
	}

	if !p.curTokenIs(TokString) {
		p.error("expected password string")
		return nil
	}
	stmt.Password = p.currTok.Value
	p.nextToken()

	return stmt
}

// parseBackup parses a BACKUP DATABASE statement.
func (p *Parser) parseBackup() *BackupStmt {
	p.nextToken() // consume BACKUP

	// Expect DATABASE
	if !p.expect(TokDatabase) && !p.curTokenIs(TokTo) {
		p.error("expected DATABASE or TO")
		return nil
	}

	// Skip DATABASE if present
	if p.curTokenIs(TokDatabase) {
		p.nextToken()
	}

	// Expect TO
	if !p.expect(TokTo) {
		return nil
	}

	// Path string
	if !p.curTokenIs(TokString) {
		p.error("expected backup path string")
		return nil
	}

	stmt := &BackupStmt{
		Path: p.currTok.Value,
	}
	p.nextToken()

	// Optional WITH COMPRESS
	if p.curTokenIs(TokWith) {
		p.nextToken()
		// Check for COMPRESS (we treat it as an identifier since it's not a keyword)
		if p.curTokenIs(TokIdent) && p.currTok.Value == "COMPRESS" {
			stmt.Compress = true
			p.nextToken()
		}
	}

	return stmt
}

// parseRestore parses a RESTORE DATABASE statement.
func (p *Parser) parseRestore() *RestoreStmt {
	p.nextToken() // consume RESTORE

	// Expect DATABASE (optional)
	if p.curTokenIs(TokDatabase) {
		p.nextToken()
	}

	// Expect FROM
	if !p.expect(TokFrom) {
		return nil
	}

	// Path string
	if !p.curTokenIs(TokString) {
		p.error("expected backup path string")
		return nil
	}

	stmt := &RestoreStmt{
		Path: p.currTok.Value,
	}
	p.nextToken()

	return stmt
}

// parseVacuum parses a VACUUM statement.
// Syntax: VACUUM [table_name] [INTO 'filename']
func (p *Parser) parseVacuum() *VacuumStmt {
	p.nextToken() // consume VACUUM

	stmt := &VacuumStmt{}

	// Check for table name (identifier) or INTO
	if p.curTokenIs(TokIdent) {
		stmt.Table = p.currTok.Value
		p.nextToken()
	}

	// Check for INTO clause
	if p.curTokenIs(TokInto) {
		p.nextToken()
		if !p.curTokenIs(TokString) {
			p.error("expected file path string after INTO")
			return nil
		}
		stmt.IntoPath = p.currTok.Value
		p.nextToken()
	}

	return stmt
}

// parsePragma parses a PRAGMA statement.
// Syntax: PRAGMA name [= value]
func (p *Parser) parsePragma() *PragmaStmt {
	p.nextToken() // consume PRAGMA

	// Get pragma name
	if !p.curTokenIs(TokIdent) {
		p.error("expected pragma name")
		return nil
	}

	stmt := &PragmaStmt{
		Name: p.currTok.Value,
	}
	p.nextToken()

	// Check for = value
	if p.curTokenIs(TokEq) {
		p.nextToken()

		switch {
		case p.curTokenIs(TokString):
			stmt.Value = p.currTok.Value
			p.nextToken()
		case p.curTokenIs(TokInt), p.curTokenIs(TokNumber):
			// Parse the string value as int64
			var intVal int64
			_, err := fmt.Sscanf(p.currTok.Value, "%d", &intVal)
			if err == nil {
				stmt.Value = intVal
			} else {
				stmt.Value = p.currTok.Value
			}
			p.nextToken()
		case p.curTokenIs(TokBoolLit):
			// TRUE or FALSE literal
			stmt.Value = strings.ToUpper(p.currTok.Value) == "TRUE"
			p.nextToken()
		case p.curTokenIs(TokOn):
			// ON keyword - treat as boolean true
			stmt.Value = true
			p.nextToken()
		case p.curTokenIs(TokIdent):
			// Could be OFF, YES, NO, or other keywords
			val := p.currTok.Value
			p.nextToken()
			switch strings.ToUpper(val) {
			case "ON", "TRUE", "YES", "1":
				stmt.Value = true
			case "OFF", "FALSE", "NO", "0":
				stmt.Value = false
			default:
				stmt.Value = val
			}
		default:
			p.error("expected pragma value")
			return nil
		}
	}

	return stmt
}

// parseCreateUser parses a CREATE USER statement.
func (p *Parser) parseCreateUser() *CreateUserStmt {
	p.nextToken() // consume USER

	stmt := &CreateUserStmt{}

	// IF NOT EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokNot) {
			return nil
		}
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfNotExists = true
	}

	// Username
	stmt.Username = p.currTok.Value
	p.nextToken()

	// @host
	if p.curTokenIs(TokAt) {
		p.nextToken()
		stmt.Host = p.currTok.Value
		p.nextToken()
	}

	// IDENTIFIED BY 'password'
	if p.curTokenIs(TokIdentified) {
		p.nextToken()
		if !p.expect(TokBy) {
			return nil
		}
		if !p.curTokenIs(TokString) {
			p.error("expected password string")
			return nil
		}
		stmt.Identified = p.currTok.Value
		p.nextToken()
	}

	// ROLE admin|user
	if p.curTokenIs(TokRole) {
		p.nextToken()
		stmt.Role = p.currTok.Value
		p.nextToken()
	}

	return stmt
}

// parseDropUser parses a DROP USER statement.
func (p *Parser) parseDropUser() *DropUserStmt {
	p.nextToken() // consume USER

	stmt := &DropUserStmt{}

	// IF EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfExists = true
	}

	// Username
	stmt.Username = p.currTok.Value
	p.nextToken()

	// @host
	if p.curTokenIs(TokAt) {
		p.nextToken()
		stmt.Host = p.currTok.Value
		p.nextToken()
	}

	return stmt
}

// parseAlterUser parses an ALTER USER statement.
func (p *Parser) parseAlterUser() *AlterUserStmt {
	p.nextToken() // consume USER

	stmt := &AlterUserStmt{}

	// Username
	stmt.Username = p.currTok.Value
	p.nextToken()

	// @host
	if p.curTokenIs(TokAt) {
		p.nextToken()
		stmt.Host = p.currTok.Value
		p.nextToken()
	}

	// IDENTIFIED BY 'password'
	if p.curTokenIs(TokIdentified) {
		p.nextToken()
		if !p.expect(TokBy) {
			return nil
		}
		if !p.curTokenIs(TokString) {
			p.error("expected password string")
			return nil
		}
		stmt.Identified = p.currTok.Value
		p.nextToken()
	}

	return stmt
}

// parseShowGrants parses a SHOW GRANTS statement.
func (p *Parser) parseShowGrants() *ShowGrantsStmt {
	p.nextToken() // consume GRANTS

	stmt := &ShowGrantsStmt{}

	// FOR user
	if p.curTokenIs(TokFor) {
		p.nextToken()
		stmt.ForUser = p.currTok.Value
		p.nextToken()

		if p.curTokenIs(TokAt) {
			p.nextToken()
			stmt.ForHost = p.currTok.Value
			p.nextToken()
		}
	}

	return stmt
}

// ============================================================================
// User Defined Function Parsing
// ============================================================================

// parseCreateFunction parses a CREATE FUNCTION statement.
// Syntax: CREATE FUNCTION name(param1 TYPE, param2 TYPE) RETURNS TYPE RETURN expression
// Current token should be FUNCTION keyword.
func (p *Parser) parseCreateFunction() *CreateFunctionStmt {
	p.nextToken() // consume FUNCTION keyword

	stmt := &CreateFunctionStmt{
		Name: p.currTok.Value,
	}
	p.nextToken() // consume function name

	// Parameters
	if !p.expect(TokLParen) {
		return nil
	}

	for !p.curTokenIs(TokRParen) && p.err == nil {
		param := &FunctionParameter{
			Name: p.currTok.Value,
		}
		p.nextToken()

		// Parse type
		param.Type = p.parseDataType()
		if param.Type == nil {
			p.error("expected data type for parameter %s", param.Name)
			return nil
		}

		// Optional DEFAULT value
		if p.curTokenIs(TokDefault) {
			p.nextToken()
			param.DefaultValue = p.parseExpression()
			if param.DefaultValue == nil {
				p.error("expected default value")
				return nil
			}
		}

		stmt.Parameters = append(stmt.Parameters, param)

		if p.curTokenIs(TokComma) {
			p.nextToken()
		}
	}

	if !p.expect(TokRParen) {
		return nil
	}

	// RETURNS
	if !p.expect(TokReturns) {
		return nil
	}

	// Return type
	stmt.ReturnType = p.parseDataType()
	if stmt.ReturnType == nil {
		p.error("expected return type")
		return nil
	}

	// Function body: either RETURN expr or BEGIN ... END block
	if p.curTokenIs(TokReturn) {
		// Simple: RETURN expression
		p.nextToken()
		stmt.Body = p.parseExpression()
		if stmt.Body == nil {
			p.error("expected function body expression")
			return nil
		}
	} else if p.curTokenIs(TokBegin) {
		// BEGIN ... END block
		stmt.Body = p.parseBlockExpr()
		if stmt.Body == nil {
			p.error("expected function body")
			return nil
		}
	} else {
		p.error("expected RETURN or BEGIN for function body")
		return nil
	}

	return stmt
}

// parseDropFunction parses a DROP FUNCTION statement.
func (p *Parser) parseDropFunction() *DropFunctionStmt {
	p.nextToken() // consume FUNCTION

	stmt := &DropFunctionStmt{}

	// IF EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfExists = true
	}

	stmt.Name = p.currTok.Value
	p.nextToken()

	return stmt
}

// parseCreateTrigger parses a CREATE TRIGGER statement.
// Syntax: CREATE TRIGGER [IF NOT EXISTS] name {BEFORE|AFTER|INSTEAD OF} {INSERT|UPDATE|DELETE} ON table [FOR EACH ROW] [WHEN condition] BEGIN statements END
func (p *Parser) parseCreateTrigger() *CreateTriggerStmt {
	p.nextToken() // consume TRIGGER

	stmt := &CreateTriggerStmt{
		Granularity: TriggerForEachRow, // default to FOR EACH ROW
	}

	// IF NOT EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokNot) {
			return nil
		}
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfNotExists = true
	}

	// Trigger name
	if !p.curTokenIs(TokIdent) {
		p.error("expected trigger name")
		return nil
	}
	stmt.TriggerName = p.currTok.Value
	p.nextToken()

	// Timing: BEFORE, AFTER, or INSTEAD OF
	if p.curTokenIs(TokBefore) {
		stmt.Timing = TriggerBefore
		p.nextToken()
	} else if p.curTokenIs(TokAfter) {
		stmt.Timing = TriggerAfter
		p.nextToken()
	} else if p.curTokenIs(TokInstead) {
		p.nextToken()
		if !p.expect(TokOf) {
			return nil
		}
		stmt.Timing = TriggerInsteadOf
	} else {
		p.error("expected BEFORE, AFTER, or INSTEAD OF")
		return nil
	}

	// Event: INSERT, UPDATE, or DELETE
	if p.curTokenIs(TokInsert) {
		stmt.Event = TriggerInsert
		p.nextToken()
	} else if p.curTokenIs(TokUpdate) {
		stmt.Event = TriggerUpdate
		p.nextToken()
	} else if p.curTokenIs(TokDelete) {
		stmt.Event = TriggerDelete
		p.nextToken()
	} else {
		p.error("expected INSERT, UPDATE, or DELETE")
		return nil
	}

	// ON table
	if !p.expect(TokOn) {
		return nil
	}
	if !p.isIdentOrKeyword() {
		p.error("expected table name")
		return nil
	}
	stmt.TableName = p.currTok.Value
	p.nextToken()

	// FOR EACH ROW or FOR EACH STATEMENT (optional)
	if p.curTokenIs(TokFor) {
		p.nextToken()
		if !p.expect(TokEach) {
			return nil
		}
		if p.curTokenIs(TokRow) {
			stmt.Granularity = TriggerForEachRow
			p.nextToken()
		} else if p.curTokenIs(TokStatement) {
			stmt.Granularity = TriggerForEachStatement
			p.nextToken()
		} else {
			p.error("expected ROW or STATEMENT after FOR EACH")
			return nil
		}
	}

	// WHEN clause (optional)
	if p.curTokenIs(TokWhen) {
		p.nextToken()
		stmt.WhenClause = p.parseExpression()
		if stmt.WhenClause == nil {
			return nil
		}
	}

	// BEGIN ... END block
	if !p.expect(TokBegin) {
		return nil
	}

	// Parse statements until END
	for !p.curTokenIs(TokEnd) && p.err == nil {
		s := p.parseStatement()
		if s != nil {
			stmt.Body = append(stmt.Body, s)
		}
		// Skip optional semicolon
		if p.curTokenIs(TokSemi) {
			p.nextToken()
		}
	}

	if !p.expect(TokEnd) {
		return nil
	}

	return stmt
}

// parseDropTrigger parses a DROP TRIGGER statement.
func (p *Parser) parseDropTrigger() *DropTriggerStmt {
	p.nextToken() // consume TRIGGER

	stmt := &DropTriggerStmt{}

	// IF EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfExists = true
	}

	// Trigger name
	if !p.curTokenIs(TokIdent) {
		p.error("expected trigger name")
		return nil
	}
	stmt.TriggerName = p.currTok.Value
	p.nextToken()

	// Optional ON table (MySQL syntax)
	if p.curTokenIs(TokOn) {
		p.nextToken()
		if p.curTokenIs(TokIdent) {
			stmt.TableName = p.currTok.Value
			p.nextToken()
		}
	}

	return stmt
}

// parseDropFTS parses a DROP FTS INDEX statement.
// Syntax: DROP FTS INDEX [IF EXISTS] name
func (p *Parser) parseDropFTS() *DropFTSStmt {
	p.nextToken() // consume FTS

	// Expect INDEX
	if !p.expect(TokIndex) {
		return nil
	}

	stmt := &DropFTSStmt{}

	// IF EXISTS
	if p.curTokenIs(TokIf) {
		p.nextToken()
		if !p.expect(TokExists) {
			return nil
		}
		stmt.IfExists = true
	}

	// Index name
	if !p.isIdentOrKeyword() {
		p.error("expected FTS index name")
		return nil
	}
	stmt.IndexName = p.currTok.Value
	p.nextToken()

	return stmt
}

// ============================================================================
// UDF Expression Parsing
// ============================================================================

// parseIfExpr parses an IF expression.
// Syntax: IF condition THEN expr [ELSE expr] END
func (p *Parser) parseIfExpr() *IfExpr {
	p.nextToken() // consume IF

	expr := &IfExpr{}

	// Parse condition
	expr.Condition = p.parseExpression()
	if expr.Condition == nil {
		p.error("expected condition after IF")
		return nil
	}

	// THEN
	if !p.expect(TokThen) {
		return nil
	}

	// Parse THEN expression
	expr.ThenExpr = p.parseExpression()
	if expr.ThenExpr == nil {
		p.error("expected expression after THEN")
		return nil
	}

	// Optional ELSE
	if p.curTokenIs(TokElse) {
		p.nextToken()
		expr.ElseExpr = p.parseExpression()
		if expr.ElseExpr == nil {
			p.error("expected expression after ELSE")
			return nil
		}
	}

	// END
	if !p.expect(TokEnd) {
		return nil
	}

	return expr
}

// parseLetExpr parses a LET expression.
// Syntax: LET name = expr
func (p *Parser) parseLetExpr() *LetExpr {
	p.nextToken() // consume LET

	if !p.curTokenIs(TokIdent) {
		p.error("expected variable name after LET")
		return nil
	}

	expr := &LetExpr{
		Name: p.currTok.Value,
	}
	p.nextToken()

	// =
	if !p.expect(TokEq) {
		return nil
	}

	// Parse value expression
	expr.Value = p.parseExpression()
	if expr.Value == nil {
		p.error("expected expression after =")
		return nil
	}

	return expr
}

// parseBlockExpr parses a BEGIN ... END block.
// Syntax: BEGIN expr [; expr]* END
func (p *Parser) parseBlockExpr() *BlockExpr {
	p.nextToken() // consume BEGIN

	expr := &BlockExpr{}

	for !p.curTokenIs(TokEnd) && p.err == nil {
		e := p.parseExpression()
		if e == nil {
			break
		}
		expr.Expressions = append(expr.Expressions, e)

		// Optional semicolon separator
		if p.curTokenIs(TokSemi) {
			p.nextToken()
		} else if !p.curTokenIs(TokEnd) {
			// If no semicolon and not END, it's an error
			break
		}
	}

	if !p.expect(TokEnd) {
		return nil
	}

	return expr
}

// parseRankExpr parses a RANK expression for FTS.
// Syntax: RANK or RANK(index_name)
func (p *Parser) parseRankExpr() *RankExpr {
	p.nextToken() // consume RANK

	expr := &RankExpr{}

	// Check for optional parentheses with index name
	if p.curTokenIs(TokLParen) {
		p.nextToken()
		if p.isIdentOrKeyword() {
			expr.IndexName = p.currTok.Value
			p.nextToken()
		}
		if !p.expect(TokRParen) {
			return nil
		}
	}

	return expr
}