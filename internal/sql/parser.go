package sql

import (
	"fmt"
	"strconv"
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
	switch p.currTok.Type {
	case TokWith:
		return p.parseWith()
	case TokSelect:
		return p.parseSelect()
	case TokInsert:
		return p.parseInsert()
	case TokUpdate:
		return p.parseUpdate()
	case TokDelete:
		return p.parseDelete()
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
	case TokLParen:
		// Could be a parenthesized SELECT
		return p.parseSelect()
	default:
		p.error("unexpected token: %s", p.currTok.Type)
		return nil
	}
}

// parseWith parses a WITH clause (CTE) statement.
// Syntax: WITH [RECURSIVE] cte_name [(col1, col2, ...)] AS (query) [, ...] main_query
func (p *Parser) parseWith() Statement {
	p.nextToken() // consume WITH

	withStmt := &WithStmt{}

	// Check for RECURSIVE keyword (applies to all CTEs in the WITH clause)
	hasRecursive := false
	if p.curTokenIs(TokRecursive) {
		hasRecursive = true
		p.nextToken()
	}

	// Parse CTE definitions
	for {
		cte := CTEDefinition{Recursive: hasRecursive}

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
				if !p.curTokenIs(TokIdent) {
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

		withStmt.CTEs = append(withStmt.CTEs, cte)

		// Check for more CTEs
		if !p.curTokenIs(TokComma) {
			break
		}
		p.nextToken()
	}

	// Parse the main query
	withStmt.MainQuery = p.parseStatement()
	if withStmt.MainQuery == nil {
		return nil
	}

	return withStmt
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
		if p.curTokenIs(TokIdent) {
			setAlias(expr, p.currTok.Value)
			p.nextToken()
		}
	} else if p.curTokenIs(TokIdent) {
		// Implicit alias (without AS)
		setAlias(expr, p.currTok.Value)
		p.nextToken()
	}

	return expr
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

	// Check for subquery: (SELECT ...) AS alias
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
		p.error("expected SELECT or table name")
		return nil
	}

	// Regular table name
	if !p.curTokenIs(TokIdent) {
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

		if p.curTokenIs(TokAsc) {
			p.nextToken()
		} else if p.curTokenIs(TokDesc) {
			item.Ascending = false
			p.nextToken()
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
	// INTERSECT ALL and EXCEPT ALL are not standard SQL, but some databases support them
	// We'll support ALL only for UNION as per standard
	if op == SetUnion && p.curTokenIs(TokAll) {
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

	return stmt
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

	return stmt
}

// parseCreate parses a CREATE statement.
func (p *Parser) parseCreate() Statement {
	p.nextToken() // consume CREATE

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
	case TokOr:
		// CREATE OR REPLACE FUNCTION
		p.nextToken()
		if !p.expect(TokReplace) {
			return nil
		}
		if !p.expect(TokFunction) {
			return nil
		}
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
	case TokFunction:
		return p.parseCreateFunction()
	default:
		p.error("expected TABLE, INDEX, USER or FUNCTION after CREATE")
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

	// Column name
	if !p.curTokenIs(TokIdent) {
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

// parseDrop parses a DROP statement.
func (p *Parser) parseDrop() Statement {
	p.nextToken() // consume DROP

	switch p.currTok.Type {
	case TokTable:
		return p.parseDropTable()
	case TokIndex:
		return p.parseDropIndex()
	case TokUser:
		return p.parseDropUser()
	case TokFunction:
		return p.parseDropFunction()
	default:
		p.error("expected TABLE, INDEX, USER or FUNCTION after DROP")
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
	if !p.curTokenIs(TokIdent) {
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

	if !p.curTokenIs(TokIdent) {
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

		if !p.curTokenIs(TokIdent) {
			p.error("expected old column name")
			return nil
		}
		oldName := p.currTok.Value
		p.nextToken()

		if !p.expect(TokTo) {
			return nil
		}

		if !p.curTokenIs(TokIdent) {
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
		TokAnd, TokOr, TokLike, TokConcat, TokIn, TokBetween, TokIs:
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

	return p.parsePrimaryExpr()
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
		TokLeft, TokRight, TokReplace: // LEFT/RIGHT/REPLACE can be keywords or function names
		name := p.currTok.Value
		p.nextToken()
		if p.curTokenIs(TokLParen) {
			return p.parseFunctionCall(name)
		}
		// Otherwise it's an identifier
		return &ColumnRef{Name: name}
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
		if !p.curTokenIs(TokIdent) {
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

	// Check for OVER clause (window function)
	if p.curTokenIs(TokOver) {
		return p.parseWindowFunction(fc)
	}

	return fc
}

// parseWindowFunction parses a window function with OVER clause.
func (p *Parser) parseWindowFunction(fc *FunctionCall) Expression {
	p.nextToken() // consume OVER

	wfc := &WindowFuncCall{
		Func:   fc,
		Window: &WindowSpec{},
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

	if !p.expect(TokRParen) {
		return nil
	}

	return wfc
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
		if !p.curTokenIs(TokIdent) {
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
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpLike, OpIn:
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