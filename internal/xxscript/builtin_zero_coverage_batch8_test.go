package xxscript

import (
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeScriptExecutor struct {
	result interface{}
	err    error
}

func (f fakeScriptExecutor) ExecuteForScript(query string) (interface{}, error) {
	return f.result, f.err
}

func TestBuiltin_ZeroCoverage_Batch8_TableCSVPrintfAndExec(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinTable([]Value{}); got != "" {
		t.Fatalf("expected empty table for no args, got %v", got)
	}
	if got := i.builtinTable([]Value{123}); got != "" {
		t.Fatalf("expected empty table for invalid type, got %v", got)
	}
	table := i.builtinTable([]Value{[]Value{[]Value{"name", "age"}, []Value{"alice", int64(30)}}})
	tableStr, ok := table.(string)
	if !ok || !strings.Contains(tableStr, "alice") || !strings.Contains(tableStr, "+") {
		t.Fatalf("expected rendered table output, got %T (%v)", table, table)
	}

	if got := i.builtinCSV([]Value{}); got != "" {
		t.Fatalf("expected empty csv for no args, got %v", got)
	}
	csvOut := i.builtinCSV([]Value{[]Value{[]Value{"a,b", "x\"y"}, []Value{"1", "2"}}})
	csvStr, ok := csvOut.(string)
	if !ok || !strings.Contains(csvStr, "\"a,b\"") || !strings.Contains(csvStr, "\"x\"\"y\"") {
		t.Fatalf("expected quoted csv values, got %T (%v)", csvOut, csvOut)
	}

	if got := i.builtinCSVParse([]Value{}); len(got.([]Value)) != 0 {
		t.Fatalf("expected empty csv parse for no args, got %v", got)
	}
	parsed := i.builtinCSVParse([]Value{"\"a,b\",c\n\"d\"\"e\",f"})
	rows, ok := parsed.([]Value)
	if !ok || len(rows) != 2 {
		t.Fatalf("expected two parsed rows, got %T (%v)", parsed, parsed)
	}
	first, ok := rows[0].([]Value)
	if !ok || first[0] != "a,b" || first[1] != "c" {
		t.Fatalf("unexpected first parsed row: %v", rows[0])
	}

	if got := i.builtinPrintf([]Value{}); got != nil {
		t.Fatalf("expected nil printf result for no args, got %v", got)
	}
	if got := i.builtinPrintf([]Value{int64(1)}); got != nil {
		t.Fatalf("expected nil printf result for non-string format, got %v", got)
	}
	rr := httptest.NewRecorder()
	i.ctx.HTTPWriter = rr
	i.builtinPrintf([]Value{"hello %s", "world"})
	if body := rr.Body.String(); !strings.Contains(body, "hello world") {
		t.Fatalf("expected printf to write formatted body, got %q", body)
	}

	if got := i.builtinExec([]Value{}); got.(map[string]Value)["success"] != false {
		t.Fatalf("expected exec no-arg failure, got %v", got)
	}
	if got := i.builtinExec([]Value{123}); got.(map[string]Value)["success"] != false {
		t.Fatalf("expected exec type failure, got %v", got)
	}
	if got := i.builtinExec([]Value{""}); got.(map[string]Value)["success"] != false {
		t.Fatalf("expected exec empty command failure, got %v", got)
	}
	execOK := i.builtinExec([]Value{"echo", []Value{"hi"}})
	execMap, ok := execOK.(map[string]Value)
	if !ok || execMap["success"] != true || !strings.Contains(execMap["output"].(string), "hi") {
		t.Fatalf("expected exec success with echo output, got %T (%v)", execOK, execOK)
	}

	if got := i.builtinExecOutput([]Value{}); got != "" {
		t.Fatalf("expected empty execOutput for no args, got %v", got)
	}
	if got := i.builtinExecOutput([]Value{123}); got != "" {
		t.Fatalf("expected empty execOutput for non-string cmd, got %v", got)
	}
	if got := i.builtinExecOutput([]Value{""}); got != "" {
		t.Fatalf("expected empty execOutput for empty cmd, got %v", got)
	}
	if got := i.builtinExecOutput([]Value{"echo", []Value{"out"}}); !strings.Contains(got.(string), "out") {
		t.Fatalf("expected execOutput echo output, got %v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch8_NumberWordAndDBHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinPadNumber([]Value{}); got != "" {
		t.Fatalf("expected empty padNumber for no args, got %v", got)
	}
	if got := i.builtinPadNumber([]Value{int64(7), int64(3), "x"}); got != "xx7" {
		t.Fatalf("expected custom padded number xx7, got %v", got)
	}

	if got := i.builtinToRoman([]Value{}); got != "" {
		t.Fatalf("expected empty toRoman for no args, got %v", got)
	}
	if got := i.builtinToRoman([]Value{int64(0)}); got != "" {
		t.Fatalf("expected empty toRoman out-of-range, got %v", got)
	}
	if got := i.builtinToRoman([]Value{int64(1994)}); got != "MCMXCIV" {
		t.Fatalf("expected roman MCMXCIV, got %v", got)
	}

	if got := i.builtinFromRoman([]Value{}); got != int64(0) {
		t.Fatalf("expected zero fromRoman for no args, got %v", got)
	}
	if got := i.builtinFromRoman([]Value{"mcmxciv"}); got != int64(1994) {
		t.Fatalf("expected fromRoman 1994, got %v", got)
	}

	if got := i.builtinToWords([]Value{}); got != "" {
		t.Fatalf("expected empty toWords for no args, got %v", got)
	}
	if got := i.builtinToWords([]Value{int64(-42)}); got != "negative forty-two" {
		t.Fatalf("expected negative words, got %v", got)
	}
	if got := i.builtinToOrdinal([]Value{int64(21)}); got != "twenty-onest" {
		t.Fatalf("expected ordinal twenty-onest, got %v", got)
	}

	db := NewDBObject(NewContext())
	if _, err := db.GetMember("query"); err != nil {
		t.Fatalf("expected query member, got error %v", err)
	}
	if _, err := db.GetMember("exec"); err != nil {
		t.Fatalf("expected exec member, got error %v", err)
	}
	if _, err := db.GetMember("queryRow"); err != nil {
		t.Fatalf("expected queryRow member, got error %v", err)
	}
	if _, err := db.GetMember("missing"); err == nil {
		t.Fatalf("expected unknown db method error")
	}

	q := &DBQueryFunc{ctx: NewContext()}
	if _, err := q.Call([]Value{"select 1"}); err == nil {
		t.Fatalf("expected query func error without executor")
	}

	e := NewContext()
	e.Executor = fakeScriptExecutor{result: map[string]interface{}{"affected": int64(2), "insert_id": int64(9)}}
	execFn := &DBExecFunc{ctx: e}
	execRes, err := execFn.Call([]Value{"update t set a=1"})
	if err != nil {
		t.Fatalf("expected exec function success, got error %v", err)
	}
	execResMap, ok := execRes.(map[string]Value)
	if !ok || execResMap["affected"] != int64(2) || execResMap["insert_id"] != int64(9) {
		t.Fatalf("unexpected exec function result: %T (%v)", execRes, execRes)
	}

	qctx := NewContext()
	qctx.Executor = fakeScriptExecutor{result: map[string]interface{}{"rows": [][]interface{}{{"alice", int64(1)}}, "columns": []string{"name", "id"}}}
	qf := &DBQueryFunc{ctx: qctx}
	qres, err := qf.Call([]Value{"select * from t"})
	if err != nil {
		t.Fatalf("expected query function success, got error %v", err)
	}
	qrows, ok := qres.([]Value)
	if !ok || len(qrows) != 1 {
		t.Fatalf("expected one query row, got %T (%v)", qres, qres)
	}

	rctx := NewContext()
	rctx.Executor = fakeScriptExecutor{result: map[string]interface{}{"rows": [][]interface{}{}, "columns": []string{"id"}}}
	rf := &DBQueryRowFunc{ctx: rctx}
	if row, err := rf.Call([]Value{"select id"}); err != nil || row != nil {
		t.Fatalf("expected nil row without error for empty result, got row=%v err=%v", row, err)
	}

	type testColumn struct {
		Alias string
		Name  string
	}
	type testResult struct {
		Rows    [][]interface{}
		Columns []testColumn
	}
	rows, cols, err := extractResult(testResult{Rows: [][]interface{}{{int64(5)}}, Columns: []testColumn{{Alias: "a", Name: "ignored"}}})
	if err != nil || len(rows) != 1 || len(cols) != 1 || cols[0] != "a" {
		t.Fatalf("unexpected extractResult reflection values rows=%v cols=%v err=%v", rows, cols, err)
	}
	if _, _, err := extractResult(int64(1)); err == nil {
		t.Fatalf("expected extractResult unsupported type error")
	}

	type execStruct struct {
		RowCount   int64
		LastInsert int64
	}
	affected, insertID := extractExecResult(execStruct{RowCount: 7, LastInsert: 11})
	if affected != 7 || insertID != 11 {
		t.Fatalf("unexpected extractExecResult reflection values affected=%d insertID=%d", affected, insertID)
	}
}
