package xxscript

import (
	"testing"
	"time"
)

func TestNewContext(t *testing.T) {
	ctx := NewContext()
	if ctx == nil {
		t.Fatal("NewContext returned nil")
	}
	if ctx.Variables == nil {
		t.Error("Variables not initialized")
	}
	if ctx.Functions == nil {
		t.Error("Functions not initialized")
	}
	if ctx.MaxSteps != 10000000 {
		t.Errorf("Expected MaxSteps 10000000, got %d", ctx.MaxSteps)
	}
}

func TestContext_Variables(t *testing.T) {
	ctx := NewContext()

	ctx.SetVariable("x", int64(42))
	val, ok := ctx.GetVariable("x")
	if !ok {
		t.Fatal("Expected to find x")
	}
	if val != int64(42) {
		t.Errorf("Expected 42, got %v", val)
	}

	ctx.DeleteVariable("x")
	_, ok = ctx.GetVariable("x")
	if ok {
		t.Error("Expected x to be deleted")
	}
}

func TestContext_Functions(t *testing.T) {
	ctx := NewContext()

	body := &BlockStmt{}
	ctx.DefineFunction("test", []string{"a", "b"}, body)

	fn, ok := ctx.GetFunction("test")
	if !ok {
		t.Fatal("Expected to find test function")
	}
	if len(fn.Params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(fn.Params))
	}
	if fn.Body != body {
		t.Error("Body mismatch")
	}
}

func TestContext_IncrementSteps(t *testing.T) {
	ctx := NewContext()
	ctx.MaxSteps = 5

	for i := 0; i < 5; i++ {
		err := ctx.IncrementSteps()
		if err != nil {
			t.Fatalf("Unexpected error at step %d: %v", i, err)
		}
	}

	err := ctx.IncrementSteps()
	if err != ErrMaxStepsExceeded {
		t.Errorf("Expected ErrMaxStepsExceeded, got %v", err)
	}
}

func TestContext_ReturnFlags(t *testing.T) {
	ctx := NewContext()

	if ctx.IsReturning() {
		t.Error("Expected not returning initially")
	}

	ctx.SetReturning(int64(42))
	if !ctx.IsReturning() {
		t.Error("Expected returning after SetReturning")
	}
	if ctx.GetReturnValue() != int64(42) {
		t.Errorf("Expected return value 42, got %v", ctx.GetReturnValue())
	}
}

func TestContext_BreakFlags(t *testing.T) {
	ctx := NewContext()

	if ctx.IsBreaking() {
		t.Error("Expected not breaking initially")
	}

	ctx.SetBreaking()
	if !ctx.IsBreaking() {
		t.Error("Expected breaking after SetBreaking")
	}

	ctx.ClearBreaking()
	if ctx.IsBreaking() {
		t.Error("Expected not breaking after ClearBreaking")
	}
}

func TestContext_ContinueFlags(t *testing.T) {
	ctx := NewContext()

	if ctx.IsContinuing() {
		t.Error("Expected not continuing initially")
	}

	ctx.SetContinuing()
	if !ctx.IsContinuing() {
		t.Error("Expected continuing after SetContinuing")
	}

	ctx.ClearContinuing()
	if ctx.IsContinuing() {
		t.Error("Expected not continuing after ClearContinuing")
	}
}

func TestContext_ResetFlowControl(t *testing.T) {
	ctx := NewContext()

	ctx.SetReturning(int64(42))
	ctx.SetBreaking()
	ctx.SetContinuing()

	ctx.ResetFlowControl()

	if ctx.IsReturning() {
		t.Error("Expected not returning after reset")
	}
	if ctx.IsBreaking() {
		t.Error("Expected not breaking after reset")
	}
	if ctx.IsContinuing() {
		t.Error("Expected not continuing after reset")
	}
	if ctx.GetReturnValue() != nil {
		t.Error("Expected nil return value after reset")
	}
}

func TestContext_CacheSetAndGet(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("key1", "value1", 0)
	val, ok := ctx.CacheGet("key1")
	if !ok {
		t.Fatal("Expected to find key1")
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %v", val)
	}
}

func TestContext_CacheWithTTL(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("temp", "data", 50*time.Millisecond)

	val, ok := ctx.CacheGet("temp")
	if !ok || val != "data" {
		t.Error("Expected to find temp before expiry")
	}

	time.Sleep(100 * time.Millisecond)

	_, ok = ctx.CacheGet("temp")
	if ok {
		t.Error("Expected temp to be expired")
	}
}

func TestContext_CacheDelete(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("key", "value", 0)
	ctx.CacheDelete("key")

	_, ok := ctx.CacheGet("key")
	if ok {
		t.Error("Expected key to be deleted")
	}
}

func TestContext_CacheHas(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("key", "value", 0)

	if !ctx.CacheHas("key") {
		t.Error("Expected CacheHas to return true")
	}

	if ctx.CacheHas("nonexistent") {
		t.Error("Expected CacheHas to return false for nonexistent key")
	}
}

func TestContext_CacheHasExpired(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("temp", "data", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	if ctx.CacheHas("temp") {
		t.Error("Expected CacheHas to return false for expired key")
	}
}

func TestContext_CacheClear(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("key1", "value1", 0)
	ctx.CacheSet("key2", "value2", 0)
	ctx.CacheClear()

	keys := ctx.CacheKeys()
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys after clear, got %d", len(keys))
	}
}

func TestContext_CacheKeys(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("a", 1, 0)
	ctx.CacheSet("b", 2, 0)
	ctx.CacheSet("c", 3, 0)

	keys := ctx.CacheKeys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}
}

func TestContext_CacheKeysExcludesExpired(t *testing.T) {
	ctx := NewContext()

	ctx.CacheSet("permanent", 1, 0)
	ctx.CacheSet("temporary", 2, 50*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	keys := ctx.CacheKeys()
	if len(keys) != 1 {
		t.Errorf("Expected 1 key (permanent), got %d", len(keys))
	}
	if keys[0] != "permanent" {
		t.Errorf("Expected 'permanent', got %s", keys[0])
	}
}

func TestScriptError(t *testing.T) {
	err := NewScriptError("test error")
	if err.Error() != "test error" {
		t.Errorf("Expected 'test error', got %s", err.Error())
	}
}

func TestThrowError_String(t *testing.T) {
	err := &ThrowError{Value: "thrown error"}
	if err.Error() != "thrown error" {
		t.Errorf("Expected 'thrown error', got %s", err.Error())
	}
	if err.String() != "thrown error" {
		t.Errorf("Expected 'thrown error', got %s", err.String())
	}
}

func TestUserFunc(t *testing.T) {
	body := &BlockStmt{}
	fn := &UserFunc{
		Params:         []string{"a", "b"},
		DefaultValues:  nil,
		RestParamIndex: -1,
		Body:           body,
	}

	if len(fn.Params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(fn.Params))
	}
	if fn.RestParamIndex != -1 {
		t.Errorf("Expected RestParamIndex -1, got %d", fn.RestParamIndex)
	}
}

func TestUserFuncWithRestParam(t *testing.T) {
	fn := &UserFunc{
		Params:         []string{"a", "...rest"},
		RestParamIndex: 1,
		Body:           &BlockStmt{},
	}

	if fn.RestParamIndex != 1 {
		t.Errorf("Expected RestParamIndex 1, got %d", fn.RestParamIndex)
	}
}
