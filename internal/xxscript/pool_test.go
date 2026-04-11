package xxscript

import (
	"testing"
)

func TestValuePool_GetSlice(t *testing.T) {
	s := ValuePool.GetSlice()
	if s == nil {
		t.Fatal("GetSlice returned nil")
	}
	if cap(*s) < 8 {
		t.Errorf("Expected capacity >= 8, got %d", cap(*s))
	}
	ValuePool.PutSlice(s)
}

func TestValuePool_PutSlice(t *testing.T) {
	s := ValuePool.GetSlice()
	*s = append(*s, int64(1), int64(2))
	ValuePool.PutSlice(s)

	s2 := ValuePool.GetSlice()
	if len(*s2) != 0 {
		t.Errorf("Expected empty slice after PutSlice, got len=%d", len(*s2))
	}
	ValuePool.PutSlice(s2)
}

func TestValuePool_GetMap(t *testing.T) {
	m := ValuePool.GetMap()
	if m == nil {
		t.Fatal("GetMap returned nil")
	}
	ValuePool.PutMap(m)
}

func TestValuePool_PutMap(t *testing.T) {
	m := ValuePool.GetMap()
	(*m)["key"] = int64(1)
	ValuePool.PutMap(m)

	m2 := ValuePool.GetMap()
	if len(*m2) != 0 {
		t.Errorf("Expected empty map after PutMap, got len=%d", len(*m2))
	}
	ValuePool.PutMap(m2)
}

func TestValuePool_GetArgs(t *testing.T) {
	a := ValuePool.GetArgs()
	if a == nil {
		t.Fatal("GetArgs returned nil")
	}
	if cap(*a) < 4 {
		t.Errorf("Expected capacity >= 4, got %d", cap(*a))
	}
	ValuePool.PutArgs(a)
}

func TestValuePool_PutArgs(t *testing.T) {
	a := ValuePool.GetArgs()
	*a = append(*a, int64(1), int64(2))
	ValuePool.PutArgs(a)

	a2 := ValuePool.GetArgs()
	if len(*a2) != 0 {
		t.Errorf("Expected empty args after PutArgs, got len=%d", len(*a2))
	}
	ValuePool.PutArgs(a2)
}

func TestTokenPool(t *testing.T) {
	tokens := GetTokenSlice()
	if tokens == nil {
		t.Fatal("GetTokenSlice returned nil")
	}
	if cap(*tokens) < 64 {
		t.Errorf("Expected capacity >= 64, got %d", cap(*tokens))
	}

	*tokens = append(*tokens, Token{Type: TokIdent, Value: "foo"})
	PutTokenSlice(tokens)

	tokens2 := GetTokenSlice()
	if len(*tokens2) != 0 {
		t.Errorf("Expected empty tokens after PutTokenSlice, got len=%d", len(*tokens2))
	}
	PutTokenSlice(tokens2)
}
