package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch52_ValidatePassword_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinValidatePassword([]Value{}).(map[string]Value); m["valid"] != false || m["score"] != 0 {
		t.Fatalf("expected validatePassword no-arg fallback, got %v", m)
	}
	if m := i.builtinValidatePassword([]Value{123}).(map[string]Value); m["valid"] != false || m["score"] != 0 {
		t.Fatalf("expected validatePassword non-string fallback, got %v", m)
	}

	veryWeak := i.builtinValidatePassword([]Value{"abc"}).(map[string]Value)
	if veryWeak["strength"] != "very weak" || veryWeak["valid"] != false {
		t.Fatalf("expected very weak password result, got %v", veryWeak)
	}

	weak := i.builtinValidatePassword([]Value{"ABCDEFGH"}).(map[string]Value)
	if weak["strength"] != "weak" || weak["score"] != 2 {
		t.Fatalf("expected weak password result, got %v", weak)
	}

	medium := i.builtinValidatePassword([]Value{"abcdef12"}).(map[string]Value)
	if medium["strength"] != "medium" || medium["score"] != 3 || medium["valid"] != true {
		t.Fatalf("expected medium+valid password result, got %v", medium)
	}

	strong := i.builtinValidatePassword([]Value{"abcDEF12"}).(map[string]Value)
	if strong["strength"] != "strong" || strong["score"] != 4 || strong["valid"] != true {
		t.Fatalf("expected strong password result, got %v", strong)
	}

	veryStrong := i.builtinValidatePassword([]Value{"Abcdef12!"}).(map[string]Value)
	if veryStrong["strength"] != "very strong" || veryStrong["score"] != 5 || veryStrong["valid"] != true {
		t.Fatalf("expected very strong password result, got %v", veryStrong)
	}

	customMin := i.builtinValidatePassword([]Value{"Ab1!", int64(4)}).(map[string]Value)
	if customMin["valid"] != true || customMin["score"] != 5 {
		t.Fatalf("expected custom min-length branch success, got %v", customMin)
	}
}

func TestBuiltin_ZeroCoverage_Batch52_RenderTemplate_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinRenderTemplate([]Value{"only-template"}); got != "" {
		t.Fatalf("expected empty string for insufficient args, got %v", got)
	}
	if got := i.builtinRenderTemplate([]Value{123, map[string]Value{"name": "A"}}); got != "" {
		t.Fatalf("expected empty string for non-string template, got %v", got)
	}
	if got := i.builtinRenderTemplate([]Value{"Hello {{name}}", "not-map"}); got != "Hello {{name}}" {
		t.Fatalf("expected template passthrough for non-map data, got %v", got)
	}

	rendered := i.builtinRenderTemplate([]Value{"Hello {{name}} {{missing}}", map[string]Value{"name": "Alice"}})
	if rendered != "Hello Alice {{missing}}" {
		t.Fatalf("unexpected rendered output: %v", rendered)
	}
}
