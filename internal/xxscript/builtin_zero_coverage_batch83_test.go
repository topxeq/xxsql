package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch83_StringCaseExtras(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinReverse(nil); got != "" {
		t.Fatalf("builtinReverse nil args: got=%#v", got)
	}
	if got := i.builtinReverse([]Value{1}); got != "" {
		t.Fatalf("builtinReverse non-string: got=%#v", got)
	}
	if got := i.builtinReverse([]Value{"ab好"}); got != "好ba" {
		t.Fatalf("builtinReverse unicode: got=%#v", got)
	}

	if got := i.builtinCamelCase(nil); got != "" {
		t.Fatalf("builtinCamelCase nil args: got=%#v", got)
	}
	if got := i.builtinCamelCase([]Value{1}); got != "" {
		t.Fatalf("builtinCamelCase non-string: got=%#v", got)
	}
	if got := i.builtinCamelCase([]Value{"___"}); got != "" {
		t.Fatalf("builtinCamelCase empty words: got=%#v", got)
	}
	if got := i.builtinCamelCase([]Value{"hello_world-test.case"}); got != "helloWorldTestCase" {
		t.Fatalf("builtinCamelCase convert: got=%#v", got)
	}

	if got := i.builtinSnakeCase(nil); got != "" {
		t.Fatalf("builtinSnakeCase nil args: got=%#v", got)
	}
	if got := i.builtinSnakeCase([]Value{1}); got != "" {
		t.Fatalf("builtinSnakeCase non-string: got=%#v", got)
	}
	if got := i.builtinSnakeCase([]Value{"helloWorld test-case.end"}); got != "hello_world_test_case_end" {
		t.Fatalf("builtinSnakeCase convert: got=%#v", got)
	}

	if got := i.builtinKebabCase(nil); got != "" {
		t.Fatalf("builtinKebabCase nil args: got=%#v", got)
	}
	if got := i.builtinKebabCase([]Value{1}); got != "" {
		t.Fatalf("builtinKebabCase non-string: got=%#v", got)
	}
	if got := i.builtinKebabCase([]Value{"helloWorld_test.case"}); got != "hello-world-test-case" {
		t.Fatalf("builtinKebabCase convert: got=%#v", got)
	}

	if got := i.builtinPascalCase(nil); got != "" {
		t.Fatalf("builtinPascalCase nil args: got=%#v", got)
	}
	if got := i.builtinPascalCase([]Value{1}); got != "" {
		t.Fatalf("builtinPascalCase non-string: got=%#v", got)
	}
	if got := i.builtinPascalCase([]Value{"hello_world-test.case"}); got != "HelloWorldTestCase" {
		t.Fatalf("builtinPascalCase convert: got=%#v", got)
	}

	if got := i.builtinSentenceCase(nil); got != "" {
		t.Fatalf("builtinSentenceCase nil args: got=%#v", got)
	}
	if got := i.builtinSentenceCase([]Value{1}); got != "" {
		t.Fatalf("builtinSentenceCase non-string: got=%#v", got)
	}
	if got := i.builtinSentenceCase([]Value{""}); got != "" {
		t.Fatalf("builtinSentenceCase empty: got=%#v", got)
	}
	if got := i.builtinSentenceCase([]Value{"hELLo WORLD"}); got != "Hello world" {
		t.Fatalf("builtinSentenceCase convert: got=%#v", got)
	}
}

func TestBuiltin_ZeroCoverage_Batch83_ValidationExtras(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinIsHexColor(nil); got != false {
		t.Fatalf("builtinIsHexColor nil args: got=%#v", got)
	}
	if got := i.builtinIsHexColor([]Value{1}); got != false {
		t.Fatalf("builtinIsHexColor non-string: got=%#v", got)
	}
	if got := i.builtinIsHexColor([]Value{"#abc"}); got != true {
		t.Fatalf("builtinIsHexColor short valid: got=%#v", got)
	}
	if got := i.builtinIsHexColor([]Value{"#A1B2C3D4"}); got != true {
		t.Fatalf("builtinIsHexColor long valid: got=%#v", got)
	}
	if got := i.builtinIsHexColor([]Value{"abc"}); got != false {
		t.Fatalf("builtinIsHexColor invalid: got=%#v", got)
	}

	if got := i.builtinIsJSONStr(nil); got != false {
		t.Fatalf("builtinIsJSONStr nil args: got=%#v", got)
	}
	if got := i.builtinIsJSONStr([]Value{1}); got != false {
		t.Fatalf("builtinIsJSONStr non-string: got=%#v", got)
	}
	if got := i.builtinIsJSONStr([]Value{"{"}); got != false {
		t.Fatalf("builtinIsJSONStr invalid JSON: got=%#v", got)
	}
	if got := i.builtinIsJSONStr([]Value{"[1,2,3]"}); got != true {
		t.Fatalf("builtinIsJSONStr valid JSON: got=%#v", got)
	}
}
