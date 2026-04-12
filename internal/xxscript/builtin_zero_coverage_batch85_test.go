package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch85_CallBuiltinMatrix(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		name string
		args []Value
		want Value
	}{
		{"len", []Value{"abc"}, 3},
		{"sprintf", []Value{"%s-%d", "x", 2}, "x-2"},
		{"json", []Value{map[string]Value{"a": int64(1)}}, `{"a":1}`},
		{"jsonParse", []Value{`[1,2]`}, []Value{float64(1), float64(2)}},
		{"int", []Value{"12"}, 12},
		{"float", []Value{"1.5"}, float64(1.5)},
		{"string", []Value{123}, "123"},
		{"typeof", []Value{[]Value{}}, "array"},
		{"range", []Value{3}, []Value{0, 1, 2}},

		{"errorMessage", []Value{map[string]Value{"message": "m"}}, "m"},
		{"isError", []Value{map[string]Value{"error": true}}, true},
		{"errorWrap", []Value{"base", "ctx"}, map[string]Value{"error": true, "message": "ctx: base", "type": "error", "original": "base"}},
		{"panic", []Value{"p"}, map[string]Value{"error": true, "message": "p", "type": "panic"}},
		{"defaultOnError", []Value{map[string]Value{"error": true}, "d"}, "d"},
		{"tryGet", []Value{map[string]Value{"k": 7}, "k"}, 7},

		{"split", []Value{"a,b", ","}, []Value{"a", "b"}},
		{"join", []Value{[]Value{"a", "b"}, ","}, "a,b"},
		{"replace", []Value{"abc", "b", "X"}, "aXc"},
		{"trim", []Value{"  a  "}, "a"},
		{"trimPrefix", []Value{"foobar", "foo"}, "bar"},
		{"trimSuffix", []Value{"foobar", "bar"}, "foo"},
		{"upper", []Value{"ab"}, "AB"},
		{"lower", []Value{"AB"}, "ab"},
		{"hasPrefix", []Value{"abc", "a"}, true},
		{"hasSuffix", []Value{"abc", "c"}, true},
		{"contains", []Value{"abc", "b"}, true},
		{"indexOf", []Value{"abc", "b"}, 1},
		{"substr", []Value{"abc", 1, 1}, "b"},
		{"repeat", []Value{"x", 3}, "xxx"},
		{"reverse", []Value{"abc"}, "cba"},
		{"left", []Value{"abc", 2}, "ab"},
		{"right", []Value{"abc", 2}, "bc"},
		{"lines", []Value{"a\nb"}, []Value{"a", "b"}},
		{"words", []Value{"a b"}, []Value{"a", "b"}},
		{"startsWith", []Value{"abc", "a"}, true},
		{"endsWith", []Value{"abc", "c"}, true},

		{"camelCase", []Value{"hello_world"}, "helloWorld"},
		{"snakeCase", []Value{"helloWorld"}, "hello_world"},
		{"kebabCase", []Value{"helloWorld"}, "hello-world"},
		{"pascalCase", []Value{"hello_world"}, "HelloWorld"},
		{"sentenceCase", []Value{"hELLO"}, "Hello"},
		{"constantCase", []Value{"helloWorld"}, "HELLO_WORLD"},
		{"dotCase", []Value{"helloWorld"}, "hello.world"},
		{"pathCase", []Value{"helloWorld"}, "hello/world"},
		{"charAt", []Value{"abc", 1}, "b"},
		{"charCodeAt", []Value{"A", 0}, int64(65)},
		{"fromCharCode", []Value{65, 66}, "AB"},
		{"isLower", []Value{"abc"}, true},
		{"isUpper", []Value{"ABC"}, true},
		{"isSpace", []Value{" \t"}, true},
		{"isPrintable", []Value{"abc"}, true},
		{"isASCII", []Value{"abc"}, true},

		{"isHexColor", []Value{"#abc"}, true},
		{"isJSON", []Value{"{}"}, true},
		{"format", []Value{"%s-%d", "x", 2}, "x-2"},
		{"template", []Value{"Hi {{n}}", map[string]Value{"n": "bob"}}, "Hi bob"},
		{"repeatUntil", []Value{"ab", 5}, "ababa"},
		{"padBetween", []Value{"a", "b", "-"}, "a-b"},
		{"unwrap", []Value{"[x]", "[]"}, "x"},

		// aliases
		{"atob", []Value{"YQ=="}, "a"},
		{"btoa", []Value{"a"}, "YQ=="},
		{"regexEscape", []Value{"a+b"}, "a\\+b"},
	}

	for _, tc := range cases {
		got, handled := i.callBuiltin(tc.name, tc.args)
		if !handled {
			t.Fatalf("builtin %q not handled", tc.name)
		}
		if !valuesEqual(got, tc.want) {
			t.Fatalf("builtin %q: got=%#v want=%#v", tc.name, got, tc.want)
		}
	}

	if got, handled := i.callBuiltin("__definitely_unknown_builtin__", nil); handled || got != nil {
		t.Fatalf("unknown builtin dispatch: got=%#v handled=%v", got, handled)
	}
}

func TestBuiltin_ZeroCoverage_Batch85_IsTruthyDefaultBranch(t *testing.T) {
	i := NewInterpreter(NewContext())

	type custom struct{ N int }
	if !i.isTruthy(custom{N: 0}) {
		t.Fatalf("isTruthy default branch should return true")
	}
}
