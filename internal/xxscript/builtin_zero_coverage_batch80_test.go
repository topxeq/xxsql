package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch80_CaseAndCharPredicates(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinConstantCase(nil); got != "" {
		t.Fatalf("constantCase nil args: got=%#v", got)
	}
	if got := i.builtinConstantCase([]Value{1}); got != "" {
		t.Fatalf("constantCase non-string: got=%#v", got)
	}
	if got := i.builtinConstantCase([]Value{"helloWorld test-case.end"}); got != "HELLO_WORLD_TEST_CASE_END" {
		t.Fatalf("constantCase convert: got=%#v", got)
	}

	if got := i.builtinDotCase(nil); got != "" {
		t.Fatalf("dotCase nil args: got=%#v", got)
	}
	if got := i.builtinDotCase([]Value{false}); got != "" {
		t.Fatalf("dotCase non-string: got=%#v", got)
	}
	if got := i.builtinDotCase([]Value{"fooBar-baz qux_quux"}); got != "foo.bar.baz.qux.quux" {
		t.Fatalf("dotCase convert: got=%#v", got)
	}

	if got := i.builtinPathCase(nil); got != "" {
		t.Fatalf("pathCase nil args: got=%#v", got)
	}
	if got := i.builtinPathCase([]Value{map[string]Value{}}); got != "" {
		t.Fatalf("pathCase non-string: got=%#v", got)
	}
	if got := i.builtinPathCase([]Value{"fooBar.baz qux_quux-quuz"}); got != "foo/bar/baz/qux/quux/quuz" {
		t.Fatalf("pathCase convert: got=%#v", got)
	}

	if got := i.builtinIsLowerStr(nil); got != false {
		t.Fatalf("isLowerStr nil args: got=%#v", got)
	}
	if got := i.builtinIsLowerStr([]Value{1}); got != false {
		t.Fatalf("isLowerStr non-string: got=%#v", got)
	}
	if got := i.builtinIsLowerStr([]Value{""}); got != false {
		t.Fatalf("isLowerStr empty string: got=%#v", got)
	}
	if got := i.builtinIsLowerStr([]Value{"abc123"}); got != true {
		t.Fatalf("isLowerStr lowercase: got=%#v", got)
	}
	if got := i.builtinIsLowerStr([]Value{"abC"}); got != false {
		t.Fatalf("isLowerStr mixed case: got=%#v", got)
	}

	if got := i.builtinIsUpperStr(nil); got != false {
		t.Fatalf("isUpperStr nil args: got=%#v", got)
	}
	if got := i.builtinIsUpperStr([]Value{1}); got != false {
		t.Fatalf("isUpperStr non-string: got=%#v", got)
	}
	if got := i.builtinIsUpperStr([]Value{""}); got != false {
		t.Fatalf("isUpperStr empty string: got=%#v", got)
	}
	if got := i.builtinIsUpperStr([]Value{"ABC123"}); got != true {
		t.Fatalf("isUpperStr uppercase: got=%#v", got)
	}
	if got := i.builtinIsUpperStr([]Value{"ABc"}); got != false {
		t.Fatalf("isUpperStr mixed case: got=%#v", got)
	}

	if got := i.builtinIsSpaceStr(nil); got != false {
		t.Fatalf("isSpaceStr nil args: got=%#v", got)
	}
	if got := i.builtinIsSpaceStr([]Value{1}); got != false {
		t.Fatalf("isSpaceStr non-string: got=%#v", got)
	}
	if got := i.builtinIsSpaceStr([]Value{""}); got != false {
		t.Fatalf("isSpaceStr empty string: got=%#v", got)
	}
	if got := i.builtinIsSpaceStr([]Value{" \t\n"}); got != true {
		t.Fatalf("isSpaceStr whitespace: got=%#v", got)
	}
	if got := i.builtinIsSpaceStr([]Value{" a "}); got != false {
		t.Fatalf("isSpaceStr mixed content: got=%#v", got)
	}

	if got := i.builtinIsPrintable(nil); got != false {
		t.Fatalf("isPrintable nil args: got=%#v", got)
	}
	if got := i.builtinIsPrintable([]Value{1}); got != false {
		t.Fatalf("isPrintable non-string: got=%#v", got)
	}
	if got := i.builtinIsPrintable([]Value{""}); got != false {
		t.Fatalf("isPrintable empty string: got=%#v", got)
	}
	if got := i.builtinIsPrintable([]Value{"abc !"}); got != true {
		t.Fatalf("isPrintable printable string: got=%#v", got)
	}
	if got := i.builtinIsPrintable([]Value{"abc\t"}); got != false {
		t.Fatalf("isPrintable tab char: got=%#v", got)
	}

	if got := i.builtinIsASCII(nil); got != false {
		t.Fatalf("isASCII nil args: got=%#v", got)
	}
	if got := i.builtinIsASCII([]Value{1}); got != false {
		t.Fatalf("isASCII non-string: got=%#v", got)
	}
	if got := i.builtinIsASCII([]Value{"abc"}); got != true {
		t.Fatalf("isASCII ascii string: got=%#v", got)
	}
	if got := i.builtinIsASCII([]Value{"caf\u00e9"}); got != false {
		t.Fatalf("isASCII non-ascii string: got=%#v", got)
	}
}
