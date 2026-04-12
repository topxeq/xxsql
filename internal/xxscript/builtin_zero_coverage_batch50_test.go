package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch50_HTMLEntityDecode_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinHTMLEntityDecode([]Value{}); got != "" {
		t.Fatalf("expected empty string for no args, got %v", got)
	}
	if got := i.builtinHTMLEntityDecode([]Value{123}); got != "" {
		t.Fatalf("expected empty string for non-string arg, got %v", got)
	}

	decoded := i.builtinHTMLEntityDecode([]Value{"A: &#65;, B: &#x42;, keep: &amp;"}).(string)
	if decoded != "A: A, B: B, keep: &amp;" {
		t.Fatalf("unexpected decoded value: %q", decoded)
	}

	unchanged := i.builtinHTMLEntityDecode([]Value{"no entities"}).(string)
	if unchanged != "no entities" {
		t.Fatalf("expected unchanged plain text, got %q", unchanged)
	}
}

func TestBuiltin_ZeroCoverage_Batch50_Minify_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinMinify([]Value{}); got != "" {
		t.Fatalf("expected empty string for no args, got %v", got)
	}
	if got := i.builtinMinify([]Value{123}); got != "" {
		t.Fatalf("expected empty string for non-string code, got %v", got)
	}

	jsonOut := i.builtinMinify([]Value{"{\n  \"a\": 1\n}", "json"}).(string)
	if jsonOut != "{\"a\":1}" {
		t.Fatalf("expected compact JSON, got %q", jsonOut)
	}

	jsonInvalid := i.builtinMinify([]Value{"{ bad json", "json"}).(string)
	if jsonInvalid != "{ bad json" {
		t.Fatalf("expected invalid JSON to fall back to original code, got %q", jsonInvalid)
	}

	htmlOut := i.builtinMinify([]Value{"<!--x--><div>  a  </div>   <span>b</span>", "html"}).(string)
	if htmlOut != "<div> a </div><span>b</span>" {
		t.Fatalf("unexpected minified HTML: %q", htmlOut)
	}

	defaultOut := i.builtinMinify([]Value{" a\n\t b  c "}).(string)
	if defaultOut != "a b c" {
		t.Fatalf("unexpected generic minify output: %q", defaultOut)
	}
}

func TestBuiltin_ZeroCoverage_Batch50_Beautify_Branches(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinBeautify([]Value{}); got != "" {
		t.Fatalf("expected empty string for no args, got %v", got)
	}
	if got := i.builtinBeautify([]Value{123}); got != "" {
		t.Fatalf("expected empty string for non-string code, got %v", got)
	}

	jsonOut := i.builtinBeautify([]Value{"{\"a\":1}", "json"}).(string)
	if !strings.Contains(jsonOut, "\n") || !strings.Contains(jsonOut, "  \"a\": 1") {
		t.Fatalf("expected pretty JSON with indentation, got %q", jsonOut)
	}

	jsonInvalid := i.builtinBeautify([]Value{"{ bad json", "json"}).(string)
	if jsonInvalid != "{ bad json" {
		t.Fatalf("expected invalid JSON beautify to fall back to original code, got %q", jsonInvalid)
	}

	xmlOut := i.builtinBeautify([]Value{"<a>1</a>", "xml"}).(string)
	if !strings.Contains(xmlOut, "\n") || !strings.Contains(xmlOut, "<a>") || !strings.Contains(xmlOut, "</a>") {
		t.Fatalf("expected XML beautify output with line breaks, got %q", xmlOut)
	}
}
