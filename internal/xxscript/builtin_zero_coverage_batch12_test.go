package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch12_DataFormats(t *testing.T) {
	i := NewInterpreter(NewContext())

	if got := i.builtinCSVStringify([]Value{}); got != "" {
		t.Fatalf("expected empty csvStringify for no args, got %v", got)
	}
	if got := i.builtinCSVStringify([]Value{[]Value{"a", "b"}}); !strings.Contains(got.(string), "a,b") {
		t.Fatalf("expected single-row csv output, got %v", got)
	}
	rowsCSV := i.builtinCSVStringify([]Value{[]Value{[]Value{"c1", "c2"}, []Value{"v1", int64(2)}}})
	if s, ok := rowsCSV.(string); !ok || !strings.Contains(s, "c1,c2") || !strings.Contains(s, "v1,2") {
		t.Fatalf("expected multi-row csv output, got %T (%v)", rowsCSV, rowsCSV)
	}
	objCSV := i.builtinCSVStringify([]Value{map[string]Value{"name": "alice", "age": int64(30)}})
	if s, ok := objCSV.(string); !ok || !strings.Contains(s, "alice") || !strings.Contains(s, "30") {
		t.Fatalf("expected object csv output, got %T (%v)", objCSV, objCSV)
	}

	if m := i.builtinXMLParse([]Value{}).(map[string]Value); len(m) != 0 {
		t.Fatalf("expected empty xmlParse map for no args, got %v", m)
	}
	if m := i.builtinXMLParse([]Value{int64(1)}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected xmlParse type error map, got %v", m)
	}
	xm := i.builtinXMLParse([]Value{"<name>alice</name>"}).(map[string]Value)
	if xm["name"] != "alice" {
		t.Fatalf("expected xmlParse to extract name alice, got %v", xm)
	}

	if got := i.builtinXMLStringify([]Value{}); got != "" {
		t.Fatalf("expected empty xmlStringify for no args, got %v", got)
	}
	if got := i.builtinXMLStringify([]Value{"bad"}); got != "" {
		t.Fatalf("expected empty xmlStringify for invalid type, got %v", got)
	}
	xs := i.builtinXMLStringify([]Value{map[string]Value{"name": "<alice&bob>"}, "person"})
	xsStr, ok := xs.(string)
	if !ok || !strings.Contains(xsStr, "<person>") || !strings.Contains(xsStr, "&lt;alice&amp;bob&gt;") {
		t.Fatalf("expected xmlStringify escaped output, got %T (%v)", xs, xs)
	}

	if got := i.builtinXMLGet([]Value{"<a>x</a>"}); got != nil {
		t.Fatalf("expected nil xmlGet for missing path arg, got %v", got)
	}
	if got := i.builtinXMLGet([]Value{"<a>x</a>", "a"}); got != "x" {
		t.Fatalf("expected xmlGet extracted value x, got %v", got)
	}

	if m := i.builtinYAMLParse([]Value{}).(map[string]Value); len(m) != 0 {
		t.Fatalf("expected empty yamlParse map for no args, got %v", m)
	}
	if m := i.builtinYAMLParse([]Value{123}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected yamlParse type error map, got %v", m)
	}
	ym := i.builtinYAMLParse([]Value{"a: 1\nb: two\n"}).(map[string]Value)
	if ym["b"] != "two" {
		t.Fatalf("expected yamlParse key b=two, got %v", ym)
	}
	if ys := i.builtinYAMLStringify([]Value{map[string]Value{"name": "alice", "age": int64(3)}}); !strings.Contains(ys.(string), "name: alice") {
		t.Fatalf("expected yamlStringify output to contain field, got %v", ys)
	}

	if m := i.builtinTOMLParse([]Value{}).(map[string]Value); len(m) != 0 {
		t.Fatalf("expected empty tomlParse map for no args, got %v", m)
	}
	if m := i.builtinTOMLParse([]Value{true}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected tomlParse type error map, got %v", m)
	}
	tm := i.builtinTOMLParse([]Value{"name = \"alice\"\nage = 2"}).(map[string]Value)
	if tm["name"] != "alice" {
		t.Fatalf("expected tomlParse key name=alice, got %v", tm)
	}
	if got := i.builtinTOMLStringify([]Value{"bad"}); got != "" {
		t.Fatalf("expected empty tomlStringify for invalid type, got %v", got)
	}
	ts := i.builtinTOMLStringify([]Value{map[string]Value{"name": "alice", "age": int64(2)}})
	if s, ok := ts.(string); !ok || !strings.Contains(s, "name = \"alice\"") {
		t.Fatalf("expected tomlStringify output to contain name field, got %T (%v)", ts, ts)
	}

	if got := i.builtinMarkdownToHTML([]Value{}); got != "" {
		t.Fatalf("expected empty markdownToHTML for no args, got %v", got)
	}
	if got := i.builtinMarkdownToHTML([]Value{int64(1)}); got != "" {
		t.Fatalf("expected empty markdownToHTML for invalid type, got %v", got)
	}
	html := i.builtinMarkdownToHTML([]Value{"# Title\n\n**bold** [x](https://e.com)"})
	htmlStr, ok := html.(string)
	if !ok || !strings.Contains(htmlStr, "<h1>Title</h1>") || !strings.Contains(htmlStr, "<strong>bold</strong>") || !strings.Contains(htmlStr, "<a href=\"https://e.com\">x</a>") {
		t.Fatalf("expected markdownToHTML conversion markers, got %T (%v)", html, html)
	}
}
