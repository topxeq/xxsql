package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch15_HTMLRSSML(t *testing.T) {
	i := NewInterpreter(NewContext())

	html := `<div><a href="https://a.com">A</a><p>Hello &amp; world</p></div>`

	if m := i.builtinHTMLParse([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected htmlParse arg error, got %v", m)
	}
	hp := i.builtinHTMLParse([]Value{html}).(map[string]Value)
	if hp["valid"] != true || hp["text"] != "A Hello & world" {
		t.Fatalf("expected htmlParse valid extracted text, got %v", hp)
	}

	if m := i.builtinHTMLSelect([]Value{html}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected htmlSelect arg error, got %v", m)
	}
	hs := i.builtinHTMLSelect([]Value{html, "a"}).(map[string]Value)
	if hs["found"] != true || hs["count"] != 1 {
		t.Fatalf("expected htmlSelect to find one anchor, got %v", hs)
	}
	hsa := i.builtinHTMLSelectAll([]Value{html, "p"}).(map[string]Value)
	if hsa["count"] != 1 {
		t.Fatalf("expected htmlSelectAll to find one paragraph, got %v", hsa)
	}

	if m := i.builtinHTMLAttr([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected htmlAttr arg error, got %v", m)
	}
	ha := i.builtinHTMLAttr([]Value{`<a href="https://a.com" id="x">A</a>`, "href"}).(map[string]Value)
	if ha["found"] != true || ha["value"] != "https://a.com" {
		t.Fatalf("expected htmlAttr href extraction, got %v", ha)
	}

	if m := i.builtinHTMLText([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected htmlText arg error, got %v", m)
	}
	ht := i.builtinHTMLText([]Value{html}).(map[string]Value)
	if ht["text"] != "A Hello & world" {
		t.Fatalf("expected htmlText extraction, got %v", ht)
	}

	if m := i.builtinHTMLLinks([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected htmlLinks arg error, got %v", m)
	}
	hl := i.builtinHTMLLinks([]Value{html}).(map[string]Value)
	if hl["count"] != 1 {
		t.Fatalf("expected one html link, got %v", hl)
	}

	rss := `<rss><channel><item><title><![CDATA[T1]]></title><link>https://e.com/1</link><description>D1</description></item></channel></rss>`
	if m := i.builtinRSSParse([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected rssParse arg error, got %v", m)
	}
	rp := i.builtinRSSParse([]Value{rss}).(map[string]Value)
	if rp["valid"] != true || rp["count"] != 1 {
		t.Fatalf("expected rssParse one item, got %v", rp)
	}

	if m := i.builtinMLTokenize([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected mlTokenize arg error, got %v", m)
	}
	tok := i.builtinMLTokenize([]Value{"Hello, world!"}).(map[string]Value)
	if tok["valid"] != true || tok["count"] != 2 {
		t.Fatalf("expected mlTokenize count 2, got %v", tok)
	}

	if m := i.builtinMLSentiment([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected mlSentiment arg error, got %v", m)
	}
	pos := i.builtinMLSentiment([]Value{"great and wonderful"}).(map[string]Value)
	if pos["sentiment"] != "positive" {
		t.Fatalf("expected positive sentiment, got %v", pos)
	}
	neg := i.builtinMLSentiment([]Value{"bad and horrible"}).(map[string]Value)
	if neg["sentiment"] != "negative" {
		t.Fatalf("expected negative sentiment, got %v", neg)
	}

	if m := i.builtinMLSimilarity([]Value{"one"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected mlSimilarity arg error, got %v", m)
	}
	sim := i.builtinMLSimilarity([]Value{"a b", "a c"}).(map[string]Value)
	if sim["similarity"] != 0.33 {
		t.Fatalf("expected rounded jaccard similarity 0.33, got %v", sim)
	}

	if m := i.builtinMLKeywords([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected mlKeywords arg error, got %v", m)
	}
	kws := i.builtinMLKeywords([]Value{"apple apple banana banana banana", 2}).(map[string]Value)
	if kws["count"] != 2 {
		t.Fatalf("expected two keywords, got %v", kws)
	}

	if m := i.builtinMLNgrams([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected mlNgrams arg error, got %v", m)
	}
	ng := i.builtinMLNgrams([]Value{"a b c", 2}).(map[string]Value)
	if ng["count"] != 2 {
		t.Fatalf("expected two bigrams, got %v", ng)
	}

	if m := i.builtinMLWordFreq([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected mlWordFreq arg error, got %v", m)
	}
	wf := i.builtinMLWordFreq([]Value{"go go test"}).(map[string]Value)
	if wf["uniqueWords"] != 2 {
		t.Fatalf("expected two unique words, got %v", wf)
	}
}
