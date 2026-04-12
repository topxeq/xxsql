package xxscript

import (
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch25_NLPBlockchainAndHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinNLPStem([]Value{}).(map[string]Value); m["error"] != "need word" {
		t.Fatalf("expected stem arg error, got %v", m)
	}
	if m := i.builtinNLPStem([]Value{123}).(map[string]Value); m["error"] != "word must be string" {
		t.Fatalf("expected stem type error, got %v", m)
	}
	stemmed := i.builtinNLPStem([]Value{"Running"}).(map[string]Value)
	if stemmed["original"] != "Running" || stemmed["stem"] != "runn" {
		t.Fatalf("expected NLP stem success payload, got %v", stemmed)
	}

	if got := stemWord("happiness"); got != "happi" {
		t.Fatalf("expected happiness -> happi, got %q", got)
	}
	if got := stemWord("go"); got != "go" {
		t.Fatalf("expected short word unchanged, got %q", got)
	}

	if m := i.builtinNLPPosTag([]Value{}).(map[string]Value); m["error"] != "need text" {
		t.Fatalf("expected pos arg error, got %v", m)
	}
	if m := i.builtinNLPPosTag([]Value{123}).(map[string]Value); m["error"] != "text must be string" {
		t.Fatalf("expected pos type error, got %v", m)
	}
	pos := i.builtinNLPPosTag([]Value{"the cat quickly jumped in 42"}).(map[string]Value)
	if pos["count"] != 6 {
		t.Fatalf("expected 6 tags, got %v", pos)
	}
	tags, ok := pos["tags"].([]Value)
	if !ok || len(tags) != 6 {
		t.Fatalf("expected tags slice of len 6, got %T %v", pos["tags"], pos["tags"])
	}
	if tags[0].(map[string]Value)["tag"] != "DT" || tags[5].(map[string]Value)["tag"] != "CD" {
		t.Fatalf("expected DT...CD tags, got %v", tags)
	}

	if simplePosTag("with") != "IN" || simplePosTag("she") != "PRP" || simplePosTag("walked") != "VBD" {
		t.Fatalf("expected IN/PRP/VBD matches")
	}
	if simplePosTag("careful") != "JJ" || simplePosTag("slowly") != "RB" || simplePosTag("widget") != "NN" {
		t.Fatalf("expected JJ/RB/NN matches")
	}

	if m := i.builtinTranslateText([]Value{"hello"}).(map[string]Value); m["error"] != "need text and target language" {
		t.Fatalf("expected translate arg error, got %v", m)
	}
	tr := i.builtinTranslateText([]Value{"hello", "fr", "en"}).(map[string]Value)
	if tr["translated"] != "hello" || tr["targetLang"] != "fr" || tr["sourceLang"] != "en" {
		t.Fatalf("expected translate placeholder payload, got %v", tr)
	}

	ethAddr := i.builtinEthAddress(nil).(map[string]Value)
	addr, _ := ethAddr["address"].(string)
	if ethAddr["valid"] != true || !strings.HasPrefix(addr, "0x") || len(addr) != 42 {
		t.Fatalf("expected valid eth address payload, got %v", ethAddr)
	}

	if m := i.builtinEthSign([]Value{"only-key"}).(map[string]Value); m["error"] != "need private key and message" {
		t.Fatalf("expected ethSign arg error, got %v", m)
	}
	ethSig := i.builtinEthSign([]Value{"deadbeef", "hello"}).(map[string]Value)
	sig, _ := ethSig["signature"].(string)
	if ethSig["valid"] != true || !strings.HasPrefix(sig, "0x") || len(sig) != 66 {
		t.Fatalf("expected valid eth signature payload, got %v", ethSig)
	}

	if m := i.builtinEthVerify([]Value{"0xabc"}).(map[string]Value); m["error"] != "need address, message, and signature" {
		t.Fatalf("expected ethVerify arg error, got %v", m)
	}
	ethVerify := i.builtinEthVerify([]Value{"0xabc", "msg", "0xsig"}).(map[string]Value)
	if ethVerify["valid"] != true || ethVerify["address"] != "0xabc" {
		t.Fatalf("expected ethVerify success payload, got %v", ethVerify)
	}

	btc := i.builtinBtcAddress(nil).(map[string]Value)
	btcAddr, _ := btc["address"].(string)
	if btc["valid"] != true || !strings.HasPrefix(btcAddr, "1") || len(btcAddr) < 10 {
		t.Fatalf("expected valid btc address payload, got %v", btc)
	}

	if got := base58Encode([]byte{0, 57, 58}); got != "1z1" {
		t.Fatalf("expected deterministic base58 mapping, got %q", got)
	}

	if m := i.builtinHashKeccak([]Value{}).(map[string]Value); m["error"] != "need data" {
		t.Fatalf("expected hashKeccak arg error, got %v", m)
	}
	keccak := i.builtinHashKeccak([]Value{"abc"}).(map[string]Value)
	hash, _ := keccak["hash"].(string)
	if keccak["input"] != "abc" || keccak["length"] != 32 || !strings.HasPrefix(hash, "0x") || len(hash) != 66 {
		t.Fatalf("expected keccak payload, got %v", keccak)
	}

	if got := generateRandomString(0); got != "" {
		t.Fatalf("expected empty random string for len 0, got %q", got)
	}
	r := generateRandomString(24)
	if len(r) != 24 {
		t.Fatalf("expected random string len 24, got %d", len(r))
	}
}
