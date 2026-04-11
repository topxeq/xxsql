package xxscript

import (
	"testing"
)

// Test builtin functions that are 0% coverage but can be tested directly
func TestBuiltin_ZeroCoverage_Direct(t *testing.T) {
	i := NewInterpreter(nil)

	// Test builtinThrow (cannot be reached via Run due to throw keyword conflict)
	t.Run("builtinThrow", func(t *testing.T) {
		result := i.builtinThrow([]Value{"error message"})
		if m, ok := result.(map[string]Value); !ok || m["error"] != true {
			t.Errorf("Expected error object, got %v", result)
		}
	})

	// Test builtinAssert
	t.Run("builtinAssert", func(t *testing.T) {
		result := i.builtinAssert([]Value{true})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinAssertEqual
	t.Run("builtinAssertEqual", func(t *testing.T) {
		result := i.builtinAssertEqual([]Value{int64(1), int64(1)})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinAssertNotEqual
	t.Run("builtinAssertNotEqual", func(t *testing.T) {
		result := i.builtinAssertNotEqual([]Value{int64(1), int64(2)})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinAssertNil
	t.Run("builtinAssertNil", func(t *testing.T) {
		result := i.builtinAssertNil([]Value{nil})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinAssertNotNil
	t.Run("builtinAssertNotNil", func(t *testing.T) {
		result := i.builtinAssertNotNil([]Value{int64(1)})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinAssertTrue
	t.Run("builtinAssertTrue", func(t *testing.T) {
		result := i.builtinAssertTrue([]Value{true})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinAssertFalse
	t.Run("builtinAssertFalse", func(t *testing.T) {
		result := i.builtinAssertFalse([]Value{false})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinStartsWith
	t.Run("builtinStartsWith", func(t *testing.T) {
		result := i.builtinStartsWith([]Value{"hello", "hel"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinEndsWith
	t.Run("builtinEndsWith", func(t *testing.T) {
		result := i.builtinEndsWith([]Value{"hello", "lo"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsLowerStr
	t.Run("builtinIsLowerStr", func(t *testing.T) {
		result := i.builtinIsLowerStr([]Value{"hello"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsUpperStr
	t.Run("builtinIsUpperStr", func(t *testing.T) {
		result := i.builtinIsUpperStr([]Value{"HELLO"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsSpaceStr
	t.Run("builtinIsSpaceStr", func(t *testing.T) {
		result := i.builtinIsSpaceStr([]Value{"   "})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinInsertStr
	t.Run("builtinInsertStr", func(t *testing.T) {
		result := i.builtinInsertStr([]Value{"hello", int64(2), "XX"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinPartitionStr
	t.Run("builtinPartitionStr", func(t *testing.T) {
		result := i.builtinPartitionStr([]Value{"a,b,c", ","})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIsURL
	t.Run("builtinIsURL", func(t *testing.T) {
		result := i.builtinIsURL([]Value{"http://example.com"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsJSONStr
	t.Run("builtinIsJSONStr", func(t *testing.T) {
		result := i.builtinIsJSONStr([]Value{"{\"a\": 1}"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinFormat
	t.Run("builtinFormat", func(t *testing.T) {
		result := i.builtinFormat([]Value{"hello %s", "world"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRegexCompile
	t.Run("builtinRegexCompile", func(t *testing.T) {
		result := i.builtinRegexCompile([]Value{"[a-z]+"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRegexCount
	t.Run("builtinRegexCount", func(t *testing.T) {
		result := i.builtinRegexCount([]Value{"hello", "l"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRegexGroups
	t.Run("builtinRegexGroups", func(t *testing.T) {
		result := i.builtinRegexGroups([]Value{"(a)(b)(c)", "abc"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRegexFindSubmatch
	t.Run("builtinRegexFindSubmatch", func(t *testing.T) {
		result := i.builtinRegexFindSubmatch([]Value{"(a)(b)", "ab"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRegexFindAllSubmatch
	t.Run("builtinRegexFindAllSubmatch", func(t *testing.T) {
		result := i.builtinRegexFindAllSubmatch([]Value{"(\\d)", "a1b2c3"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRegexReplaceFunc
	t.Run("builtinRegexReplaceFunc", func(t *testing.T) {
		result := i.builtinRegexReplaceFunc([]Value{"hello world", "\\w+", "UPPER"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinQuote
	t.Run("builtinQuote", func(t *testing.T) {
		result := i.builtinQuote([]Value{"hello"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinUnquote
	t.Run("builtinUnquote", func(t *testing.T) {
		result := i.builtinUnquote([]Value{"\"hello\""})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinNormalizeSpace
	t.Run("builtinNormalizeSpace", func(t *testing.T) {
		result := i.builtinNormalizeSpace([]Value{"  hello   world  "})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinNormalizeNewlines
	t.Run("builtinNormalizeNewlines", func(t *testing.T) {
		result := i.builtinNormalizeNewlines([]Value{"hello\r\nworld\r\n"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinReplaceN
	t.Run("builtinReplaceN", func(t *testing.T) {
		result := i.builtinReplaceN([]Value{"aaa", "a", "b", int64(2)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinReplaceIgnoreCase
	t.Run("builtinReplaceIgnoreCase", func(t *testing.T) {
		result := i.builtinReplaceIgnoreCase([]Value{"Hello HELLO", "hello", "hi"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinCommonPrefix
	t.Run("builtinCommonPrefix", func(t *testing.T) {
		result := i.builtinCommonPrefix([]Value{"hello", "help"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinCommonSuffix
	t.Run("builtinCommonSuffix", func(t *testing.T) {
		result := i.builtinCommonSuffix([]Value{"running", "jogging"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinCharCount
	t.Run("builtinCharCount", func(t *testing.T) {
		result := i.builtinCharCount([]Value{"hello", "l"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinByteCount
	t.Run("builtinByteCount", func(t *testing.T) {
		result := i.builtinByteCount([]Value{"hello"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinSplitN
	t.Run("builtinSplitN", func(t *testing.T) {
		result := i.builtinSplitN([]Value{"a,b,c", ",", int64(2)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRsplit
	t.Run("builtinRsplit", func(t *testing.T) {
		result := i.builtinRsplit([]Value{"a,b,c", ","})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRpartition
	t.Run("builtinRpartition", func(t *testing.T) {
		result := i.builtinRpartition([]Value{"a,b,c", ","})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinDedent
	t.Run("builtinDedent", func(t *testing.T) {
		result := i.builtinDedent([]Value{"  hello\n    world"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinRepeatUntil
	t.Run("builtinRepeatUntil", func(t *testing.T) {
		result := i.builtinRepeatUntil([]Value{"a", int64(10)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinPadBetween
	t.Run("builtinPadBetween", func(t *testing.T) {
		result := i.builtinPadBetween([]Value{"hi", int64(10), "-"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinUnwrap
	t.Run("builtinUnwrap", func(t *testing.T) {
		result := i.builtinUnwrap([]Value{"  hello  "})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinToSize
	t.Run("builtinToSize", func(t *testing.T) {
		result := i.builtinToSize([]Value{int64(1024)})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinFromSize
	t.Run("builtinFromSize", func(t *testing.T) {
		result := i.builtinFromSize([]Value{"1KB"})
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
	})

	// Test builtinIsEmail
	t.Run("builtinIsEmail", func(t *testing.T) {
		result := i.builtinIsEmail([]Value{"test@example.com"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsUUID
	t.Run("builtinIsUUID", func(t *testing.T) {
		result := i.builtinIsUUID([]Value{"550e8400-e29b-41d4-a716-446655440000"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsIP
	t.Run("builtinIsIP", func(t *testing.T) {
		result := i.builtinIsIP([]Value{"192.168.1.1"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsCreditCard
	t.Run("builtinIsCreditCard", func(t *testing.T) {
		result := i.builtinIsCreditCard([]Value{"4111111111111111"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})

	// Test builtinIsHexColor
	t.Run("builtinIsHexColor", func(t *testing.T) {
		result := i.builtinIsHexColor([]Value{"#ff0000"})
		if result != true {
			t.Errorf("Expected true, got %v", result)
		}
	})
}
