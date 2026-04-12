package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch86_CallBuiltinHandledSmoke(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		name string
		args []Value
	}{
		// Error/result helpers
		{"error", []Value{"boom"}},
		{"throw", []Value{"boom"}},
		{"assert", []Value{true}},
		{"assertEqual", []Value{1, 1}},
		{"assertNotEqual", []Value{1, 2}},
		{"assertNil", []Value{nil}},
		{"assertNotNil", []Value{"x"}},
		{"assertTrue", []Value{true}},
		{"assertFalse", []Value{false}},
		{"ok", nil},
		{"fail", []Value{"no"}},
		{"must", []Value{"x"}},
		{"recover", []Value{1}},
		{"tryParse", []Value{"{\"a\":1}"}},
		{"safeCall", nil},
		{"errorFromResult", []Value{map[string]Value{"error": true, "message": "m"}}},
		{"resultOrError", []Value{map[string]Value{"error": false, "value": 1}}},

		// Extra string cases
		{"padLeft", []Value{"x", 3, "0"}},
		{"padRight", []Value{"x", 3, "0"}},
		{"ltrim", []Value{"  x"}},
		{"rtrim", []Value{"x  "}},
		{"count", []Value{"banana", "a"}},
		{"lastIndexOf", []Value{"banana", "a"}},
		{"capitalize", []Value{"hello"}},
		{"title", []Value{"hello world"}},
		{"swapCase", []Value{"AbC"}},
		{"isAlpha", []Value{"abc"}},
		{"isNumeric", []Value{"123"}},
		{"isAlphaNumeric", []Value{"a1"}},
		{"isEmpty", []Value{" "}},
		{"truncate", []Value{"abcdef", 4}},
		{"wordCount", []Value{"a b c"}},
		{"escapeHTML", []Value{"<b>"}},
		{"unescapeHTML", []Value{"&lt;b&gt;"}},
		{"escapeURL", []Value{"a b"}},
		{"unescapeURL", []Value{"a+b"}},
		{"center", []Value{"x", 3, "-"}},

		// Encoding/decoding
		{"base64Encode", []Value{"a"}},
		{"base64Decode", []Value{"YQ=="}},
		{"base64URLEncode", []Value{"a"}},
		{"base64URLDecode", []Value{"YQ=="}},
		{"base32Encode", []Value{"a"}},
		{"base32Decode", []Value{"ME======"}},
		{"hexEncode", []Value{"a"}},
		{"hexDecode", []Value{"61"}},
		{"rot13", []Value{"abc"}},
		{"rotN", []Value{"abc", 5}},
		{"caesarEncode", []Value{"abc", 3}},
		{"caesarDecode", []Value{"def", 3}},
		{"quotedPrintableEncode", []Value{"a=b"}},
		{"quotedPrintableDecode", []Value{"a=3Db"}},
		{"htmlEntityEncode", []Value{"<"}},
		{"htmlEntityDecode", []Value{"&lt;"}},
		{"unicodeEncode", []Value{"A"}},
		{"unicodeDecode", []Value{"\\u0041"}},
		{"utf8Encode", []Value{"A"}},
		{"utf8Decode", []Value{"A"}},
		{"jsEscape", []Value{"a\nb"}},
		{"jsUnescape", []Value{"a\\nb"}},
		{"cEscape", []Value{"a\tb"}},
		{"cUnescape", []Value{"a\\tb"}},
		{"toBinary", []Value{10}},
		{"fromBinary", []Value{"1010"}},
		{"toOctal", []Value{8}},
		{"fromOctal", []Value{"10"}},
		{"asciiToHex", []Value{"A"}},
		{"hexToAscii", []Value{"41"}},
		{"strToBytes", []Value{"ab"}},
		{"bytesToStr", []Value{[]Value{97, 98}}},
		{"gzipCompress", []Value{"hello"}},
		{"isBase64", []Value{"YQ=="}},
		{"isHex", []Value{"0a"}},
		{"isBase32", []Value{"ME======"}},

		// Regex helpers
		{"regexMatch", []Value{"a", "a"}},
		{"regexFind", []Value{"ab", "b"}},
		{"regexFindAll", []Value{"aba", "a"}},
		{"regexReplace", []Value{"aba", "a", "x"}},
		{"regexSplit", []Value{"a,b", ","}},
		{"regexCompile", []Value{"a+"}},
		{"regexQuote", []Value{"a+b"}},
		{"regexCount", []Value{"aba", "a"}},
		{"regexGroups", []Value{"ab", "(a)(b)"}},
		{"regexFindSubmatch", []Value{"ab", "(a)(b)"}},
		{"regexFindAllSubmatch", []Value{"ab", "(a)(b)"}},
		{"regexReplaceFunc", []Value{"ab", "a", "x"}},
		{"regexValid", []Value{"a+"}},

		// Additional aliases / utils
		{"startsWith", []Value{"abc", "a"}},
		{"endsWith", []Value{"abc", "c"}},
		{"uuidv4", nil},
		{"randomUUID", nil},
		{"regexEscape", []Value{"a+b"}},
	}

	for _, tc := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("builtin %q panicked: %v", tc.name, r)
				}
			}()

			_, handled := i.callBuiltin(tc.name, tc.args)
			if !handled {
				t.Fatalf("builtin %q not handled", tc.name)
			}
		}()
	}
}
