package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch88_CallBuiltinMoreDispatch(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		name string
		args []Value
	}{
		// Core misc
		{"keys", []Value{map[string]Value{"a": 1}}},
		{"values", []Value{map[string]Value{"a": 1}}},
		{"now", nil},
		{"rand", nil},
		{"toSize", []Value{int64(1024)}},
		{"fromSize", []Value{"1KB"}},

		// Array basics
		{"push", []Value{[]Value{1}, 2}},
		{"pop", []Value{[]Value{1, 2}}},
		{"slice", []Value{[]Value{1, 2, 3}, 1, 3}},

		// Math + stats
		{"abs", []Value{-2}},
		{"min", []Value{3, 1, 2}},
		{"max", []Value{3, 1, 2}},
		{"floor", []Value{1.9}},
		{"ceil", []Value{1.1}},
		{"round", []Value{1.5}},
		{"sqrt", []Value{9}},
		{"pow", []Value{2, 3}},
		{"sin", []Value{0}},
		{"cos", []Value{0}},
		{"tan", []Value{0}},
		{"asin", []Value{0}},
		{"acos", []Value{1}},
		{"atan", []Value{1}},
		{"atan2", []Value{1, 1}},
		{"sinh", []Value{0}},
		{"cosh", []Value{0}},
		{"tanh", []Value{0}},
		{"log", []Value{1}},
		{"log10", []Value{1}},
		{"log2", []Value{1}},
		{"exp", []Value{0}},
		{"cbrt", []Value{8}},
		{"hypot", []Value{3, 4}},
		{"sign", []Value{-2}},
		{"mod", []Value{7, 3}},
		{"div", []Value{7, 2}},
		{"clamp", []Value{5, 1, 3}},
		{"lerp", []Value{0, 10, 0.5}},
		{"degrees", []Value{3.1415926535}},
		{"radians", []Value{180}},
		{"isInf", []Value{1.0}},
		{"isNaN", []Value{1.0}},
		{"factorial", []Value{5}},
		{"gcd", []Value{18, 24}},
		{"lcm", []Value{6, 8}},
		{"isPrime", []Value{7}},
		{"fibonacci", []Value{10}},
		{"binomial", []Value{5, 2}},
		{"sum", []Value{[]Value{1, 2, 3}}},
		{"product", []Value{[]Value{2, 3, 4}}},
		{"mean", []Value{[]Value{1, 2, 3}}},
		{"median", []Value{[]Value{1, 2, 3}}},
		{"variance", []Value{[]Value{1, 2, 3}}},
		{"stddev", []Value{[]Value{1, 2, 3}}},
		{"percentile", []Value{[]Value{1, 2, 3, 4}, 50}},

		// Array processing
		{"sort", []Value{[]Value{3, 1, 2}}},
		{"sortDesc", []Value{[]Value{3, 1, 2}}},
		{"sortStrings", []Value{[]Value{"b", "a"}}},
		{"sortStringsDesc", []Value{[]Value{"b", "a"}}},
		{"arrayReverse", []Value{[]Value{1, 2, 3}}},
		{"unique", []Value{[]Value{1, 1, 2}}},
		{"flatten", []Value{[]Value{[]Value{1, 2}, 3}}},
		{"chunk", []Value{[]Value{1, 2, 3}, 2}},
		{"take", []Value{[]Value{1, 2, 3}, 2}},
		{"drop", []Value{[]Value{1, 2, 3}, 1}},
		{"first", []Value{[]Value{1, 2, 3}}},
		{"last", []Value{[]Value{1, 2, 3}}},
		{"nth", []Value{[]Value{1, 2, 3}, 1}},
		{"indicesOf", []Value{[]Value{1, 2, 1}, 1}},
		{"indexOfAll", []Value{[]Value{1, 2, 1}, 1}},
		{"splitAt", []Value{[]Value{1, 2, 3}, 1}},
		{"aperture", []Value{[]Value{1, 2, 3}, 2}},
		{"xprod", []Value{[]Value{1, 2}, []Value{"a", "b"}}},
		{"fromPairs", []Value{[]Value{[]Value{"a", 1}, []Value{"b", 2}}}},
		{"toPairs", []Value{map[string]Value{"a": 1}}},
		{"rangeStep", []Value{0, 5, 2}},
		{"repeatAll", []Value{[]Value{"a", "b"}, 2}},
		{"cycle", []Value{[]Value{1, 2}, 3}},
		{"intersperse", []Value{[]Value{1, 2, 3}, 0}},
		{"mode", []Value{[]Value{1, 1, 2}}},
		{"stdDev", []Value{[]Value{1, 2, 3}}},
		{"minBy", []Value{[]Value{map[string]Value{"v": 2}, map[string]Value{"v": 1}}, "v"}},
		{"maxBy", []Value{[]Value{map[string]Value{"v": 2}, map[string]Value{"v": 1}}, "v"}},

		// Hashing / crypto dispatch
		{"md5", []Value{"abc"}},
		{"sha1", []Value{"abc"}},
		{"sha256", []Value{"abc"}},
		{"sha512", []Value{"abc"}},
		{"sha224", []Value{"abc"}},
		{"sha384", []Value{"abc"}},
		{"sha3_256", []Value{"abc"}},
		{"sha3_512", []Value{"abc"}},
		{"blake2b256", []Value{"abc"}},
		{"blake2b512", []Value{"abc"}},
		{"crc32", []Value{"abc"}},
		{"adler32", []Value{"abc"}},
		{"hmacSHA1", []Value{"abc", "k"}},
		{"hmacSHA256", []Value{"abc", "k"}},
		{"hmacSHA512", []Value{"abc", "k"}},
		{"argon2id", []Value{"pwd", "salt"}},
		{"pbkdf2", []Value{"pwd", "salt", 10, 16}},
		{"hkdf", []Value{"key", "salt", "info", 16}},
		{"randomBytes", []Value{8}},
		{"randomHex", []Value{8}},
		{"randomString", []Value{8}},
		{"generatePassword", []Value{12}},
		{"uuid", nil},
		{"uuidv7", nil},
		{"xorEncrypt", []Value{"abc", "k"}},
		{"xorDecrypt", []Value{"abc", "k"}},
		{"jwtEncode", []Value{map[string]Value{"a": 1}, "k"}},
		{"jwtDecode", []Value{"bad.token", "k"}},

		// Path / URL / JSON helpers
		{"pathJoin", []Value{"a", "b"}},
		{"pathBase", []Value{"/tmp/a.txt"}},
		{"pathDir", []Value{"/tmp/a.txt"}},
		{"pathExt", []Value{"a.txt"}},
		{"pathClean", []Value{"a/../b"}},
		{"pathSplit", []Value{"/tmp/a.txt"}},
		{"pathIsAbs", []Value{"/tmp/a"}},
		{"urlParse", []Value{"https://example.com/a?b=1"}},
		{"urlEncode", []Value{"a b"}},
		{"urlDecode", []Value{"a+b"}},
		{"urlJoin", []Value{"https://example.com", "a"}},
		{"urlBuild", []Value{"https", "example.com", "/p", map[string]Value{"q": "1"}}},
		{"jsonEncode", []Value{map[string]Value{"a": 1}}},
		{"jsonDecode", []Value{"{\"a\":1}"}},
		{"jsonPretty", []Value{"{\"a\":1}"}},
		{"jsonMinify", []Value{"{\n  \"a\": 1\n}"}},
		{"jsonGet", []Value{"{\"a\":1}", "a"}},
		{"jsonSet", []Value{"{\"a\":1}", "b", 2}},
		{"jsonDelete", []Value{"{\"a\":1}", "a"}},
		{"jsonHas", []Value{"{\"a\":1}", "a"}},
		{"jsonKeys", []Value{"{\"a\":1}"}},
		{"jsonValues", []Value{"{\"a\":1}"}},
		{"jsonType", []Value{"{\"a\":1}"}},
		{"jsonMerge", []Value{"{\"a\":1}", "{\"b\":2}"}},
		{"jsonDeepMerge", []Value{"{\"a\":{\"x\":1}}", "{\"a\":{\"y\":2}}"}},
		{"jsonArrayLength", []Value{"[1,2,3]"}},
		{"jsonArrayAppend", []Value{"[1,2]", 3}},
		{"jsonArrayPrepend", []Value{"[2,3]", 1}},
		{"jsonArrayFlatten", []Value{"[[1],[2]]"}},
		{"jsonObjectFromArrays", []Value{"[\"a\",\"b\"]", "[1,2]"}},
		{"jsonValidate", []Value{"{\"a\":1}"}},
		{"jsonOmit", []Value{"{\"a\":1,\"b\":2}", "a"}},
		{"jsonPick", []Value{"{\"a\":1,\"b\":2}", "b"}},
		{"jsonTransform", []Value{"{\"a\":1}", map[string]Value{"a": "x"}}},

		// OS/info (read-only)
		{"pid", nil},
		{"ppid", nil},
		{"uid", nil},
		{"gid", nil},
		{"hostname", nil},
		{"arch", nil},
		{"cwd", nil},
		{"home", nil},
		{"tempDir", nil},
		{"clock", nil},
		{"timestamp", nil},
		{"dateParts", nil},
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
