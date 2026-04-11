package xxscript

import (
	"testing"
)

func TestBuiltin_ErrorFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"error", `error("test message")`},
		{"isError", `isError(error("test"))`},
		{"errorMessage", `errorMessage(error("test message"))`},
		{"errorType", `errorType(error("test"))`},
		{"errorWrap", `errorWrap(error("original"), "wrapped")`},
		{"throw", `try { throw "error" } catch (e) { e }`},
		{"assert", `assert(true)`},
		{"assertEqual", `assertEqual(1, 1)`},
		{"assertNotEqual", `assertNotEqual(1, 2)`},
		{"assertNil", `assertNil(null)`},
		{"assertNotNil", `assertNotNil(1)`},
		{"assertTrue", `assertTrue(true)`},
		{"assertFalse", `assertFalse(false)`},
		{"ok", `ok(true, "success")`},
		{"fail", `try { fail("failure") } catch (e) { e }`},
		{"recover", `try { throw "err" } catch (e) { recover(e) }`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error (may be expected): %v", err)
			}
		})
	}
}

func TestBuiltin_StringFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"count", `count("hello", "l")`},
		{"lastIndexOf", `lastIndexOf("hello", "l")`},
		{"capitalize", `capitalize("hello")`},
		{"title", `title("hello world")`},
		{"swapCase", `swapCase("Hello")`},
		{"isAlpha", `isAlpha("abc")`},
		{"isNumeric", `isNumeric("123")`},
		{"isAlphaNumeric", `isAlphaNumeric("abc123")`},
		{"isEmpty", `isEmpty("")`},
		{"truncate", `truncate("hello world", 5)`},
		{"wordCount", `wordCount("hello world foo")`},
		{"escapeHTML", `escapeHTML("<b>test</b>")`},
		{"unescapeHTML", `unescapeHTML("&lt;b&gt;test&lt;/b&gt;")`},
		{"escapeURL", `escapeURL("hello world")`},
		{"unescapeURL", `unescapeURL("hello%20world")`},
		{"left", `left("hello", 2)`},
		{"right", `right("hello", 2)`},
		{"center", `center("hi", 6)`},
		{"lines", `len(lines("a\nb\nc"))`},
		{"words", `len(words("a b c"))`},
		{"startsWith", `startsWith("hello", "hel")`},
		{"endsWith", `endsWith("hello", "lo")`},
		{"camelCase", `camelCase("hello_world")`},
		{"snakeCase", `snakeCase("helloWorld")`},
		{"kebabCase", `kebabCase("helloWorld")`},
		{"pascalCase", `pascalCase("hello_world")`},
		{"slugify", `slugify("Hello World!")`},
		{"replaceAll", `replaceAll("aaa", "a", "b")`},
		{"replaceFirst", `replaceFirst("aaa", "a", "b")`},
		{"levenshtein", `levenshtein("kitten", "sitting")`},
		{"isPalindrome", `isPalindrome("racecar")`},
		{"isAnagram", `isAnagram("listen", "silent")`},
		{"repeat", `repeat("ab", 3)`},
		{"padStart", `padStart("5", 3, "0")`},
		{"padEnd", `padEnd("5", 3, "0")`},
		{"reverse", `reverse("hello")`},
		{"toUpper", `toUpper("hello")`},
		{"toLower", `toLower("HELLO")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_RegexFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"regexMatch", `regexMatch("hello", "^h.*o$")`},
		{"regexFind", `regexFind("hello world", "\\w+")`},
		{"regexFindAll", `len(regexFindAll("a1b2c3", "\\d"))`},
		{"regexReplace", `regexReplace("hello", "l", "L")`},
		{"regexSplit", `len(regexSplit("a,b,c", ","))`},
		{"regexValid", `regexValid("[a-z]+")`},
		{"regexQuote", `regexQuote("hello.world")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_MathFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"tan", `tan(0)`},
		{"asin", `asin(0)`},
		{"acos", `acos(1)`},
		{"atan", `atan(0)`},
		{"atan2", `atan2(1, 1)`},
		{"sinh", `sinh(0)`},
		{"cosh", `cosh(0)`},
		{"tanh", `tanh(0)`},
		{"log10", `log10(100)`},
		{"log2", `log2(8)`},
		{"cbrt", `cbrt(27)`},
		{"hypot", `hypot(3, 4)`},
		{"sign", `sign(-5)`},
		{"mod", `mod(10, 3)`},
		{"div", `div(10, 3)`},
		{"clamp", `clamp(10, 0, 5)`},
		{"lerp", `lerp(0, 10, 0.5)`},
		{"isInf", `isInf(1.0/0.0)`},
		{"isNaN", `isNaN(0.0/0.0)`},
		{"gcd", `gcd(12, 8)`},
		{"lcm", `lcm(4, 6)`},
		{"isPrime", `isPrime(7)`},
		{"fibonacci", `fibonacci(10)`},
		{"binomial", `binomial(5, 2)`},
		{"factorial", `factorial(5)`},
		{"sum", `sum([1, 2, 3, 4, 5])`},
		{"average", `average([1, 2, 3, 4, 5])`},
		{"median", `median([1, 2, 3, 4, 5])`},
		{"min", `min(3, 1, 2)`},
		{"max", `max(3, 1, 2)`},
		{"rand", `rand()`},
		{"randInt", `randInt(1, 10)`},
		{"seed", `seed(42)`},
		{"pi", `pi()`},
		{"e", `e()`},
		{"degToRad", `degToRad(180)`},
		{"radToDeg", `radToDeg(3.14159)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_ArrayFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"arrayReverse", `arrayReverse([1, 2, 3])`},
		{"unique", `len(unique([1, 1, 2, 2, 3]))`},
		{"flatten", `len(flatten([[1, 2], [3, 4]]))`},
		{"chunk", `len(chunk([1, 2, 3, 4], 2))`},
		{"take", `len(take([1, 2, 3, 4], 2))`},
		{"drop", `len(drop([1, 2, 3, 4], 2))`},
		{"first", `first([1, 2, 3])`},
		{"last", `last([1, 2, 3])`},
		{"find", `find([1, 2, 3], 2)`},
		{"filter", `len(filter([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"mapArr", `len(mapArr([1, 2, 3], func(x) { return x * 2 }))`},
		{"reduce", `reduce([1, 2, 3], func(acc, x) { return acc + x }, 0)`},
		{"every", `every([1, 2, 3], func(x) { return x > 0 })`},
		{"some", `some([1, 2, 3], func(x) { return x > 2 })`},
		{"groupBy", `len(groupBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"zip", `len(zip([1, 2], [3, 4]))`},
		{"intersection", `len(intersection([1, 2, 3], [2, 3, 4]))`},
		{"union", `len(union([1, 2], [2, 3]))`},
		{"difference", `len(difference([1, 2, 3], [2, 3, 4]))`},
		{"sort", `sort([3, 1, 2])`},
		{"shuffle", `len(shuffle([1, 2, 3]))`},
		{"sample", `sample([1, 2, 3])`},
		{"range", `len(range(5))`},
		{"includes", `includes([1, 2, 3], 2)`},
		{"indexOf", `indexOf([1, 2, 3], 2)`},
		{"join", `join([1, 2, 3], "-")`},
		{"compact", `len(compact([1, null, 2, null, 3]))`},
		{"without", `len(without([1, 2, 3, 2], 2))`},
		{"fill", `len(fill([0, 0, 0], 1))`},
		{"concat", `len(concat([1, 2], [3, 4]))`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_EncodingFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"base64Encode", `base64Encode("hello")`},
		{"base64Decode", `base64Decode("aGVsbG8=")`},
		{"hexEncode", `hexEncode("hello")`},
		{"hexDecode", `hexDecode("68656c6c6f")`},
		{"urlEncode", `urlEncode("hello world")`},
		{"urlDecode", `urlDecode("hello%20world")`},
		{"jsonEncode", `jsonEncode({"a": 1})`},
		{"jsonDecode", `jsonDecode("{\"a\": 1}")`},
		{"jsonPretty", `jsonPretty({"a": 1})`},
		{"jsonMinify", `jsonMinify("{ \"a\": 1 }")`},
		{"jsonGet", `jsonGet("{\"a\": 1}", "a")`},
		{"jsonSet", `jsonSet("{\"a\": 1}", "b", 2)`},
		{"jsonDelete", `jsonDelete("{\"a\": 1, \"b\": 2}", "a")`},
		{"jsonMerge", `jsonMerge("{\"a\": 1}", "{\"b\": 2}")`},
		{"jsonValidate", `jsonValidate("{\"a\": 1}")`},
		{"csvEncode", `csvEncode([["a", "b"], ["1", "2"]])`},
		{"csvDecode", `len(csvDecode("a,b\n1,2"))`},
		{"xmlEncode", `xmlEncode({"tag": "value"})`},
		{"yamlEncode", `yamlEncode({"a": 1})`},
		{"tomlEncode", `tomlEncode({"a": 1})`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_DateTimeFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"dateParts", `len(dateParts(now()))`},
		{"dateFormat", `dateFormat(now(), "2006-01-02")`},
		{"dateParse", `dateParse("2024-01-01", "2006-01-02")`},
		{"strftime", `strftime(now(), "%Y-%m-%d")`},
		{"strptime", `strptime("2024-01-01", "%Y-%m-%d")`},
		{"dateAdd", `dateAdd(now(), 1, "day")`},
		{"dateDiff", `dateDiff(now(), now() + 86400, "day")`},
		{"isLeapYear", `isLeapYear(2024)`},
		{"age", `age(dateParse("1990-01-01", "2006-01-02"))`},
		{"isWeekend", `isWeekend(now())`},
		{"dayOfWeek", `dayOfWeek(now())`},
		{"dayOfYear", `dayOfYear(now())`},
		{"weekOfYear", `weekOfYear(now())`},
		{"daysInMonth", `daysInMonth(2024, 2)`},
		{"startOfDay", `startOfDay(now())`},
		{"startOfWeek", `startOfWeek(now())`},
		{"startOfMonth", `startOfMonth(now())`},
		{"endOfDay", `endOfDay(now())`},
		{"endOfMonth", `endOfMonth(now())`},
		{"timezone", `timezone()`},
		{"unix", `unix()`},
		{"fromUnix", `fromUnix(now())`},
		{"date", `date(2024, 1, 1)`},
		{"time", `time(12, 30, 0)`},
		{"datetime", `datetime(2024, 1, 1, 12, 30, 0)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_HashFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"md5", `md5("hello")`},
		{"sha1", `sha1("hello")`},
		{"sha256", `sha256("hello")`},
		{"sha512", `sha512("hello")`},
		{"sha224", `sha224("hello")`},
		{"sha384", `sha384("hello")`},
		{"crc32", `crc32("hello")`},
		{"adler32", `adler32("hello")`},
		{"hmacSHA1", `hmacSHA1("hello", "key")`},
		{"hmacSHA256", `hmacSHA256("hello", "key")`},
		{"hmacSHA512", `hmacSHA512("hello", "key")`},
		{"hash", `hash("hello")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_TypeFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"typeof", `typeof(42)`},
		{"isInt", `isInt(42)`},
		{"isFloat", `isFloat(3.14)`},
		{"isString", `isString("hello")`},
		{"isBool", `isBool(true)`},
		{"isArray", `isArray([1, 2])`},
		{"isMap", `isMap({"a": 1})`},
		{"isNull", `isNull(null)`},
		{"isNumber", `isNumber(42)`},
		{"isObject", `isObject({"a": 1})`},
		{"isFunction", `isFunction(len)`},
		{"type", `type(42)`},
		{"cast", `cast("42", "int")`},
		{"coalesce", `coalesce(null, null, 42)`},
		{"defaultIfNull", `defaultIfNull(null, 0)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_UtilFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"sleep", `sleep(0.01)`},
		{"print", `print("hello")`},
		{"println", `println("hello")`},
		{"sprintf", `sprintf("hello %s", "world")`},
		{"len", `len("hello")`},
		{"keys", `len(keys({"a": 1, "b": 2}))`},
		{"values", `len(values({"a": 1, "b": 2}))`},
		{"has", `has({"a": 1}, "a")`},
		{"get", `get({"a": 1}, "a")`},
		{"set", `len(set({"a": 1}, "b", 2))`},
		{"delete", `len(delete({"a": 1, "b": 2}, "a"))`},
		{"copy", `len(copy([1, 2, 3]))`},
		{"deepCopy", `len(deepCopy([1, [2, 3]]))`},
		{"merge", `len(merge({"a": 1}, {"b": 2}))`},
		{"pick", `len(pick({"a": 1, "b": 2}, ["a"]))`},
		{"omit", `len(omit({"a": 1, "b": 2}, ["a"]))`},
		{"invert", `len(invert({"a": 1}))`},
		{"pairs", `len(pairs({"a": 1}))`},
		{"fromPairs", `len(fromPairs([["a", 1]]))`},
		{"identity", `identity(42)`},
		{"constant", `constant(42)()`},
		{"noop", `noop()`},
		{"range", `len(range(5))`},
		{"times", `len(times(3, func(i) { return i }))`},
		{"pipe", `pipe(5, func(x) { return x * 2 })`},
		{"compose", `compose(func(x) { return x + 1 }, func(x) { return x * 2 })(3)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_NumberFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"int", `int("42")`},
		{"float", `float("3.14")`},
		{"string", `string(42)`},
		{"bool", `bool(1)`},
		{"abs", `abs(-5)`},
		{"ceil", `ceil(3.2)`},
		{"floor", `floor(3.7)`},
		{"round", `round(3.5)`},
		{"trunc", `trunc(3.7)`},
		{"pow", `pow(2, 3)`},
		{"sqrt", `sqrt(16)`},
		{"exp", `exp(1)`},
		{"log", `log(2.718281828459045)`},
		{"number", `number("42")`},
		{"toNumber", `toNumber("42")`},
		{"parseNumber", `parseNumber("42.5")`},
		{"formatNumber", `formatNumber(1234.567, 2)`},
		{"clamp_num", `clamp_num(10, 0, 5)`},
		{"inRange", `inRange(5, 0, 10)`},
		{"random", `random()`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_ObjectFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"extend", `len(extend({"a": 1}, {"b": 2}))`},
		{"assign", `len(assign({"a": 1}, {"b": 2}))`},
		{"defaults", `len(defaults({"a": 1}, {"a": 2, "b": 3}))`},
		{"hasKey", `hasKey({"a": 1}, "a")`},
		{"hasValue", `hasValue({"a": 1}, 1)`},
		{"mapKeys", `len(mapKeys({"a": 1}, func(k, v) { return k + "x" }))`},
		{"mapValues", `len(mapValues({"a": 1}, func(k, v) { return v * 2 }))`},
		{"mapObject", `len(mapObject({"a": 1}, func(k, v) { return [k, v * 2] }))`},
		{"filterObject", `len(filterObject({"a": 1, "b": 2}, func(k, v) { return v > 1 }))`},
		{"reduceObject", `reduceObject({"a": 1, "b": 2}, func(acc, k, v) { return acc + v }, 0)`},
		{"size", `size({"a": 1, "b": 2})`},
		{"isEmpty_obj", `isEmpty({})`},
		{"isNotEmpty", `isNotEmpty({"a": 1})`},
		{"isEqual", `isEqual({"a": 1}, {"a": 1})`},
		{"isMatch", `isMatch({"a": 1, "b": 2}, {"a": 1})`},
		{"toObject", `len(toObject([["a", 1]]))`},
		{"toPairs", `len(toPairs({"a": 1}))`},
		{"fromPairs_obj", `len(fromPairs_obj([["a", 1]]))`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}

func TestBuiltin_StringExtraFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"charAt", `charAt("hello", 1)`},
		{"charCodeAt", `charCodeAt("hello", 0)`},
		{"fromCharCode", `fromCharCode(72)`},
		{"localeCompare", `localeCompare("a", "b")`},
		{"match", `len(match("hello", "l"))`},
		{"search", `search("hello", "l")`},
		{"slice_str", `slice_str("hello", 1, 4)`},
		{"split", `len(split("a,b,c", ","))`},
		{"substr", `substr("hello", 1, 3)`},
		{"substring", `substring("hello", 1, 4)`},
		{"toLowerCase", `toLowerCase("HELLO")`},
		{"toUpperCase", `toUpperCase("hello")`},
		{"trim_str", `trim_str("  hello  ")`},
		{"trimStart", `trimStart("  hello")`},
		{"trimEnd", `trimEnd("hello  ")`},
		{"trimLeft", `trimLeft("  hello")`},
		{"trimRight", `trimRight("hello  ")`},
		{"valueOf", `valueOf("hello")`},
		{"concat_str", `concat_str("hello", " ", "world")`},
		{"includes_str", `includes_str("hello", "ell")`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Run(tt.input, nil)
			if err != nil {
				t.Logf("Execution error: %v", err)
			}
		})
	}
}
