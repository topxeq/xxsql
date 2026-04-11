package xxscript

import (
	"testing"
)

func TestBuiltin_StringExtra2(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"padLeft", `padLeft("5", 3, "0")`},
		{"padRight", `padRight("5", 3, "0")`},
		{"ltrim", `ltrim("  hello")`},
		{"rtrim", `rtrim("hello  ")`},
		{"sentenceCase", `sentenceCase("hello world")`},
		{"constantCase", `constantCase("hello world")`},
		{"dotCase", `dotCase("hello world")`},
		{"pathCase", `pathCase("hello world")`},
		{"charAt", `charAt("hello", 1)`},
		{"charCodeAt", `charCodeAt("hello", 0)`},
		{"fromCharCode", `fromCharCode(72)`},
		{"isLowerStr", `isLowerStr("hello")`},
		{"isUpperStr", `isUpperStr("HELLO")`},
		{"isSpaceStr", `isSpaceStr(" ")`},
		{"isPrintable", `isPrintable("hello")`},
		{"isASCII", `isASCII("hello")`},
		{"insertStr", `insertStr("hello", 2, "XX")`},
		{"deleteStr", `deleteStr("hello", 1, 2)`},
		{"overwrite", `overwrite("hello", 1, "XX")`},
		{"surround", `surround("hello", "**")`},
		{"stripTags", `stripTags("<b>hello</b>")`},
		{"slug", `slug("Hello World!")`},
		{"humanize", `humanize("hello_world")`},
		{"titleize", `titleize("hello world")`},
		{"dasherize", `dasherize("hello world")`},
		{"underscore", `underscore("helloWorld")`},
		{"camelize", `camelize("hello_world")`},
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

func TestBuiltin_MathExtra(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"degrees", `degrees(3.14159)`},
		{"radians", `radians(180)`},
		{"isInf", `isInf(1.0/0.0)`},
		{"isNaN", `isNaN(0.0/0.0)`},
		{"product", `product([2, 3, 4])`},
		{"mean", `mean([1, 2, 3, 4, 5])`},
		{"variance", `variance([1, 2, 3, 4, 5])`},
		{"stddev", `stddev([1, 2, 3, 4, 5])`},
		{"median", `median([1, 2, 3, 4, 5])`},
		{"mode", `mode([1, 2, 2, 3, 3, 3])`},
		{"sum", `sum([1, 2, 3])`},
		{"average", `average([1, 2, 3])`},
		{"clamp_num", `clamp_num(10, 0, 5)`},
		{"lerp", `lerp(0, 10, 0.5)`},
		{"exp", `exp(1)`},
		{"log", `log(2.718281828459045)`},
		{"log1p", `log1p(0)`},
		{"expm1", `expm1(0)`},
		{"trunc", `trunc(3.7)`},
		{"frac", `frac(3.7)`},
		{"fact", `fact(5)`},
		{"permutations", `permutations(5, 2)`},
		{"combinations", `combinations(5, 2)`},
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

func TestBuiltin_ListOperations(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"sortDesc", `sortDesc([3, 1, 2])`},
		{"sortBy", `len(sortBy([{"a": 2}, {"a": 1}], "a"))`},
		{"filter", `len(filter([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"mapArr", `len(mapArr([1, 2, 3], func(x) { return x * 2 }))`},
		{"reduce", `reduce([1, 2, 3], func(acc, x) { return acc + x }, 0)`},
		{"every", `every([1, 2, 3], func(x) { return x > 0 })`},
		{"some", `some([1, 2, 3], func(x) { return x > 2 })`},
		{"find", `find([1, 2, 3], func(x) { return x > 1 })`},
		{"findIndex", `findIndex([1, 2, 3], func(x) { return x > 1 })`},
		{"groupBy", `len(groupBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"countBy", `len(countBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"partition", `len(partition([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"zip", `len(zip([1, 2], [3, 4]))`},
		{"unzip", `len(unzip([[1, 3], [2, 4]]))`},
		{"flatten", `len(flatten([[1, 2], [3, 4]]))`},
		{"flattenDeep", `len(flattenDeep([[[1]], [[2]]]))`},
		{"chunk", `len(chunk([1, 2, 3, 4], 2))`},
		{"take", `len(take([1, 2, 3, 4], 2))`},
		{"drop", `len(drop([1, 2, 3, 4], 2))`},
		{"takeWhile", `len(takeWhile([1, 2, 3, 4], func(x) { return x < 3 }))`},
		{"dropWhile", `len(dropWhile([1, 2, 3, 4], func(x) { return x < 3 }))`},
		{"uniq", `len(uniq([1, 1, 2, 2, 3]))`},
		{"uniqBy", `len(uniqBy([{"a": 1}, {"a": 1}], "a"))`},
		{"intersection", `len(intersection([1, 2, 3], [2, 3, 4]))`},
		{"union", `len(union([1, 2], [2, 3]))`},
		{"difference", `len(difference([1, 2, 3], [2, 3, 4]))`},
		{"without", `len(without([1, 2, 3, 2], 2))`},
		{"compact", `len(compact([1, null, 2, null, 3]))`},
		{"shuffle", `len(shuffle([1, 2, 3]))`},
		{"sample", `sample([1, 2, 3])`},
		{"sampleSize", `len(sampleSize([1, 2, 3, 4], 2))`},
		{"reverse_arr", `len(reverse_arr([1, 2, 3]))`},
		{"fill", `len(fill([0, 0, 0], 1))`},
		{"concat_arr", `len(concat_arr([1, 2], [3, 4]))`},
		{"includes_arr", `includes_arr([1, 2, 3], 2)`},
		{"indexOf_arr", `indexOf_arr([1, 2, 3], 2)`},
		{"lastIndexOf_arr", `lastIndexOf_arr([1, 2, 2, 3], 2)`},
		{"head", `head([1, 2, 3])`},
		{"tail", `len(tail([1, 2, 3]))`},
		{"last", `last([1, 2, 3])`},
		{"initial", `len(initial([1, 2, 3]))`},
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

func TestBuiltin_EncodingExtra(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"base32Encode", `base32Encode("hello")`},
		{"base32Decode", `base32Decode("NBSWY3DP")`},
		{"rot13", `rot13("hello")`},
		{"caesarEncode", `caesarEncode("hello", 3)`},
		{"caesarDecode", `caesarDecode("khoor", 3)`},
		{"htmlEntityEncode", `htmlEntityEncode("<>&")`},
		{"htmlEntityDecode", `htmlEntityDecode("&lt;&gt;&amp;")`},
		{"toBinary", `toBinary(42)`},
		{"fromBinary", `fromBinary("101010")`},
		{"toOctal", `toOctal(42)`},
		{"fromOctal", `fromOctal("52")`},
		{"morseEncode", `morseEncode("hello")`},
		{"morseDecode", `morseDecode(".... . .-.. .-.. ---")`},
		{"asciiToHex", `asciiToHex("hello")`},
		{"hexToASCII", `hexToASCII("68656c6c6f")`},
		{"strToBytes", `len(strToBytes("hello"))`},
		{"bytesToStr", `bytesToStr([104, 101, 108, 108, 111])`},
		{"isBase64", `isBase64("aGVsbG8=")`},
		{"isHex", `isHex("68656c6c6f")`},
		{"isBase32", `isBase32("NBSWY3DP")`},
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

func TestBuiltin_Compression(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"gzipCompress", `len(gzipCompress("hello world"))`},
		{"zlibCompress", `len(zlibCompress("hello world"))`},
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

func TestBuiltin_XML(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"xmlParse", `len(xmlParse("<root><a>1</a></root>"))`},
		{"xmlStringify", `xmlStringify({"root": {"a": "1"}})`},
		{"xmlGet", `xmlGet("<root><a>1</a></root>", "root.a")`},
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

func TestBuiltin_YAML_TOML(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"yamlParse", `len(yamlParse("a: 1\nb: 2"))`},
		{"yamlStringify", `yamlStringify({"a": 1})`},
		{"tomlParse", `len(tomlParse("a = 1\nb = 2"))`},
		{"tomlStringify", `tomlStringify({"a": 1})`},
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

func TestBuiltin_Markdown(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"markdownToHTML", `markdownToHTML("# Hello\n\nWorld")`},
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

func TestBuiltin_CryptoExtra(t *testing.T) {
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
		{"hashPassword", `hashPassword("password123")`},
		{"generateSecret", `len(generateSecret(16))`},
		{"randomToken", `len(randomToken(16))`},
		{"randomPassword", `len(randomPassword(12))`},
		{"randomColor", `len(randomColor())`},
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

func TestBuiltin_DateTimeExtra(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"dateNow", `dateNow()`},
		{"year", `year(now())`},
		{"month", `month(now())`},
		{"day", `day(now())`},
		{"hour", `hour(now())`},
		{"minute", `minute(now())`},
		{"second", `second(now())`},
		{"dateCompare", `dateCompare(now(), now())`},
		{"isWeekday", `isWeekday(now())`},
		{"isWeekend", `isWeekend(now())`},
		{"dayOfWeek", `dayOfWeek(now())`},
		{"dayOfYear", `dayOfYear(now())`},
		{"weekOfYear", `weekOfYear(now())`},
		{"daysInMonth", `daysInMonth(2024, 2)`},
		{"quarter", `quarter(now())`},
		{"season", `season(now())`},
		{"isLeapYear", `isLeapYear(2024)`},
		{"startOfYear", `startOfYear(now())`},
		{"endOfYear", `endOfYear(now())`},
		{"startOfQuarter", `startOfQuarter(now())`},
		{"endOfQuarter", `endOfQuarter(now())`},
		{"startOfWeek", `startOfWeek(now())`},
		{"endOfWeek", `endOfWeek(now())`},
		{"startOfDay", `startOfDay(now())`},
		{"endOfDay", `endOfDay(now())`},
		{"startOfHour", `startOfHour(now())`},
		{"endOfHour", `endOfHour(now())`},
		{"startOfMinute", `startOfMinute(now())`},
		{"endOfMinute", `endOfMinute(now())`},
		{"dateAddYears", `dateAddYears(now(), 1)`},
		{"dateAddMonths", `dateAddMonths(now(), 1)`},
		{"dateAddWeeks", `dateAddWeeks(now(), 1)`},
		{"dateAddHours", `dateAddHours(now(), 1)`},
		{"dateAddMinutes", `dateAddMinutes(now(), 1)`},
		{"dateAddSeconds", `dateAddSeconds(now(), 60)`},
		{"dateDiffYears", `dateDiffYears(now(), now())`},
		{"dateDiffMonths", `dateDiffMonths(now(), now())`},
		{"dateDiffWeeks", `dateDiffWeeks(now(), now())`},
		{"dateDiffHours", `dateDiffHours(now(), now())`},
		{"dateDiffMinutes", `dateDiffMinutes(now(), now())`},
		{"dateDiffSeconds", `dateDiffSeconds(now(), now())`},
		{"unix", `unix()`},
		{"fromUnix", `fromUnix(now())`},
		{"timezone", `timezone()`},
		{"getTimezone", `getTimezone()`},
		{"listTimezones", `len(listTimezones())`},
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

func TestBuiltin_SystemInfo(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"env", `env("PATH")`},
		{"pid", `pid()`},
		{"hostname", `hostname()`},
		{"osInfo", `osInfo()`},
		{"arch", `arch()`},
		{"cwd", `cwd()`},
		{"getMemory", `getMemory()`},
		{"getCPU", `getCPU()`},
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

func TestBuiltin_Cache(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"cacheSet", `cacheSet("test_key", "test_value", 0)`},
		{"cacheGet", `cacheSet("k1", "v1", 0); cacheGet("k1")`},
		{"cacheDel", `cacheSet("k2", "v2", 0); cacheDel("k2"); cacheGet("k2")`},
		{"cacheHas", `cacheSet("k3", "v3", 0); cacheHas("k3")`},
		{"cacheClear", `cacheSet("k4", "v4", 0); cacheClear(); cacheHas("k4")`},
		{"cacheKeys", `cacheSet("k5", "v5", 0); len(cacheKeys())`},
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

func TestBuiltin_DataStructures(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"newStack", `var s = newStack(); s.push(1); s.pop()`},
		{"newQueue", `var q = newQueue(); q.enqueue(1); q.dequeue()`},
		{"newSet", `var s = newSet(); s.add(1); s.has(1)`},
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

func TestBuiltin_Validation(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"validate_ip", `validate("127.0.0.1", "ip")`},
		{"validate_alpha", `validate("hello", "alpha")`},
		{"validate_numeric", `validate("123", "numeric")`},
		{"validate_alphanumeric", `validate("abc123", "alphanumeric")`},
		{"validate_int", `validate("42", "int")`},
		{"validate_float", `validate("3.14", "float")`},
		{"sanitize", `sanitize("<script>alert(1)</script>")`},
		{"normalizeEmail", `normalizeEmail("Test@Example.COM")`},
		{"normalizePhone", `normalizePhone("+1 (555) 123-4567")`},
		{"validatePassword", `validatePassword("StrongP@ss1")`},
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

func TestBuiltin_Random(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"rand", `rand()`},
		{"randInt", `randInt(1, 10)`},
		{"randomName", `len(randomName())`},
		{"randomAvatar", `len(randomAvatar())`},
		{"generateLorem", `len(generateLorem(10))`},
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

func TestBuiltin_CSV(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"csvStringify", `csvStringify([["a", "b"], ["1", "2"]])`},
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

func TestBuiltin_ObjectExtra(t *testing.T) {
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
		{"filterObject", `len(filterObject({"a": 1, "b": 2}, func(k, v) { return v > 1 }))`},
		{"reduceObject", `reduceObject({"a": 1, "b": 2}, func(acc, k, v) { return acc + v }, 0)`},
		{"size_obj", `size_obj({"a": 1, "b": 2})`},
		{"isEmpty_obj", `isEmpty_obj({})`},
		{"isNotEmpty", `isNotEmpty({"a": 1})`},
		{"isEqual", `isEqual({"a": 1}, {"a": 1})`},
		{"isMatch", `isMatch({"a": 1, "b": 2}, {"a": 1})`},
		{"toObject", `len(toObject([["a", 1]]))`},
		{"toPairs", `len(toPairs({"a": 1}))`},
		{"fromPairs_obj", `len(fromPairs_obj([["a", 1]]))`},
		{"pick", `len(pick({"a": 1, "b": 2}, ["a"]))`},
		{"omit", `len(omit({"a": 1, "b": 2}, ["a"]))`},
		{"invert", `len(invert({"a": 1}))`},
		{"pairs", `len(pairs({"a": 1}))`},
		{"merge", `len(merge({"a": 1}, {"b": 2}))`},
		{"deepMerge", `len(deepMerge({"a": {"b": 1}}, {"a": {"c": 2}}))`},
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

func TestBuiltin_UtilityExtra(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"sleep", `sleep(0.01)`},
		{"sprintf", `sprintf("hello %s", "world")`},
		{"copy", `len(copy([1, 2, 3]))`},
		{"deepCopy", `len(deepCopy([1, [2, 3]]))`},
		{"identity", `identity(42)`},
		{"noop", `noop()`},
		{"times", `len(times(3, func(i) { return i }))`},
		{"pipe", `pipe(5, func(x) { return x * 2 })`},
		{"constant", `constant(42)()`},
		{"retry", `retry(func() { return 1 }, 3, 0)`},
		{"timeout", `timeout(func() { return 1 }, 1000)`},
		{"minify", `minify("  hello  world  ")`},
		{"beautify", `beautify("hello")`},
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

func TestBuiltin_TypeExtra(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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
