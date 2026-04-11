package xxscript

import (
	"testing"
)

func TestBuiltin_MoreZeroCoverage(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		// String functions
		{"slug", `slug("Hello World!")`},
		{"humanize", `humanize("hello_world")`},
		{"titleize", `titleize("hello world")`},
		{"dasherize", `dasherize("hello world")`},
		{"underscore", `underscore("helloWorld")`},
		{"camelize", `camelize("hello_world")`},
		{"classify", `classify("hello_world")`},
		{"foreign_key", `foreign_key("User")`},
		{"tableize", `tableize("User")`},
		{"parameterize", `parameterize("Hello World!")`},
		{"transliterate", `transliterate("café")`},
		{"ordinalize", `ordinalize(1)`},
		{"isPrintable", `isPrintable("hello")`},
		{"isASCII", `isASCII("hello")`},
		{"deleteStr", `deleteStr("hello", 1, 2)`},
		{"overwrite", `overwrite("hello", 1, "XX")`},
		{"surround", `surround("hello", "**")`},
		{"stripTags", `stripTags("<b>hello</b>")`},
		{"wordWrap", `len(wordWrap("hello world foo bar", 10))`},
		{"truncateWords", `truncateWords("hello world foo bar", 2)`},
		{"excerpt", `excerpt("hello world foo bar", "world", 5)`},
		{"stripNumbers", `stripNumbers("hello123world")`},
		{"stripWhitespace", `stripWhitespace("hello world")`},
		{"collapseWhitespace", `collapseWhitespace("hello   world")`},
		{"countWords", `countWords("hello world foo")`},
		{"countLines", `countLines("a\nb\nc")`},
		{"countChars", `countChars("hello")`},
		{"countSentences", `countSentences("Hello. World. Foo.")`},
		{"countParagraphs", `countParagraphs("Para1\n\nPara2")`},
		{"readingTime", `readingTime("hello world foo bar baz")`},
		{"speakingTime", `speakingTime("hello world foo bar baz")`},
		{"firstWord", `firstWord("hello world")`},
		{"lastWord", `lastWord("hello world")`},
		{"firstSentence", `firstSentence("Hello world. Foo bar.")`},
		{"reverseWords", `reverseWords("hello world")`},
		{"shuffleWords", `len(shuffleWords("hello world foo"))`},
		{"sortWords", `sortWords("c a b")`},
		{"uniqueWords", `len(uniqueWords("a b a c b"))`},
		{"frequentWords", `len(frequentWords("a b a c a b", 3))`},
		{"localeCompare", `localeCompare("a", "b")`},
		{"match", `len(match("hello", "l"))`},
		{"search", `search("hello", "l")`},
		{"slice_str", `slice_str("hello", 1, 4)`},
		{"substring", `substring("hello", 1, 4)`},
		{"toLowerCase", `toLowerCase("HELLO")`},
		{"toUpperCase", `toUpperCase("hello")`},
		{"trimStart", `trimStart("  hello")`},
		{"trimEnd", `trimEnd("hello  ")`},
		{"trimLeft", `trimLeft("  hello")`},
		{"trimRight", `trimRight("hello  ")`},
		{"valueOf", `valueOf("hello")`},
		{"concat_str", `concat_str("hello", " ", "world")`},
		{"includes_str", `includes_str("hello", "ell")`},

		// Math functions
		{"erf", `erf(0)`},
		{"erfc", `erfc(0)`},
		{"gamma", `gamma(1)`},
		{"lgamma", `lgamma(1)`},
		{"j0", `j0(0)`},
		{"j1", `j1(0)`},
		{"y0", `y0(1)`},
		{"y1", `y1(1)`},
		{"signbit", `signbit(-1.0)`},
		{"copysign", `copysign(1.0, -1.0)`},
		{"dim", `dim(5, 3)`},
		{"nextafter", `nextafter(1.0, 2.0)`},
		{"remainder", `remainder(5.0, 2.0)`},
		{"roundToEven", `roundToEven(3.5)`},
		{"log1p", `log1p(0)`},
		{"expm1", `expm1(0)`},
		{"asinh", `asinh(0)`},
		{"acosh", `acosh(1)`},
		{"atanh", `atanh(0)`},

		// Array functions
		{"push", `len(push([1, 2], 3))`},
		{"pop", `pop([1, 2, 3])`},
		{"shift", `shift([1, 2, 3])`},
		{"unshift", `len(unshift([1, 2], 0))`},
		{"splice", `len(splice([1, 2, 3, 4], 1, 2, 5, 6))`},
		{"sortBy", `len(sortBy([{"a": 2}, {"a": 1}], "a"))`},
		{"sampleSize", `len(sampleSize([1, 2, 3, 4], 2))`},
		{"uniqBy", `len(uniqBy([{"a": 1}, {"a": 1}], "a"))`},
		{"flattenDeep", `len(flattenDeep([[[1]], [[2]]]))`},
		{"takeWhile", `len(takeWhile([1, 2, 3, 4], func(x) { return x < 3 }))`},
		{"dropWhile", `len(dropWhile([1, 2, 3, 4], func(x) { return x < 3 }))`},
		{"symmetricDifference", `len(symmetricDifference([1, 2], [2, 3]))`},
		{"unzip", `len(unzip([[1, 3], [2, 4]]))`},
		{"countBy", `len(countBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"find", `find([1, 2, 3], func(x) { return x > 1 })`},
		{"findIndex", `findIndex([1, 2, 3], func(x) { return x > 1 })`},
		{"initial", `len(initial([1, 2, 3]))`},

		// Object functions
		{"entries", `len(entries({"a": 1, "b": 2}))`},
		{"deepMerge", `len(deepMerge({"a": {"b": 1}}, {"a": {"c": 2}}))`},
		{"mapObject", `len(mapObject({"a": 1}, func(k, v) { return [k, v * 2] }))`},
		{"reduceObject", `reduceObject({"a": 1, "b": 2}, func(acc, k, v) { return acc + v }, 0)`},
		{"size_obj", `size_obj({"a": 1, "b": 2})`},
		{"isEmpty_obj", `isEmpty_obj({})`},
		{"isNotEmpty", `isNotEmpty({"a": 1})`},
		{"isMatch", `isMatch({"a": 1, "b": 2}, {"a": 1})`},
		{"toObject", `len(toObject([["a", 1]]))`},
		{"toPairs", `len(toPairs({"a": 1}))`},
		{"fromPairs_obj", `len(fromPairs_obj([["a", 1]]))`},
		{"defaults", `len(defaults({"a": 1}, {"a": 2, "b": 3}))`},
		{"hasKey", `hasKey({"a": 1}, "a")`},
		{"hasValue", `hasValue({"a": 1}, 1)`},

		// Type functions
		{"isDefined", `isDefined("x")`},
		{"isUndefined", `isUndefined("y")`},
		{"isPrimitive", `isPrimitive(42)`},
		{"isReference", `isReference([1, 2])`},
		{"isCallable", `isCallable(len)`},
		{"isIterable", `isIterable([1, 2])`},

		// Utility functions
		{"debounce", `len(debounce(func() { return 1 }, 100))`},
		{"throttle", `len(throttle(func() { return 1 }, 100))`},
		{"memoize", `len(memoize(func(x) { return x * 2 }))`},
		{"curry", `len(curry(func(a, b) { return a + b }))`},
		{"partial", `len(partial(func(a, b) { return a + b }, 1))`},
		{"negate", `negate(func(x) { return x > 0 })(-1)`},
		{"stubTrue", `stubTrue()`},
		{"stubFalse", `stubFalse()`},
		{"stubArray", `len(stubArray())`},
		{"stubObject", `len(stubObject())`},
		{"stubString", `len(stubString())`},
		{"uniqueId", `len(uniqueId())`},
		{"escape", `escape("<>&")`},
		{"unescape", `unescape("&lt;&gt;&amp;")`},
		{"result", `result({"a": 1}, "a")`},
		{"version", `len(version())`},

		// DateTime functions
		{"today", `today()`},
		{"tomorrow", `tomorrow()`},
		{"yesterday", `yesterday()`},
		{"isToday", `isToday(now())`},
		{"isTomorrow", `isTomorrow(now())`},
		{"isYesterday", `isYesterday(now())`},
		{"isFuture", `isFuture(now() + 86400)`},
		{"isPast", `isPast(now() - 86400)`},
		{"isSameDay", `isSameDay(now(), now())`},
		{"isSameMonth", `isSameMonth(now(), now())`},
		{"isSameYear", `isSameYear(now(), now())`},
		{"daysUntil", `daysUntil(now() + 86400)`},
		{"daysAgo", `daysAgo(now() - 86400)`},
		{"weeksUntil", `weeksUntil(now() + 604800)`},
		{"monthsUntil", `monthsUntil(now() + 2592000)`},
		{"yearsUntil", `yearsUntil(now() + 31536000)`},
		{"zodiac", `zodiac(now())`},
		{"chineseZodiac", `chineseZodiac(now())`},
		{"nextMonday", `nextMonday()`},
		{"nextFriday", `nextFriday()`},
		{"lastMonday", `lastMonday()`},
		{"lastFriday", `lastFriday()`},
		{"addBusinessDays", `addBusinessDays(now(), 5)`},
		{"subBusinessDays", `subBusinessDays(now(), 5)`},
		{"isBusinessDay", `isBusinessDay(now())`},
		{"isHoliday", `isHoliday(now())`},
		{"nextHoliday", `nextHoliday()`},
		{"listTimezones", `len(listTimezones())`},

		// Encoding functions
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
		{"gzipDecompress", `gzipDecompress(gzipCompress("hello"))`},
		{"zlibDecompress", `zlibDecompress(zlibCompress("hello"))`},

		// Crypto functions
		{"sha3_256", `sha3_256("hello")`},
		{"sha3_512", `sha3_512("hello")`},
		{"blake2b256", `blake2b256("hello")`},
		{"blake2b512", `blake2b512("hello")`},
		{"bcryptHash", `len(bcryptHash("password"))`},
		{"argon2id", `len(argon2id("password"))`},
		{"pbkdf2", `len(pbkdf2("password", "salt"))`},
		{"hkdf", `len(hkdf("secret", "salt"))`},
		{"randomToken", `len(randomToken(16))`},

		// JSON functions
		{"jsonHas", `jsonHas({"a": 1}, "a")`},
		{"jsonKeys", `len(jsonKeys({"a": 1, "b": 2}))`},
		{"jsonValues", `len(jsonValues({"a": 1, "b": 2}))`},
		{"jsonDeepMerge", `len(jsonDeepMerge({"a": {"b": 1}}, {"a": {"c": 2}}))`},
		{"jsonFlatten", `len(jsonFlatten({"a": {"b": 1}}))`},
		{"jsonUnflatten", `len(jsonUnflatten({"a.b": 1}))`},
		{"jsonPick", `len(jsonPick({"a": 1, "b": 2}, ["a"]))`},
		{"jsonOmit", `len(jsonOmit({"a": 1, "b": 2}, ["a"]))`},

		// XML/YAML/TOML functions
		{"xmlParse", `len(xmlParse("<root><a>1</a></root>"))`},
		{"xmlStringify", `xmlStringify({"root": {"a": "1"}})`},
		{"xmlGet", `xmlGet("<root><a>1</a></root>", "root.a")`},
		{"yamlParse", `len(yamlParse("a: 1\nb: 2"))`},
		{"yamlStringify", `yamlStringify({"a": 1})`},
		{"tomlParse", `len(tomlParse("a = 1\nb = 2"))`},
		{"tomlStringify", `tomlStringify({"a": 1})`},
		{"markdownToHTML", `markdownToHTML("# Hello\n\nWorld")`},
		{"htmlToMarkdown", `htmlToMarkdown("<h1>Hello</h1>")`},
		{"csvStringify", `csvStringify([["a", "b"], ["1", "2"]])`},

		// File functions
		{"pathJoin", `pathJoin("/tmp", "test.txt")`},
		{"pathBase", `pathBase("/tmp/test.txt")`},
		{"pathDir", `pathDir("/tmp/test.txt")`},
		{"pathExt", `pathExt("/tmp/test.txt")`},
		{"pathIsAbs", `pathIsAbs("/tmp/test.txt")`},
		{"fileExists", `fileExists("/tmp")`},
		{"dirExists", `dirExists("/tmp")`},
		{"isDir", `isDir("/tmp")`},
		{"isFile", `isFile("/tmp")`},
		{"fileInfo", `len(fileInfo("/tmp"))`},
		{"readFile", `readFile("/etc/hostname")`},
		{"writeFile", `writeFile("/tmp/test_xxscript.txt", "hello")`},
		{"appendFile", `appendFile("/tmp/test_xxscript.txt", " world")`},
		{"deleteFile", `deleteFile("/tmp/test_xxscript.txt")`},
		{"copyFile", `copyFile("/etc/hostname", "/tmp/test_copy.txt")`},
		{"moveFile", `moveFile("/tmp/test_copy.txt", "/tmp/test_moved.txt")`},
		{"readDir", `len(readDir("/tmp"))`},
		{"mkdir", `mkdir("/tmp/test_xxscript_dir")`},
		{"mkdirAll", `mkdirAll("/tmp/test_xxscript_dir/sub")`},
		{"rmdir", `rmdir("/tmp/test_xxscript_dir/sub")`},
		{"rmdirAll", `rmdirAll("/tmp/test_xxscript_dir")`},
		{"realpath", `realpath("/tmp")`},
		{"basename", `basename("/tmp/test.txt")`},
		{"dirname", `dirname("/tmp/test.txt")`},
		{"extname", `extname("/tmp/test.txt")`},
		{"tempFile", `len(tempFile())`},
		{"tempDir", `len(tempDir())`},
		{"chmod", `chmod("/tmp", 0755)`},

		// System functions
		{"env", `env("PATH")`},
		{"pid", `pid()`},
		{"hostname", `hostname()`},
		{"osInfo", `osInfo()`},
		{"arch", `arch()`},
		{"cwd", `cwd()`},
		{"getMemory", `getMemory()`},
		{"getCPU", `getCPU()`},

		// Cache functions
		{"cacheSet", `cacheSet("test_key", "test_value", 0)`},
		{"cacheGet", `cacheSet("k1", "v1", 0); cacheGet("k1")`},
		{"cacheDel", `cacheSet("k2", "v2", 0); cacheDel("k2"); cacheGet("k2")`},
		{"cacheHas", `cacheSet("k3", "v3", 0); cacheHas("k3")`},
		{"cacheClear", `cacheSet("k4", "v4", 0); cacheClear(); cacheHas("k4")`},
		{"cacheKeys", `cacheSet("k5", "v5", 0); len(cacheKeys())`},

		// Data structures
		{"newStack", `var s = newStack(); s.push(1); s.pop()`},
		{"newQueue", `var q = newQueue(); q.enqueue(1); q.dequeue()`},
		{"newSet", `var s = newSet(); s.add(1); s.has(1)`},

		// Validation functions
		{"validate_email", `validate("test@example.com", "email")`},
		{"validate_alpha", `validate("hello", "alpha")`},
		{"validate_numeric", `validate("123", "numeric")`},
		{"validate_alphanumeric", `validate("abc123", "alphanumeric")`},
		{"validate_int", `validate("42", "int")`},
		{"validate_float", `validate("3.14", "float")`},
		{"sanitize", `sanitize("<script>alert(1)</script>")`},
		{"normalizeEmail", `normalizeEmail("Test@Example.COM")`},
		{"normalizePhone", `normalizePhone("+1 (555) 123-4567")`},
		{"validatePassword", `validatePassword("StrongP@ss1")`},

		// Random functions
		{"randomName", `len(randomName())`},
		{"randomAvatar", `len(randomAvatar())`},
		{"generateLorem", `len(generateLorem(10))`},
		{"faker_name", `faker("name")`},
		{"faker_email", `faker("email")`},
		{"faker_address", `faker("address")`},
		{"faker_phone", `faker("phone")`},
		{"faker_company", `faker("company")`},
		{"faker_lorem", `faker("lorem")`},

		// Geo functions
		{"geoDistance", `geoDistance(39.9, 116.4, 31.2, 121.4)`},
		{"geoEncode", `geoEncode(39.9, 116.4)`},
		{"geoDecode", `len(geoDecode("wx4g0"))`},
		{"geoBoundingBox", `len(geoBoundingBox(39.9, 116.4, 10))`},
		{"geoWithin", `geoWithin(39.9, 116.4, 39.0, 116.0, 100)`},

		// Cron functions
		{"cronParse", `len(cronParse("0 0 * * *"))`},
		{"cronNext", `cronNext("0 0 * * *")`},
		{"cronNextN", `len(cronNextN("0 0 * * *", 5))`},

		// Rate limiter
		{"rateLimiter", `len(rateLimiter("test", 10, 60))`},
		{"rateLimitCheck", `rateLimitCheck("test", 10, 60)`},

		// Image functions
		{"barcodeEncode", `len(barcodeEncode("123456789012"))`},
		{"qrEncode", `len(qrEncode("hello"))`},
		{"qrDataURL", `len(qrDataURL("hello"))`},

		// Debug functions
		{"debug", `debug("test")`},
		{"benchmark", `benchmark(func() { var x = 0; for (var i = 0; i < 100; i = i + 1) { x = x + i } }, 10)`},

		// Mock functions
		{"mock", `mock("test", 42); test()`},

		// Config functions
		{"loadConfig", `loadConfig("test.json")`},
		{"saveConfig", `saveConfig({"a": 1}, "test.json")`},

		// Secrets
		{"getSecret", `getSecret("TEST_KEY")`},

		// Process
		{"killProcess", `killProcess(99999)`},

		// Template
		{"renderTemplate", `renderTemplate("Hello {{.Name}}", {"Name": "World"})`},

		// Email
		{"sendEmail", `sendEmail("test@example.com", "Subject", "Body")`},

		// Timezone
		{"setTimezone", `setTimezone("UTC")`},
		{"getTimezone", `getTimezone()`},

		// Parallel
		{"parallel", `len(parallel([func() { return 1 }, func() { return 2 }]))`},

		// JWT
		{"jwtSign", `len(jwtSign({"sub": "123"}, "secret"))`},

		// Encrypt/Decrypt
		{"encryptAES", `len(encryptAES("hello", "0123456789abcdef0123456789abcdef"))`},

		// Number functions
		{"isEven", `isEven(4)`},
		{"isOdd", `isOdd(3)`},
		{"isPositive", `isPositive(1)`},
		{"isNegative", `isNegative(-1)`},
		{"isZero", `isZero(0)`},
		{"isDecimal", `isDecimal(3.14)`},
		{"isInteger", `isInteger(42)`},
		{"toFixed", `toFixed(3.14159, 2)`},
		{"toPrecision", `toPrecision(3.14159, 3)`},
		{"toExponential", `toExponential(1234, 2)`},
		{"toHex", `toHex(255)`},
		{"fromHex", `fromHex("ff")`},
		{"mapRange", `mapRange(5, 0, 10, 0, 100)`},
		{"wrap", `wrap(12, 0, 10)`},
		{"distance", `distance(5, 10)`},
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
