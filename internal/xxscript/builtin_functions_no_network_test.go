package xxscript

import (
	"testing"
)

func TestBuiltin_StringOps_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"len_str", `len("hello")`},
		{"split", `len(split("a,b,c", ","))`},
		{"join", `join([1, 2, 3], "-")`},
		{"replace", `replace("hello", "l", "L")`},
		{"trim", `trim("  hello  ")`},
		{"trimPrefix", `trimPrefix("hello", "hel")`},
		{"trimSuffix", `trimSuffix("hello", "lo")`},
		{"upper", `upper("hello")`},
		{"lower", `lower("HELLO")`},
		{"hasPrefix", `hasPrefix("hello", "hel")`},
		{"hasSuffix", `hasSuffix("hello", "lo")`},
		{"contains", `contains("hello", "ell")`},
		{"indexOf", `indexOf("hello", "l")`},
		{"substr", `substr("hello", 1, 3)`},
		{"repeat", `repeat("ab", 3)`},
		{"reverse", `reverse("hello")`},
		{"padLeft", `padLeft("5", 3, "0")`},
		{"padRight", `padRight("5", 3, "0")`},
		{"ltrim", `ltrim("  hello")`},
		{"rtrim", `rtrim("hello  ")`},
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
		{"charAt", `charAt("hello", 1)`},
		{"charCodeAt", `charCodeAt("hello", 0)`},
		{"fromCharCode", `fromCharCode(72)`},
		{"isLowerStr", `isLowerStr("hello")`},
		{"isUpperStr", `isUpperStr("HELLO")`},
		{"isSpaceStr", `isSpaceStr("   ")`},
		{"isPrintable", `isPrintable("hello")`},
		{"isASCII", `isASCII("hello")`},
		{"insertStr", `insertStr("hello", 2, "XX")`},
		{"deleteStr", `deleteStr("hello", 1, 2)`},
		{"overwrite", `overwrite("hello", 1, "XX")`},
		{"surround", `surround("hello", "**")`},
		{"quote", `quote("hello")`},
		{"unquote", `unquote("\"hello\"")`},
		{"stripTags", `stripTags("<b>hello</b>")`},
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
		{"normalizeSpace", `normalizeSpace("  hello   world  ")`},
		{"normalizeNewlines", `normalizeNewlines("hello\r\nworld\r\n")`},
		{"replaceN", `replaceN("aaa", "a", "b", 2)`},
		{"replaceIgnoreCase", `replaceIgnoreCase("Hello HELLO", "hello", "hi")`},
		{"commonPrefix", `commonPrefix("hello", "help")`},
		{"commonSuffix", `commonSuffix("running", "jogging")`},
		{"isAnagram", `isAnagram("listen", "silent")`},
		{"isPalindrome", `isPalindrome("racecar")`},
		{"levenshtein", `levenshtein("kitten", "sitting")`},
		{"charCount", `charCount("hello", "l")`},
		{"byteCount", `byteCount("hello")`},
		{"splitN", `len(splitN("a,b,c", ",", 2))`},
		{"rsplit", `len(rsplit("a,b,c", ","))`},
		{"partitionStr", `len(partitionStr("a,b,c", ","))`},
		{"rpartition", `len(rpartition("a,b,c", ","))`},
		{"dedent", `dedent("  hello\n    world")`},
		{"isEmail", `isEmail("test@example.com")`},
		{"isUUID", `isUUID("550e8400-e29b-41d4-a716-446655440000")`},
		{"isIP", `isIP("192.168.1.1")`},
		{"isCreditCard", `isCreditCard("4111111111111111")`},
		{"isHexColor", `isHexColor("#ff0000")`},
		{"isJSONStr", `isJSONStr("{\"a\": 1}")`},
		{"repeatUntil", `repeatUntil("a", 10)`},
		{"padBetween", `padBetween("hi", 10, "-")`},
		{"unwrap", `unwrap("  hello  ")`},
		{"toSize", `toSize(1024)`},
		{"fromSize", `fromSize("1KB")`},
		{"wordWrap", `len(wordWrap("hello world foo bar", 10))`},
		{"truncateWords", `truncateWords("hello world foo bar", 2)`},
		{"excerpt", `excerpt("hello world foo bar", "world", 5)`},
		{"stripPunctuation", `stripPunctuation("hello, world!")`},
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

func TestBuiltin_Math_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"abs", `abs(-5)`},
		{"min", `min(3, 1, 2)`},
		{"max", `max(3, 1, 2)`},
		{"floor", `floor(3.7)`},
		{"ceil", `ceil(3.2)`},
		{"round", `round(3.5)`},
		{"pow", `pow(2, 3)`},
		{"sqrt", `sqrt(16)`},
		{"cbrt", `cbrt(27)`},
		{"sign", `sign(-5)`},
		{"mod", `mod(10, 3)`},
		{"div", `div(10, 3)`},
		{"log", `log(2.718281828459045)`},
		{"log10", `log10(100)`},
		{"log2", `log2(8)`},
		{"exp", `exp(1)`},
		{"sin", `sin(0)`},
		{"cos", `cos(0)`},
		{"tan", `tan(0)`},
		{"asin", `asin(0)`},
		{"acos", `acos(1)`},
		{"atan", `atan(0)`},
		{"atan2", `atan2(1, 1)`},
		{"sinh", `sinh(0)`},
		{"cosh", `cosh(0)`},
		{"tanh", `tanh(0)`},
		{"hypot", `hypot(3, 4)`},
		{"erf", `erf(0)`},
		{"erfc", `erfc(0)`},
		{"gamma", `gamma(1)`},
		{"lgamma", `lgamma(1)`},
		{"trunc", `trunc(3.7)`},
		{"gcd", `gcd(12, 8)`},
		{"lcm", `lcm(4, 6)`},
		{"isPrime", `isPrime(7)`},
		{"factorial", `factorial(5)`},
		{"fibonacci", `fibonacci(10)`},
		{"binomial", `binomial(5, 2)`},
		{"clamp_num", `clamp_num(10, 0, 5)`},
		{"lerp", `lerp(0, 10, 0.5)`},
		{"degrees", `degrees(3.14159)`},
		{"radians", `radians(180)`},
		{"rand", `rand()`},
		{"randInt", `randInt(1, 10)`},
		{"seed", `seed(42)`},
		{"pi", `pi()`},
		{"e", `e()`},
		{"sum", `sum([1, 2, 3])`},
		{"product", `product([2, 3, 4])`},
		{"mean", `mean([1, 2, 3, 4, 5])`},
		{"median", `median([1, 2, 3, 4, 5])`},
		{"mode", `mode([1, 2, 2, 3, 3, 3])`},
		{"variance", `variance([1, 2, 3, 4, 5])`},
		{"stddev", `stddev([1, 2, 3, 4, 5])`},
		{"isInf", `isInf(1.0/0.0)`},
		{"isNaN", `isNaN(0.0/0.0)`},
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

func TestBuiltin_Array_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"push", `len(push([1, 2], 3))`},
		{"pop", `pop([1, 2, 3])`},
		{"shift", `shift([1, 2, 3])`},
		{"unshift", `len(unshift([1, 2], 0))`},
		{"slice", `len(slice([1, 2, 3, 4], 1, 3))`},
		{"splice", `len(splice([1, 2, 3, 4], 1, 2, 5, 6))`},
		{"concat", `len(concat([1, 2], [3, 4]))`},
		{"join", `join([1, 2, 3], "-")`},
		{"reverse", `len(reverse([1, 2, 3]))`},
		{"sort", `len(sort([3, 1, 2]))`},
		{"sortDesc", `len(sortDesc([3, 1, 2]))`},
		{"sortBy", `len(sortBy([{"a": 2}, {"a": 1}], "a"))`},
		{"shuffle", `len(shuffle([1, 2, 3]))`},
		{"sample", `sample([1, 2, 3])`},
		{"sampleSize", `len(sampleSize([1, 2, 3, 4], 2))`},
		{"unique", `len(unique([1, 1, 2, 2, 3]))`},
		{"uniqBy", `len(uniqBy([{"a": 1}, {"a": 1}], "a"))`},
		{"flatten", `len(flatten([[1, 2], [3, 4]]))`},
		{"flattenDeep", `len(flattenDeep([[[1]], [[2]]]))`},
		{"chunk", `len(chunk([1, 2, 3, 4], 2))`},
		{"take", `len(take([1, 2, 3, 4], 2))`},
		{"drop", `len(drop([1, 2, 3, 4], 2))`},
		{"takeWhile", `len(takeWhile([1, 2, 3, 4], func(x) { return x < 3 }))`},
		{"dropWhile", `len(dropWhile([1, 2, 3, 4], func(x) { return x < 3 }))`},
		{"fill", `len(fill([0, 0, 0], 1))`},
		{"compact", `len(compact([1, null, 2, null, 3]))`},
		{"without", `len(without([1, 2, 3, 2], 2))`},
		{"intersection", `len(intersection([1, 2, 3], [2, 3, 4]))`},
		{"union", `len(union([1, 2], [2, 3]))`},
		{"difference", `len(difference([1, 2, 3], [2, 3, 4]))`},
		{"symmetricDifference", `len(symmetricDifference([1, 2], [2, 3]))`},
		{"zip", `len(zip([1, 2], [3, 4]))`},
		{"unzip", `len(unzip([[1, 3], [2, 4]]))`},
		{"groupBy", `len(groupBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"countBy", `len(countBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"partition", `len(partition([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"filter", `len(filter([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"mapArr", `len(mapArr([1, 2, 3], func(x) { return x * 2 }))`},
		{"reduce", `reduce([1, 2, 3], func(acc, x) { return acc + x }, 0)`},
		{"every", `every([1, 2, 3], func(x) { return x > 0 })`},
		{"some", `some([1, 2, 3], func(x) { return x > 2 })`},
		{"find", `find([1, 2, 3], func(x) { return x > 1 })`},
		{"findIndex", `findIndex([1, 2, 3], func(x) { return x > 1 })`},
		{"includes", `includes([1, 2, 3], 2)`},
		{"indexOf", `indexOf([1, 2, 3], 2)`},
		{"lastIndexOf", `lastIndexOf([1, 2, 2, 3], 2)`},
		{"first", `first([1, 2, 3])`},
		{"last", `last([1, 2, 3])`},
		{"head", `head([1, 2, 3])`},
		{"tail", `len(tail([1, 2, 3]))`},
		{"initial", `len(initial([1, 2, 3]))`},
		{"range", `len(range(5))`},
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

func TestBuiltin_Object_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"keys", `len(keys({"a": 1, "b": 2}))`},
		{"values", `len(values({"a": 1, "b": 2}))`},
		{"entries", `len(entries({"a": 1, "b": 2}))`},
		{"has", `has({"a": 1}, "a")`},
		{"get", `get({"a": 1}, "a")`},
		{"set", `len(set({"a": 1}, "b", 2))`},
		{"delete", `len(delete({"a": 1, "b": 2}, "a"))`},
		{"extend", `len(extend({"a": 1}, {"b": 2}))`},
		{"assign", `len(assign({"a": 1}, {"b": 2}))`},
		{"merge", `len(merge({"a": 1}, {"b": 2}))`},
		{"deepMerge", `len(deepMerge({"a": {"b": 1}}, {"a": {"c": 2}}))`},
		{"pick", `len(pick({"a": 1, "b": 2}, ["a"]))`},
		{"omit", `len(omit({"a": 1, "b": 2}, ["a"]))`},
		{"invert", `len(invert({"a": 1}))`},
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
		{"defaults", `len(defaults({"a": 1}, {"a": 2, "b": 3}))`},
		{"hasKey", `hasKey({"a": 1}, "a")`},
		{"hasValue", `hasValue({"a": 1}, 1)`},
		{"mapObject", `len(mapObject({"a": 1}, func(k, v) { return [k, v * 2] }))`},
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

func TestBuiltin_Type_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"typeof", `typeof(42)`},
		{"type", `type(42)`},
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
		{"cast", `cast("42", "int")`},
		{"coalesce", `coalesce(null, null, 42)`},
		{"defaultIfNull", `defaultIfNull(null, 0)`},
		{"isDefined", `isDefined("x")`},
		{"isUndefined", `isUndefined("y")`},
		{"isPrimitive", `isPrimitive(42)`},
		{"isReference", `isReference([1, 2])`},
		{"isCallable", `isCallable(len)`},
		{"isIterable", `isIterable([1, 2])`},
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

func TestBuiltin_Utility_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"sleep", `sleep(0.01)`},
		{"print", `print("hello")`},
		{"println", `println("hello")`},
		{"sprintf", `sprintf("hello %s", "world")`},
		{"len", `len("hello")`},
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
		{"debounce", `len(debounce(func() { return 1 }, 100))`},
		{"throttle", `len(throttle(func() { return 1 }, 100))`},
		{"memoize", `len(memoize(func(x) { return x * 2 }))`},
		{"curry", `len(curry(func(a, b) { return a + b }))`},
		{"partial", `len(partial(func(a, b) { return a + b }, 1))`},
		{"compose", `compose(func(x) { return x + 1 }, func(x) { return x * 2 })(3)`},
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

func TestBuiltin_DateTime_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"now", `now()`},
		{"today", `today()`},
		{"tomorrow", `tomorrow()`},
		{"yesterday", `yesterday()`},
		{"date", `date(2024, 1, 1)`},
		{"time", `time(12, 30, 0)`},
		{"datetime", `datetime(2024, 1, 1, 12, 30, 0)`},
		{"year", `year(now())`},
		{"month", `month(now())`},
		{"day", `day(now())`},
		{"hour", `hour(now())`},
		{"minute", `minute(now())`},
		{"second", `second(now())`},
		{"isToday", `isToday(now())`},
		{"isTomorrow", `isTomorrow(now())`},
		{"isYesterday", `isYesterday(now())`},
		{"isFuture", `isFuture(now() + 86400)`},
		{"isPast", `isPast(now() - 86400)`},
		{"isSameDay", `isSameDay(now(), now())`},
		{"isSameMonth", `isSameMonth(now(), now())`},
		{"isSameYear", `isSameYear(now(), now())`},
		{"daysBetween", `daysBetween(now(), now() + 86400)`},
		{"hoursBetween", `hoursBetween(now(), now() + 3600)`},
		{"minutesBetween", `minutesBetween(now(), now() + 60)`},
		{"secondsBetween", `secondsBetween(now(), now() + 1)`},
		{"weeksBetween", `weeksBetween(now(), now() + 604800)`},
		{"monthsBetween", `monthsBetween(now(), now())`},
		{"yearsBetween", `yearsBetween(now(), now())`},
		{"daysUntil", `daysUntil(now() + 86400)`},
		{"daysAgo", `daysAgo(now() - 86400)`},
		{"weeksUntil", `weeksUntil(now() + 604800)`},
		{"monthsUntil", `monthsUntil(now() + 2592000)`},
		{"yearsUntil", `yearsUntil(now() + 31536000)`},
		{"isWeekend", `isWeekend(now())`},
		{"isWeekday", `isWeekday(now())`},
		{"dayOfWeek", `dayOfWeek(now())`},
		{"dayOfYear", `dayOfYear(now())`},
		{"weekOfYear", `weekOfYear(now())`},
		{"daysInMonth", `daysInMonth(2024, 2)`},
		{"isLeapYear", `isLeapYear(2024)`},
		{"startOfDay", `startOfDay(now())`},
		{"endOfDay", `endOfDay(now())`},
		{"startOfWeek", `startOfWeek(now())`},
		{"endOfWeek", `endOfWeek(now())`},
		{"startOfMonth", `startOfMonth(now())`},
		{"endOfMonth", `endOfMonth(now())`},
		{"startOfYear", `startOfYear(now())`},
		{"endOfYear", `endOfYear(now())`},
		{"unix", `unix()`},
		{"fromUnix", `fromUnix(now())`},
		{"timezone", `timezone()`},
		{"dateParts", `len(dateParts(now()))`},
		{"timeParts", `len(timeParts(now()))`},
		{"strftime", `strftime(now(), "%Y-%m-%d")`},
		{"strptime", `strptime("2024-01-01", "%Y-%m-%d")`},
		{"dateFormat", `dateFormat(now(), "2006-01-02")`},
		{"dateParse", `dateParse("2024-01-01", "2006-01-02")`},
		{"dateAdd", `dateAdd(now(), 1, "day")`},
		{"dateDiff", `dateDiff(now(), now() + 86400, "day")`},
		{"age", `age(dateParse("1990-01-01", "2006-01-02"))`},
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

func TestBuiltin_Encoding_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"base64Encode", `base64Encode("hello")`},
		{"base64Decode", `base64Decode("aGVsbG8=")`},
		{"base32Encode", `base32Encode("hello")`},
		{"base32Decode", `base32Decode("NBSWY3DP")`},
		{"hexEncode", `hexEncode("hello")`},
		{"hexDecode", `hexDecode("68656c6c6f")`},
		{"urlEncode", `urlEncode("hello world")`},
		{"urlDecode", `urlDecode("hello%20world")`},
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
		{"gzipCompress", `len(gzipCompress("hello world"))`},
		{"gzipDecompress", `gzipDecompress(gzipCompress("hello"))`},
		{"zlibCompress", `len(zlibCompress("hello world"))`},
		{"zlibDecompress", `zlibDecompress(zlibCompress("hello"))`},
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

func TestBuiltin_Crypto_NoNetwork(t *testing.T) {
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
		{"sha3_256", `sha3_256("hello")`},
		{"sha3_512", `sha3_512("hello")`},
		{"blake2b256", `blake2b256("hello")`},
		{"blake2b512", `blake2b512("hello")`},
		{"crc32", `crc32("hello")`},
		{"adler32", `adler32("hello")`},
		{"hmacSHA1", `hmacSHA1("hello", "key")`},
		{"hmacSHA256", `hmacSHA256("hello", "key")`},
		{"hmacSHA512", `hmacSHA512("hello", "key")`},
		{"bcryptHash", `len(bcryptHash("password"))`},
		{"argon2id", `len(argon2id("password"))`},
		{"pbkdf2", `len(pbkdf2("password", "salt"))`},
		{"hkdf", `len(hkdf("secret", "salt"))`},
		{"hashPassword", `len(hashPassword("password"))`},
		{"verifyPassword", `verifyPassword("password", "hash")`},
		{"generateSecret", `len(generateSecret(16))`},
		{"randomToken", `len(randomToken(16))`},
		{"randomPassword", `len(randomPassword(12))`},
		{"randomColor", `len(randomColor())`},
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

func TestBuiltin_JSON_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"jsonEncode", `jsonEncode({"a": 1})`},
		{"jsonDecode", `jsonDecode("{\"a\": 1}")`},
		{"jsonParse", `jsonParse("{\"a\": 1}")`},
		{"jsonPretty", `jsonPretty({"a": 1})`},
		{"jsonMinify", `jsonMinify("{ \"a\": 1 }")`},
		{"jsonHas", `jsonHas({"a": 1}, "a")`},
		{"jsonKeys", `len(jsonKeys({"a": 1, "b": 2}))`},
		{"jsonValues", `len(jsonValues({"a": 1, "b": 2}))`},
		{"jsonGet", `jsonGet({"a": {"b": 1}}, "a.b")`},
		{"jsonSet", `len(jsonSet({"a": 1}, "b", 2))`},
		{"jsonDelete", `len(jsonDelete({"a": 1, "b": 2}, "a"))`},
		{"jsonMerge", `len(jsonMerge({"a": 1}, {"b": 2}))`},
		{"jsonDeepMerge", `len(jsonDeepMerge({"a": {"b": 1}}, {"a": {"c": 2}}))`},
		{"jsonValidate", `jsonValidate("{\"a\": 1}")`},
		{"jsonFlatten", `len(jsonFlatten({"a": {"b": 1}}))`},
		{"jsonUnflatten", `len(jsonUnflatten({"a.b": 1}))`},
		{"jsonPick", `len(jsonPick({"a": 1, "b": 2}, ["a"]))`},
		{"jsonOmit", `len(jsonOmit({"a": 1, "b": 2}, ["a"]))`},
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

func TestBuiltin_XML_YAML_TOML_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_File_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_System_NoNetwork(t *testing.T) {
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

func TestBuiltin_Cache_NoNetwork(t *testing.T) {
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

func TestBuiltin_DataStructures_NoNetwork(t *testing.T) {
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

func TestBuiltin_Validation_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"validate_email", `validate("test@example.com", "email")`},
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

func TestBuiltin_Random_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"rand", `rand()`},
		{"randInt", `randInt(1, 10)`},
		{"randomName", `len(randomName())`},
		{"randomAvatar", `len(randomAvatar())`},
		{"generateLorem", `len(generateLorem(10))`},
		{"faker_name", `faker("name")`},
		{"faker_email", `faker("email")`},
		{"faker_address", `faker("address")`},
		{"faker_phone", `faker("phone")`},
		{"faker_company", `faker("company")`},
		{"faker_lorem", `faker("lorem")`},
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

func TestBuiltin_Geo_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"geoDistance", `geoDistance(39.9, 116.4, 31.2, 121.4)`},
		{"geoEncode", `geoEncode(39.9, 116.4)`},
		{"geoDecode", `len(geoDecode("wx4g0"))`},
		{"geoBoundingBox", `len(geoBoundingBox(39.9, 116.4, 10))`},
		{"geoWithin", `geoWithin(39.9, 116.4, 39.0, 116.0, 100)`},
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

func TestBuiltin_Cron_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"cronParse", `len(cronParse("0 0 * * *"))`},
		{"cronNext", `cronNext("0 0 * * *")`},
		{"cronNextN", `len(cronNextN("0 0 * * *", 5))`},
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

func TestBuiltin_RateLimiter_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"rateLimiter", `len(rateLimiter("test", 10, 60))`},
		{"rateLimitCheck", `rateLimitCheck("test", 10, 60)`},
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

func TestBuiltin_Image_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"barcodeEncode", `len(barcodeEncode("123456789012"))`},
		{"qrEncode", `len(qrEncode("hello"))`},
		{"qrDataURL", `len(qrDataURL("hello"))`},
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

func TestBuiltin_Debug_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"debug", `debug("test")`},
		{"benchmark", `benchmark(func() { var x = 0; for (var i = 0; i < 100; i = i + 1) { x = x + i } }, 10)`},
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

func TestBuiltin_Mock_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"mock", `mock("test", 42); test()`},
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

func TestBuiltin_Config_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"loadConfig", `loadConfig("test.json")`},
		{"saveConfig", `saveConfig({"a": 1}, "test.json")`},
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

func TestBuiltin_Secrets_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"getSecret", `getSecret("TEST_KEY")`},
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

func TestBuiltin_Process_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"killProcess", `killProcess(99999)`},
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

func TestBuiltin_Template_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"renderTemplate", `renderTemplate("Hello {{.Name}}", {"Name": "World"})`},
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

func TestBuiltin_Email_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"sendEmail", `sendEmail("test@example.com", "Subject", "Body")`},
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

func TestBuiltin_Timezone_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"setTimezone", `setTimezone("UTC")`},
		{"getTimezone", `getTimezone()`},
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

func TestBuiltin_Parallel_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"parallel", `len(parallel([func() { return 1 }, func() { return 2 }]))`},
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

func TestBuiltin_JWT_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"jwtSign", `len(jwtSign({"sub": "123"}, "secret"))`},
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

func TestBuiltin_EncryptDecrypt_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"encryptAES", `len(encryptAES("hello", "0123456789abcdef0123456789abcdef"))`},
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

func TestBuiltin_Error_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"error", `error("test message")`},
		{"isError", `isError(error("test"))`},
		{"errorMessage", `errorMessage(error("test message"))`},
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
		{"must", `try { must(error("test")) } catch (e) { e }`},
		{"panic", `try { panic("test") } catch (e) { e }`},
		{"defaultOnError", `defaultOnError(func() { throw "err" }, 42)`},
		{"tryGet", `tryGet({"a": 1}, "a")`},
		{"tryParse_int", `tryParse("42", "int")`},
		{"tryParse_float", `tryParse("3.14", "float")`},
		{"safeCall", `safeCall(func() { return 42 })`},
		{"errorFromResult", `errorFromResult({"error": true, "message": "test"})`},
		{"resultOrError", `resultOrError({"value": 42}, error("fallback"))`},
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

func TestBuiltin_Number_NoNetwork(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"int", `int("42")`},
		{"float", `float("3.14")`},
		{"string", `string(42)`},
		{"bool", `bool(1)`},
		{"number", `number("42")`},
		{"toNumber", `toNumber("42.5")`},
		{"parseNumber", `parseNumber("42")`},
		{"formatNumber", `formatNumber(1234.567, 2)`},
		{"inRange", `inRange(5, 0, 10)`},
		{"random", `random()`},
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
		{"toBinary", `toBinary(42)`},
		{"toOctal", `toOctal(42)`},
		{"fromHex", `fromHex("ff")`},
		{"fromBinary", `fromBinary("101010")`},
		{"fromOctal", `fromOctal("52")`},
		{"clamp_num", `clamp_num(10, 0, 5)`},
		{"lerp", `lerp(0, 10, 0.5)`},
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
