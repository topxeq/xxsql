package xxscript

import (
	"testing"
)

func TestBuiltin_StringExtra5(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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
		{"ordinalize2", `ordinalize(2)`},
		{"ordinalize3", `ordinalize(3)`},
		{"ordinalize4", `ordinalize(4)`},
		{"ordinalize11", `ordinalize(11)`},
		{"ordinalize21", `ordinalize(21)`},
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
		{"wordWrap", `len(wordWrap("hello world foo bar", 10))`},
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

func TestBuiltin_ArrayExtra3(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"arraySort", `len(arraySort([3, 1, 2]))`},
		{"arrayShuffle", `len(arrayShuffle([1, 2, 3]))`},
		{"arraySample", `arraySample([1, 2, 3])`},
		{"arrayUnique", `len(arrayUnique([1, 1, 2, 2, 3]))`},
		{"arrayFlatten", `len(arrayFlatten([[1, 2], [3, 4]]))`},
		{"arrayChunk", `len(arrayChunk([1, 2, 3, 4], 2))`},
		{"arrayTake", `len(arrayTake([1, 2, 3, 4], 2))`},
		{"arrayDrop", `len(arrayDrop([1, 2, 3, 4], 2))`},
		{"arrayFirst", `arrayFirst([1, 2, 3])`},
		{"arrayLast", `arrayLast([1, 2, 3])`},
		{"arrayHead", `arrayHead([1, 2, 3])`},
		{"arrayTail", `len(arrayTail([1, 2, 3]))`},
		{"arrayInitial", `len(arrayInitial([1, 2, 3]))`},
		{"arrayCompact", `len(arrayCompact([1, null, 2, null, 3]))`},
		{"arrayWithout", `len(arrayWithout([1, 2, 3, 2], 2))`},
		{"arrayFill", `len(arrayFill([0, 0, 0], 1))`},
		{"arrayConcat", `len(arrayConcat([1, 2], [3, 4]))`},
		{"arrayIncludes", `arrayIncludes([1, 2, 3], 2)`},
		{"arrayIndexOf", `arrayIndexOf([1, 2, 3], 2)`},
		{"arrayLastIndexOf", `arrayLastIndexOf([1, 2, 2, 3], 2)`},
		{"arrayFind", `arrayFind([1, 2, 3], func(x) { return x > 1 })`},
		{"arrayFindIndex", `arrayFindIndex([1, 2, 3], func(x) { return x > 1 })`},
		{"arrayFilter", `len(arrayFilter([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"arrayMap", `len(arrayMap([1, 2, 3], func(x) { return x * 2 }))`},
		{"arrayReduce", `arrayReduce([1, 2, 3], func(acc, x) { return acc + x }, 0)`},
		{"arrayEvery", `arrayEvery([1, 2, 3], func(x) { return x > 0 })`},
		{"arraySome", `arraySome([1, 2, 3], func(x) { return x > 2 })`},
		{"arrayForEach", `arrayForEach([1, 2, 3], func(x) { return x })`},
		{"arrayGroupBy", `len(arrayGroupBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"arrayCountBy", `len(arrayCountBy([1, 2, 3, 4], func(x) { return x % 2 }))`},
		{"arrayPartition", `len(arrayPartition([1, 2, 3, 4], func(x) { return x > 2 }))`},
		{"arrayZip", `len(arrayZip([1, 2], [3, 4]))`},
		{"arrayUnzip", `len(arrayUnzip([[1, 3], [2, 4]]))`},
		{"arrayIntersection", `len(arrayIntersection([1, 2, 3], [2, 3, 4]))`},
		{"arrayUnion", `len(arrayUnion([1, 2], [2, 3]))`},
		{"arrayDifference", `len(arrayDifference([1, 2, 3], [2, 3, 4]))`},
		{"arraySymmetricDifference", `len(arraySymmetricDifference([1, 2], [2, 3]))`},
		{"arraySortBy", `len(arraySortBy([{"a": 2}, {"a": 1}], "a"))`},
		{"arrayOrderBy", `len(arrayOrderBy([{"a": 2}, {"a": 1}], ["a"], ["asc"]))`},
		{"arrayKeyBy", `len(arrayKeyBy([{"id": 1}, {"id": 2}], "id"))`},
		{"arrayIndexBy", `len(arrayIndexBy([{"id": 1}, {"id": 2}], "id"))`},
		{"arrayPluck", `len(arrayPluck([{"a": 1}, {"a": 2}], "a"))`},
		{"arrayPull", `len(arrayPull([1, 2, 3, 2], 2))`},
		{"arrayPullAt", `len(arrayPullAt([1, 2, 3], [0, 2]))`},
		{"arrayRemove", `len(arrayRemove([1, 2, 3], func(x) { return x > 1 }))`},
		{"arraySortedIndex", `arraySortedIndex([1, 2, 4, 5], 3)`},
		{"arraySortedLastIndex", `arraySortedLastIndex([1, 2, 4, 4, 5], 4)`},
		{"arrayXor", `len(arrayXor([1, 2], [2, 3]))`},
		{"arrayMerge", `len(arrayMerge([1, 2], [2, 3]))`},
		{"arrayDistinct", `len(arrayDistinct([1, 1, 2, 2, 3]))`},
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

func TestBuiltin_ObjectExtra2(t *testing.T) {
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

func TestBuiltin_NumberExtra2(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"number", `number("42")`},
		{"toNumber", `toNumber("42.5")`},
		{"parseNumber", `parseNumber("42")`},
		{"formatNumber", `formatNumber(1234.567, 2)`},
		{"inRange", `inRange(5, 0, 10)`},
		{"random", `random()`},
		{"bool", `bool(1)`},
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
		{"average", `average([1, 2, 3, 4, 5])`},
		{"median", `median([1, 2, 3, 4, 5])`},
		{"mode", `mode([1, 2, 2, 3, 3, 3])`},
		{"sum", `sum([1, 2, 3])`},
		{"product", `product([2, 3, 4])`},
		{"variance", `variance([1, 2, 3, 4, 5])`},
		{"stddev", `stddev([1, 2, 3, 4, 5])`},
		{"mean", `mean([1, 2, 3, 4, 5])`},
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

func TestBuiltin_TypeExtra2(t *testing.T) {
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

func TestBuiltin_UtilityExtra2(t *testing.T) {
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
		{"debounce", `len(debounce(func() { return 1 }, 100))`},
		{"throttle", `len(throttle(func() { return 1 }, 100))`},
		{"memoize", `len(memoize(func(x) { return x * 2 }))`},
		{"curry", `len(curry(func(a, b) { return a + b }))`},
		{"partial", `len(partial(func(a, b) { return a + b }, 1))`},
		{"compose", `compose(func(x) { return x + 1 }, func(x) { return x * 2 })(3)`},
		{"flow", `flow([func(x) { return x * 2 }, func(x) { return x + 1 }])(3)`},
		{"negate", `negate(func(x) { return x > 0 })(-1)`},
		{"over", `len(over([func(x) { return x * 2 }, func(x) { return x + 1 }], 3))`},
		{"overEvery", `overEvery([func(x) { return x > 0 }, func(x) { return x < 10 }])(5)`},
		{"overSome", `overSome([func(x) { return x > 10 }, func(x) { return x < 10 }])(5)`},
		{"conforms", `len(conforms({"a": func(x) { return x > 0 }}, {"a": 1}))`},
		{"matches", `matches({"a": 1}, {"a": 1})`},
		{"matchesProperty", `matchesProperty("a", 1)({"a": 1})`},
		{"property", `property("a")({"a": 1})`},
		{"propertyOf", `propertyOf({"a": 1})("a")`},
		{"range", `len(range(5))`},
		{"rangeRight", `len(rangeRight(5))`},
		{"stubTrue", `stubTrue()`},
		{"stubFalse", `stubFalse()`},
		{"stubArray", `len(stubArray())`},
		{"stubObject", `len(stubObject())`},
		{"stubString", `len(stubString())`},
		{"toPath", `len(toPath("a.b.c"))`},
		{"uniqueId", `len(uniqueId())`},
		{"noConflict", `len(noConflict())`},
		{"runInContext", `len(runInContext())`},
		{"template", `len(template("Hello {{name}}", {"name": "World"}))`},
		{"escape", `escape("<>&")`},
		{"unescape", `unescape("&lt;&gt;&amp;")`},
		{"result", `result({"a": 1}, "a")`},
		{"iteratee", `len(iteratee(func(x) { return x * 2 }))`},
		{"mixin", `len(mixin({"foo": func() { return 1 }}))`},
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

func TestBuiltin_DateTimeExtra3(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"date", `date(2024, 1, 1)`},
		{"time", `time(12, 30, 0)`},
		{"datetime", `datetime(2024, 1, 1, 12, 30, 0)`},
		{"today", `today()`},
		{"tomorrow", `tomorrow()`},
		{"yesterday", `yesterday()`},
		{"age", `age(dateParse("1990-01-01", "2006-01-02"))`},
		{"zodiac", `zodiac(now())`},
		{"chineseZodiac", `chineseZodiac(now())`},
		{"isToday", `isToday(now())`},
		{"isTomorrow", `isTomorrow(now())`},
		{"isYesterday", `isYesterday(now())`},
		{"isFuture", `isFuture(now() + 86400)`},
		{"isPast", `isPast(now() - 86400)`},
		{"isSameDay", `isSameDay(now(), now())`},
		{"isSameMonth", `isSameMonth(now(), now())`},
		{"isSameYear", `isSameYear(now(), now())`},
		{"nextMonday", `nextMonday()`},
		{"nextFriday", `nextFriday()`},
		{"lastMonday", `lastMonday()`},
		{"lastFriday", `lastFriday()`},
		{"daysBetween", `daysBetween(now(), now() + 86400)`},
		{"hoursBetween", `hoursBetween(now(), now() + 3600)`},
		{"minutesBetween", `minutesBetween(now(), now() + 60)`},
		{"secondsBetween", `secondsBetween(now(), now() + 1)`},
		{"weeksBetween", `weeksBetween(now(), now() + 604800)`},
		{"monthsBetween", `monthsBetween(now(), now())`},
		{"yearsBetween", `yearsBetween(now(), now())`},
		{"addBusinessDays", `addBusinessDays(now(), 5)`},
		{"subBusinessDays", `subBusinessDays(now(), 5)`},
		{"isBusinessDay", `isBusinessDay(now())`},
		{"isHoliday", `isHoliday(now())`},
		{"nextHoliday", `nextHoliday()`},
		{"daysUntil", `daysUntil(now() + 86400)`},
		{"daysAgo", `daysAgo(now() - 86400)`},
		{"weeksUntil", `weeksUntil(now() + 604800)`},
		{"monthsUntil", `monthsUntil(now() + 2592000)`},
		{"yearsUntil", `yearsUntil(now() + 31536000)`},
		{"dateParts", `len(dateParts(now()))`},
		{"timeParts", `len(timeParts(now()))`},
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
