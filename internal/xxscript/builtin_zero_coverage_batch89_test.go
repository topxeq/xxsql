package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch89_CallBuiltinDateAndFormatDispatch(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		name string
		args []Value
	}{
		// More array/data builtins
		{"sortNatural", []Value{[]Value{"a2", "a10", "a1"}}},
		{"sortNaturalDesc", []Value{[]Value{"a2", "a10", "a1"}}},
		{"partition", []Value{[]Value{1, 2, 3, 4}, 2}},
		{"groupBySorted", []Value{[]Value{map[string]Value{"k": "a"}, map[string]Value{"k": "b"}}, "k"}},
		{"compact", []Value{[]Value{1, nil, 2}}},
		{"compactFlat", []Value{[]Value{1, nil, []Value{2, nil}}}},
		{"findIndex", []Value{[]Value{1, 2, 3}, 2}},
		{"findLastIndex", []Value{[]Value{1, 2, 1}, 1}},
		{"takeWhile", []Value{[]Value{1, 2, 3}, 2}},
		{"dropWhile", []Value{[]Value{1, 2, 3}, 2}},
		{"span", []Value{[]Value{1, 2, 3}, 2}},
		{"breakList", []Value{[]Value{1, 2, 3}, 2}},
		{"splitWhen", []Value{[]Value{1, 2, 3}, 2}},
		{"prependAll", []Value{[]Value{1, 2}, 0}},
		{"appendAll", []Value{[]Value{1, 2}, 0}},
		{"intercalate", []Value{[]Value{"a", "b"}, ","}},
		{"subsequences", []Value{[]Value{1, 2}}},
		{"permutations", []Value{[]Value{1, 2, 3}}},

		// Date/time dispatch surface
		{"dateFormat", []Value{"2006-01-02", int64(1710000000)}},
		{"dateParse", []Value{"2006-01-02", "2024-01-02"}},
		{"strftime", []Value{"%Y-%m-%d", int64(1710000000)}},
		{"strptime", []Value{"%Y-%m-%d", "2024-01-02"}},
		{"date", []Value{"2024-01-02"}},
		{"dateFromUnix", []Value{int64(1710000000)}},
		{"dateNow", nil},
		{"today", nil},
		{"year", []Value{"2024-01-02"}},
		{"month", []Value{"2024-01-02"}},
		{"day", []Value{"2024-01-02"}},
		{"hour", []Value{"2024-01-02T03:04:05Z"}},
		{"minute", []Value{"2024-01-02T03:04:05Z"}},
		{"second", []Value{"2024-01-02T03:04:05Z"}},
		{"weekday", []Value{"2024-01-02"}},
		{"yearday", []Value{"2024-01-02"}},
		{"week", []Value{"2024-01-02"}},
		{"quarter", []Value{"2024-01-02"}},
		{"dateAdd", []Value{"2024-01-02", "24h"}},
		{"dateSub", []Value{"2024-01-03", "24h"}},
		{"dateAddDays", []Value{"2024-01-02", 1}},
		{"dateAddMonths", []Value{"2024-01-02", 1}},
		{"dateAddYears", []Value{"2024-01-02", 1}},
		{"startOfDay", []Value{"2024-01-02T03:04:05Z"}},
		{"endOfDay", []Value{"2024-01-02T03:04:05Z"}},
		{"startOfWeek", []Value{"2024-01-02"}},
		{"endOfWeek", []Value{"2024-01-02"}},
		{"startOfMonth", []Value{"2024-01-02"}},
		{"endOfMonth", []Value{"2024-01-02"}},
		{"startOfYear", []Value{"2024-01-02"}},
		{"endOfYear", []Value{"2024-01-02"}},
		{"dateDiff", []Value{"2024-01-02", "2024-01-01"}},
		{"dateCompare", []Value{"2024-01-02", "2024-01-01"}},
		{"dateBefore", []Value{"2024-01-01", "2024-01-02"}},
		{"dateAfter", []Value{"2024-01-02", "2024-01-01"}},
		{"dateEqual", []Value{"2024-01-02", "2024-01-02"}},
		{"dateBetween", []Value{"2024-01-02", "2024-01-01", "2024-01-03"}},
		{"isLeapYear", []Value{2024}},
		{"daysInMonth", []Value{2024, 2}},
		{"daysInYear", []Value{2024}},
		{"parseDuration", []Value{"1h30m"}},
		{"formatDuration", []Value{int64(3600)}},
		{"age", []Value{"2000-01-01"}},
		{"isWeekend", []Value{"2024-01-06"}},
		{"isWorkday", []Value{"2024-01-08"}},

		// OS/time + misc safe calls
		{"sleep", []Value{0}},
		{"osInfo", nil},
		{"memStats", nil},
		{"goroutines", nil},
		{"userHome", nil},
		{"userCache", nil},
		{"userConfig", nil},

		// Formatting family
		{"formatNumber", []Value{12345.678, 2}},
		{"formatFloat", []Value{3.14159, 2}},
		{"formatInt", []Value{12345, 8}},
		{"formatCurrency", []Value{1234.56, "$"}},
		{"formatPercent", []Value{0.1234, 2}},
		{"formatBytes", []Value{int64(1024)}},
		{"formatDate", []Value{"2006-01-02", "2024-01-02"}},
		{"parseDate", []Value{"2006-01-02", "2024-01-02"}},
		{"indent", []Value{"a\nb", 2}},
		{"wrap", []Value{"a b c d", 3}},
		{"align", []Value{"x", 3, "left"}},
		{"alignLeft", []Value{"x", 3}},
		{"alignRight", []Value{"x", 3}},
		{"alignCenter", []Value{"x", 3}},
		{"table", []Value{[]Value{map[string]Value{"a": 1}}}},
		{"csv", []Value{[]Value{[]Value{"a", "b"}}}},
		{"csvParse", []Value{"a,b\n1,2"}},
		{"csvStringify", []Value{[]Value{[]Value{"a", "b"}}}},
		{"csvEncode", []Value{[]Value{[]Value{"a", "b"}}}},
		{"xmlParse", []Value{"<root><a>1</a></root>"}},
		{"xmlStringify", []Value{map[string]Value{"a": 1}}},
		{"xmlEncode", []Value{map[string]Value{"a": 1}}},
		{"xmlGet", []Value{"<root><a>1</a></root>", "a"}},
		{"yamlParse", []Value{"a: 1\n"}},
		{"yamlStringify", []Value{map[string]Value{"a": 1}}},
		{"yamlEncode", []Value{map[string]Value{"a": 1}}},
		{"tomlParse", []Value{"a = 1\n"}},
		{"tomlStringify", []Value{map[string]Value{"a": 1}}},
		{"tomlEncode", []Value{map[string]Value{"a": 1}}},
		{"markdownToHTML", []Value{"# hi"}},
		{"htmlToMarkdown", []Value{"<h1>hi</h1>"}},
		{"printf", []Value{"%s-%d", "x", 1}},
		{"padNumber", []Value{12, 5}},
		{"toRoman", []Value{12}},
		{"fromRoman", []Value{"XII"}},
		{"toWords", []Value{123}},
		{"toOrdinal", []Value{21}},
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
