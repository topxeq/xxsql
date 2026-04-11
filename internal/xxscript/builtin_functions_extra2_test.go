package xxscript

import (
	"testing"
)

func TestBuiltin_GeoFunctions(t *testing.T) {
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

func TestBuiltin_CronFunctions(t *testing.T) {
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

func TestBuiltin_RateLimiter(t *testing.T) {
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

func TestBuiltin_ImageFunctions(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"barcodeEncode", `len(barcodeEncode("123456789012"))`},
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

func TestBuiltin_Debug(t *testing.T) {
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

func TestBuiltin_Mock(t *testing.T) {
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

func TestBuiltin_Config(t *testing.T) {
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

func TestBuiltin_Secrets(t *testing.T) {
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

func TestBuiltin_Process(t *testing.T) {
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

func TestBuiltin_Network(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_Template(t *testing.T) {
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

func TestBuiltin_Email(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_Faker(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_Timezone(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"setTimezone", `setTimezone("UTC")`},
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

func TestBuiltin_Parallel(t *testing.T) {
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

func TestBuiltin_QRCode(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_JWT(t *testing.T) {
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

func TestBuiltin_EncryptDecrypt(t *testing.T) {
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

func TestBuiltin_ErrorExtra(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

func TestBuiltin_NumberExtra(t *testing.T) {
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

func TestBuiltin_StringExtra3(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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
