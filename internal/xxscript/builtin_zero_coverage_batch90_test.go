package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch90_CallBuiltinExtendedDispatch(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		name string
		args []Value
	}{
		// HTTP + DNS + IP
		{"httpGet", []Value{"http://127.0.0.1:0"}},
		{"httpPost", []Value{"http://127.0.0.1:0", map[string]Value{"a": 1}}},
		{"httpPut", []Value{"http://127.0.0.1:0", map[string]Value{"a": 1}}},
		{"httpDelete", []Value{"http://127.0.0.1:0"}},
		{"httpRequest", []Value{"GET", "http://127.0.0.1:0", nil}},
		{"httpHead", []Value{"http://127.0.0.1:0"}},
		{"httpPatch", []Value{"http://127.0.0.1:0", map[string]Value{"a": 1}}},
		{"httpOptions", []Value{"http://127.0.0.1:0"}},
		{"dnsLookup", []Value{"localhost"}},
		{"dnsLookupHost", []Value{"localhost"}},
		{"dnsLookupAddr", []Value{"127.0.0.1"}},
		{"ipParse", []Value{"127.0.0.1"}},
		{"isIPv4", []Value{"127.0.0.1"}},
		{"isIPv6", []Value{"::1"}},

		// Environment/cache/security helpers
		{"env", []Value{"PATH"}},
		{"envSet", []Value{"XXSCRIPT_TMP", "1"}},
		{"envUnset", []Value{"XXSCRIPT_TMP"}},
		{"envList", nil},
		{"cacheSet", []Value{"k", "v", 1}},
		{"cacheGet", []Value{"k"}},
		{"cacheHas", []Value{"k"}},
		{"cacheKeys", nil},
		{"cacheDel", []Value{"k"}},
		{"cacheClear", nil},
		{"jwtSign", []Value{map[string]Value{"a": 1}, "k"}},
		{"jwtVerify", []Value{"bad.token", "k"}},
		{"hashPassword", []Value{"pass"}},
		{"verifyPassword", []Value{"pass", "badhash"}},
		{"generateSecret", []Value{16}},
		{"encryptAES", []Value{"abc", "short"}},
		{"decryptAES", []Value{"abc", "short"}},

		// Validation/template/content
		{"validate", []Value{map[string]Value{"email": "a@b.com"}, map[string]Value{"email": "required|email"}}},
		{"sanitize", []Value{"<b>x</b>", "html"}},
		{"normalizeEmail", []Value{" A@B.COM "}},
		{"normalizePhone", []Value{"+1 (555) 123-4567"}},
		{"validatePassword", []Value{"Abcd1234!"}},
		{"isStrongPassword", []Value{"Abcd1234!"}},
		{"renderTemplate", []Value{"Hi {{name}}", map[string]Value{"name": "Bob"}}},
		{"minify", []Value{"<html>  <body>x</body> </html>", "html"}},
		{"beautify", []Value{"{\"a\":1}", "json"}},
		{"randomAvatar", []Value{"seed"}},
		{"generateLorem", []Value{10}},
		{"faker", []Value{"name"}},

		// Timezone/image/data-structures/debug/config/system
		{"getTimezone", nil},
		{"setTimezone", []Value{"UTC"}},
		{"listTimezones", nil},
		{"imageInfo", []Value{"/tmp/notfound.png"}},
		{"imageToBase64", []Value{"/tmp/notfound.png"}},
		{"base64ToImage", []Value{"", "/tmp/out.png"}},
		{"barcodeEncode", []Value{"12345"}},
		{"newStack", nil},
		{"newQueue", nil},
		{"newSet", nil},
		{"debug", []Value{"x"}},
		{"mock", []Value{"now", 1}},
		{"loadConfig", []Value{"/tmp/notfound.json"}},
		{"saveConfig", []Value{"/tmp/xxscript_config.json", map[string]Value{"a": 1}}},
		{"getSecret", []Value{"X"}},
		{"getMemory", nil},
		{"getCPU", nil},

		// Additional aliases and random helpers
		{"random", nil},
		{"randomInt", []Value{1, 10}},
		{"randomFloat", []Value{0, 1}},
		{"shuffle", []Value{[]Value{1, 2, 3}}},
		{"sample", []Value{[]Value{1, 2, 3}, 2}},
		{"randomPassword", []Value{12}},
		{"randomColor", nil},
		{"randomName", nil},
		{"randomToken", []Value{16}},
		{"uuidv4", nil},
		{"randomUUID", nil},

		// Scripting/metrics/queue/rules/cloud/other phase2 surfaces
		{"stateMachine", []Value{"idle", []Value{}}},
		{"stateTransition", []Value{map[string]Value{}, "x"}},
		{"rateLimiter", []Value{"k", 1, 60}},
		{"rateLimitCheck", []Value{"k"}},
		{"exprEval", []Value{"1+2"}},
		{"gitStatus", nil},
		{"gitLog", nil},
		{"gitBranch", nil},
		{"metricsCounter", []Value{"hits"}},
		{"metricsGauge", []Value{"temp", 1}},
		{"metricsGet", []Value{"hits"}},
		{"sessionCreate", []Value{"user1", 60}},
		{"sessionValidate", []Value{"token"}},
		{"sessionDestroy", []Value{"token"}},
		{"queueCreate", []Value{"q"}},
		{"queuePush", []Value{"q", "x"}},
		{"queuePop", []Value{"q"}},
		{"queuePeek", []Value{"q"}},
		{"queueSize", []Value{"q"}},
		{"topicCreate", []Value{"t"}},
		{"topicPublish", []Value{"t", "x"}},
		{"topicSubscribe", []Value{"t"}},
		{"ruleCreate", []Value{"r"}},
		{"ruleAddCondition", []Value{"r", "x > 1"}},
		{"ruleEvaluate", []Value{"r", map[string]Value{"x": 2}}},
		{"ruleChain", []Value{[]Value{"r1", "r2"}, map[string]Value{"x": 2}}},
		{"decisionTable", []Value{[]Value{}, map[string]Value{}}},
		{"openapiSpec", []Value{map[string]Value{"title": "x"}}},
		{"openapiGenerate", []Value{map[string]Value{}}},
		{"apiMock", []Value{map[string]Value{"path": "/x"}}},
		{"apiTest", []Value{map[string]Value{"url": "http://127.0.0.1:0"}}},
		{"nlpStem", []Value{"running"}},
		{"nlpPosTag", []Value{"hello world"}},
		{"translateText", []Value{"hello", "en", "zh"}},
		{"ethAddress", []Value{"seed"}},
		{"ethSign", []Value{"msg", "key"}},
		{"ethVerify", []Value{"msg", "sig", "addr"}},
		{"btcAddress", []Value{"seed"}},
		{"hashKeccak", []Value{"abc"}},
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
