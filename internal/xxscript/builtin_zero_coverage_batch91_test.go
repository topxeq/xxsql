package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch91_CallBuiltinPhase2Dispatch(t *testing.T) {
	i := NewInterpreter(NewContext())

	cases := []struct {
		name string
		args []Value
	}{
		// Network/connectivity/concurrency helpers
		{"httpDownload", []Value{"http://127.0.0.1:0", "/tmp/x.out"}},
		{"httpUpload", []Value{"http://127.0.0.1:0", "/tmp/x.in"}},
		{"ping", []Value{"127.0.0.1"}},
		{"portCheck", []Value{"127.0.0.1", 0}},
		{"portScan", []Value{"127.0.0.1", 1, 2}},
		{"qrEncode", []Value{"hello"}},
		{"qrDataURL", []Value{"hello"}},

		// Email/webhook
		{"sendEmail", []Value{map[string]Value{"to": "a@b.com", "subject": "x", "body": "y"}}},
		{"sendWebhook", []Value{"http://127.0.0.1:0", map[string]Value{"a": 1}}},

		// Additional system/network format helpers
		{"ipLookup", []Value{"127.0.0.1"}},
		{"whois", []Value{"example.com"}},

		// WebSocket dispatch
		{"wsConnect", []Value{"ws://127.0.0.1:0/ws"}},
		{"wsSend", []Value{"id", "msg"}},
		{"wsReceive", []Value{"id"}},
		{"wsClose", []Value{"id"}},
		{"wsIsConnected", []Value{"id"}},

		// Redis dispatch
		{"redisConnect", []Value{"127.0.0.1:0", "", 0}},
		{"redisGet", []Value{"k"}},
		{"redisSet", []Value{"k", "v", 1}},
		{"redisDel", []Value{"k"}},
		{"redisExists", []Value{"k"}},
		{"redisExpire", []Value{"k", 1}},
		{"redisIncr", []Value{"k"}},
		{"redisDecr", []Value{"k"}},
		{"redisLPush", []Value{"k", "v"}},
		{"redisRPush", []Value{"k", "v"}},
		{"redisLPop", []Value{"k"}},
		{"redisRPop", []Value{"k"}},
		{"redisHSet", []Value{"h", "k", "v"}},
		{"redisHGet", []Value{"h", "k"}},
		{"redisHDel", []Value{"h", "k"}},
		{"redisHGetAll", []Value{"h"}},
		{"redisKeys", []Value{"*"}},
		{"redisTTL", []Value{"k"}},

		// PDF/excel/charts
		{"pdfCreate", nil},
		{"pdfAddPage", nil},
		{"pdfAddText", []Value{"hello", 10, 10}},
		{"pdfSetFont", []Value{"Arial", "", 12}},
		{"pdfCell", []Value{10, 10, "x"}},
		{"pdfSave", []Value{"/tmp/test.pdf"}},
		{"excelCreate", nil},
		{"excelOpen", []Value{"/tmp/notfound.xlsx"}},
		{"excelSetCell", []Value{"Sheet1", "A1", "x"}},
		{"excelGetCell", []Value{"Sheet1", "A1"}},
		{"excelNewSheet", []Value{"Sheet2"}},
		{"excelSave", []Value{"/tmp/test.xlsx"}},
		{"excelClose", nil},
		{"chartLine", []Value{[]Value{1, 2, 3}, []Value{2, 3, 4}}},
		{"chartBar", []Value{[]Value{"a", "b"}, []Value{1, 2}}},
		{"chartPie", []Value{[]Value{"a", "b"}, []Value{1, 2}}},

		// Cron/geo/html/ml and related
		{"cronParse", []Value{"*/5 * * * *"}},
		{"cronNext", []Value{"*/5 * * * *"}},
		{"cronNextN", []Value{"*/5 * * * *", 3}},
		{"geoDistance", []Value{39.9, 116.3, 31.2, 121.5}},
		{"geoEncode", []Value{39.9, 116.3}},
		{"geoDecode", []Value{"wx4g0ec1"}},
		{"geoIP", []Value{"127.0.0.1"}},
		{"geoBoundingBox", []Value{39.9, 116.3, 10}},
		{"htmlParse", []Value{"<html><body><a href='x'>x</a></body></html>"}},
		{"htmlSelect", []Value{"<div id='a'>x</div>", "#a"}},
		{"htmlSelectAll", []Value{"<a>a</a><a>b</a>", "a"}},
		{"htmlAttr", []Value{"<a href='x'>a</a>", "a", "href"}},
		{"htmlText", []Value{"<p>x</p>"}},
		{"htmlLinks", []Value{"<a href='x'>x</a>"}},
		{"rssParse", []Value{"<rss><channel><title>x</title></channel></rss>"}},
		{"mlTokenize", []Value{"hello world"}},
		{"mlSentiment", []Value{"good"}},
		{"mlSimilarity", []Value{"hello", "world"}},
		{"mlKeywords", []Value{"hello world hello"}},
		{"mlNgrams", []Value{"hello world", 2}},
		{"mlWordFreq", []Value{"hello world hello"}},

		// OAuth and cloud interfaces
		{"oauth2URL", []Value{"id", "secret", "https://example.com/callback"}},
		{"oauth2Token", []Value{"code", "id", "secret"}},
		{"oauth2Refresh", []Value{"refresh", "id", "secret"}},
		{"s3Config", []Value{map[string]Value{"region": "us-east-1"}}},
		{"s3Upload", []Value{"bucket", "key", "/tmp/notfound.txt"}},
		{"s3Download", []Value{"bucket", "key", "/tmp/out.txt"}},
		{"s3List", []Value{"bucket", ""}},
		{"s3Delete", []Value{"bucket", "key"}},
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
