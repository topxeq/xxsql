package xxscript

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltin_ZeroCoverage_Batch31_WSRedisS3AndAPIHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinWSConnect([]Value{}).(map[string]Value); m["error"] != "need URL" {
		t.Fatalf("expected wsConnect arg error, got %v", m)
	}
	if m := i.builtinWSConnect([]Value{123}).(map[string]Value); m["error"] != "URL must be string" {
		t.Fatalf("expected wsConnect type error, got %v", m)
	}
	if m := i.builtinWSConnect([]Value{"://bad"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected wsConnect dial error, got %v", m)
	}

	if m := i.builtinWSSend([]Value{}).(map[string]Value); m["error"] != "need connection ID and message" {
		t.Fatalf("expected wsSend arg error, got %v", m)
	}
	if m := i.builtinWSSend([]Value{123, "x"}).(map[string]Value); m["error"] != "connection ID must be string" {
		t.Fatalf("expected wsSend id type error, got %v", m)
	}
	if m := i.builtinWSSend([]Value{"id", 123}).(map[string]Value); m["error"] != "message must be string" {
		t.Fatalf("expected wsSend message type error, got %v", m)
	}
	if m := i.builtinWSSend([]Value{"missing", "x"}).(map[string]Value); m["error"] != "connection not found" {
		t.Fatalf("expected wsSend missing conn, got %v", m)
	}

	if m := i.builtinWSReceive([]Value{}).(map[string]Value); m["error"] != "need connection ID" {
		t.Fatalf("expected wsReceive arg error, got %v", m)
	}
	if m := i.builtinWSReceive([]Value{123}).(map[string]Value); m["error"] != "connection ID must be string" {
		t.Fatalf("expected wsReceive type error, got %v", m)
	}
	if m := i.builtinWSReceive([]Value{"missing", 1}).(map[string]Value); m["error"] != "connection not found" {
		t.Fatalf("expected wsReceive missing conn, got %v", m)
	}

	if m := i.builtinWSClose([]Value{}).(map[string]Value); m["error"] != "need connection ID" {
		t.Fatalf("expected wsClose arg error, got %v", m)
	}
	if m := i.builtinWSClose([]Value{123}).(map[string]Value); m["error"] != "connection ID must be string" {
		t.Fatalf("expected wsClose type error, got %v", m)
	}
	if m := i.builtinWSClose([]Value{"missing"}).(map[string]Value); m["error"] != "connection not found" {
		t.Fatalf("expected wsClose missing conn, got %v", m)
	}

	if m := i.builtinWSIsConnected([]Value{}).(map[string]Value); m["error"] != "need connection ID" {
		t.Fatalf("expected wsIsConnected arg error, got %v", m)
	}
	if m := i.builtinWSIsConnected([]Value{123}).(map[string]Value); m["error"] != "connection ID must be string" {
		t.Fatalf("expected wsIsConnected type error, got %v", m)
	}
	if m := i.builtinWSIsConnected([]Value{"missing"}).(map[string]Value); m["connected"] != false {
		t.Fatalf("expected wsIsConnected false for missing conn, got %v", m)
	}

	if m := i.builtinRedisConnect([]Value{}).(map[string]Value); m["error"] != "need connection string or address" {
		t.Fatalf("expected redisConnect arg error, got %v", m)
	}
	if m := i.builtinRedisConnect([]Value{123}).(map[string]Value); m["error"] != "address must be string" {
		t.Fatalf("expected redisConnect type error, got %v", m)
	}
	if m := i.builtinRedisConnect([]Value{"redis://%%%"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected redisConnect parseURL error, got %v", m)
	}

	if c, ok := getRedisClient("missing"); ok || c != nil {
		t.Fatalf("expected getRedisClient missing=false,nil")
	}

	if m := i.builtinRedisGet([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisGet arg error, got %v", m)
	}
	if m := i.builtinRedisGet([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisGet missing client, got %v", m)
	}
	if m := i.builtinRedisSet([]Value{}).(map[string]Value); m["error"] != "need client ID, key, and value" {
		t.Fatalf("expected redisSet arg error, got %v", m)
	}
	if m := i.builtinRedisSet([]Value{"missing", "k", "v", 1}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisSet missing client, got %v", m)
	}
	if m := i.builtinRedisDel([]Value{}).(map[string]Value); m["error"] != "need client ID and key(s)" {
		t.Fatalf("expected redisDel arg error, got %v", m)
	}
	if m := i.builtinRedisDel([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisDel missing client, got %v", m)
	}
	if m := i.builtinRedisExists([]Value{}).(map[string]Value); m["error"] != "need client ID and key(s)" {
		t.Fatalf("expected redisExists arg error, got %v", m)
	}
	if m := i.builtinRedisExists([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisExists missing client, got %v", m)
	}
	if m := i.builtinRedisExpire([]Value{}).(map[string]Value); m["error"] != "need client ID, key, and seconds" {
		t.Fatalf("expected redisExpire arg error, got %v", m)
	}
	if m := i.builtinRedisExpire([]Value{"missing", "k", 1}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisExpire missing client, got %v", m)
	}
	if m := i.builtinRedisIncr([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisIncr arg error, got %v", m)
	}
	if m := i.builtinRedisIncr([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisIncr missing client, got %v", m)
	}
	if m := i.builtinRedisDecr([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisDecr arg error, got %v", m)
	}
	if m := i.builtinRedisDecr([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisDecr missing client, got %v", m)
	}
	if m := i.builtinRedisLPush([]Value{}).(map[string]Value); m["error"] != "need client ID, key, and value(s)" {
		t.Fatalf("expected redisLPush arg error, got %v", m)
	}
	if m := i.builtinRedisLPush([]Value{"missing", "k", "v"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisLPush missing client, got %v", m)
	}
	if m := i.builtinRedisRPush([]Value{}).(map[string]Value); m["error"] != "need client ID, key, and value(s)" {
		t.Fatalf("expected redisRPush arg error, got %v", m)
	}
	if m := i.builtinRedisRPush([]Value{"missing", "k", "v"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisRPush missing client, got %v", m)
	}
	if m := i.builtinRedisLPop([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisLPop arg error, got %v", m)
	}
	if m := i.builtinRedisLPop([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisLPop missing client, got %v", m)
	}
	if m := i.builtinRedisRPop([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisRPop arg error, got %v", m)
	}
	if m := i.builtinRedisRPop([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisRPop missing client, got %v", m)
	}
	if m := i.builtinRedisHSet([]Value{}).(map[string]Value); m["error"] != "need client ID, key, field, and value" {
		t.Fatalf("expected redisHSet arg error, got %v", m)
	}
	if m := i.builtinRedisHSet([]Value{"missing", "k", "f", "v"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisHSet missing client, got %v", m)
	}
	if m := i.builtinRedisHGet([]Value{}).(map[string]Value); m["error"] != "need client ID, key, and field" {
		t.Fatalf("expected redisHGet arg error, got %v", m)
	}
	if m := i.builtinRedisHGet([]Value{"missing", "k", "f"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisHGet missing client, got %v", m)
	}
	if m := i.builtinRedisHDel([]Value{}).(map[string]Value); m["error"] != "need client ID, key, and field(s)" {
		t.Fatalf("expected redisHDel arg error, got %v", m)
	}
	if m := i.builtinRedisHDel([]Value{"missing", "k", "f"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisHDel missing client, got %v", m)
	}
	if m := i.builtinRedisHGetAll([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisHGetAll arg error, got %v", m)
	}
	if m := i.builtinRedisHGetAll([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisHGetAll missing client, got %v", m)
	}
	if m := i.builtinRedisKeys([]Value{}).(map[string]Value); m["error"] != "need client ID and pattern" {
		t.Fatalf("expected redisKeys arg error, got %v", m)
	}
	if m := i.builtinRedisKeys([]Value{"missing", "*"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisKeys missing client, got %v", m)
	}
	if m := i.builtinRedisTTL([]Value{}).(map[string]Value); m["error"] != "need client ID and key" {
		t.Fatalf("expected redisTTL arg error, got %v", m)
	}
	if m := i.builtinRedisTTL([]Value{"missing", "k"}).(map[string]Value); m["error"] != "Redis client not found" {
		t.Fatalf("expected redisTTL missing client, got %v", m)
	}

	if m := i.builtinS3Config([]Value{}).(map[string]Value); m["error"] != "need name, endpoint, accessKey, secretKey" {
		t.Fatalf("expected s3Config arg error, got %v", m)
	}
	s3cfg := i.builtinS3Config([]Value{"bad", "://bad", "ak", "sk", "bkt"}).(map[string]Value)
	if s3cfg["configured"] != true {
		t.Fatalf("expected s3Config success, got %v", s3cfg)
	}
	if m := i.builtinS3Upload([]Value{}).(map[string]Value); m["error"] != "need config name, key, and data" {
		t.Fatalf("expected s3Upload arg error, got %v", m)
	}
	if m := i.builtinS3Upload([]Value{"missing", "k", "v"}).(map[string]Value); m["error"] != "S3 config not found" {
		t.Fatalf("expected s3Upload missing config, got %v", m)
	}
	if m := i.builtinS3Upload([]Value{"bad", "k", "v"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected s3Upload bad endpoint error, got %v", m)
	}
	if m := i.builtinS3Download([]Value{}).(map[string]Value); m["error"] != "need config name and key" {
		t.Fatalf("expected s3Download arg error, got %v", m)
	}
	if m := i.builtinS3Download([]Value{"missing", "k"}).(map[string]Value); m["error"] != "S3 config not found" {
		t.Fatalf("expected s3Download missing config, got %v", m)
	}
	if m := i.builtinS3Download([]Value{"bad", "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected s3Download bad endpoint error, got %v", m)
	}
	if m := i.builtinS3List([]Value{}).(map[string]Value); m["error"] != "need config name" {
		t.Fatalf("expected s3List arg error, got %v", m)
	}
	if m := i.builtinS3List([]Value{"missing"}).(map[string]Value); m["error"] != "S3 config not found" {
		t.Fatalf("expected s3List missing config, got %v", m)
	}
	if m := i.builtinS3List([]Value{"bad", "p"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected s3List bad endpoint error, got %v", m)
	}
	if m := i.builtinS3Delete([]Value{}).(map[string]Value); m["error"] != "need config name and key" {
		t.Fatalf("expected s3Delete arg error, got %v", m)
	}
	if m := i.builtinS3Delete([]Value{"missing", "k"}).(map[string]Value); m["error"] != "S3 config not found" {
		t.Fatalf("expected s3Delete missing config, got %v", m)
	}
	if m := i.builtinS3Delete([]Value{"bad", "k"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected s3Delete bad endpoint error, got %v", m)
	}

	if m := i.builtinOpenAPISpec([]Value{}).(map[string]Value); m["error"] != "need spec map or path" {
		t.Fatalf("expected openapiSpec arg error, got %v", m)
	}
	if m := i.builtinOpenAPISpec([]Value{123}).(map[string]Value); m["error"] != "spec must be a map or file path" {
		t.Fatalf("expected openapiSpec type error, got %v", m)
	}
	mapSpec := i.builtinOpenAPISpec([]Value{map[string]Value{"openapi": "3.0.0"}}).(map[string]Value)
	if mapSpec["valid"] != true || mapSpec["version"] != "3.0.0" {
		t.Fatalf("expected openapiSpec map success, got %v", mapSpec)
	}
	if m := i.builtinOpenAPISpec([]Value{filepath.Join(t.TempDir(), "missing.json")}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected openapiSpec file read error, got %v", m)
	}
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte("{"), 0644); err != nil {
		t.Fatalf("write bad openapi file failed: %v", err)
	}
	if m := i.builtinOpenAPISpec([]Value{badPath}).(map[string]Value); !strings.Contains(m["error"].(string), "invalid JSON") {
		t.Fatalf("expected openapiSpec invalid JSON error, got %v", m)
	}
	goodPath := filepath.Join(t.TempDir(), "good.json")
	if err := os.WriteFile(goodPath, []byte(`{"openapi":"3.0.1","info":{"title":"x"}}`), 0644); err != nil {
		t.Fatalf("write good openapi file failed: %v", err)
	}
	fileSpec := i.builtinOpenAPISpec([]Value{goodPath}).(map[string]Value)
	if fileSpec["valid"] != true || fileSpec["version"] != "3.0.1" {
		t.Fatalf("expected openapiSpec file success, got %v", fileSpec)
	}

	if m := i.builtinOpenAPIGenerate([]Value{}).(map[string]Value); m["error"] != "need endpoints array" {
		t.Fatalf("expected openapiGenerate arg error, got %v", m)
	}
	gen := i.builtinOpenAPIGenerate([]Value{[]Value{
		map[string]Value{"path": "/health", "method": "GET", "description": "ok"},
		map[string]Value{"path": "", "method": "POST", "description": "skip"},
	}, "Svc", "2.0.0"}).(map[string]Value)
	if gen["generated"] != true || gen["endpoints"] != 2 || !strings.Contains(gen["spec"].(string), "\"/health\"") {
		t.Fatalf("expected openapiGenerate success payload, got %v", gen)
	}

	if m := i.builtinAPIMock([]Value{}).(map[string]Value); m["error"] != "need endpoint and response" {
		t.Fatalf("expected apiMock arg error, got %v", m)
	}
	mock := i.builtinAPIMock([]Value{"/x", map[string]Value{"ok": true}, 201}).(map[string]Value)
	if mock["created"] != true || mock["statusCode"] != 201 {
		t.Fatalf("expected apiMock success payload, got %v", mock)
	}

	if m := i.builtinAPITest([]Value{}).(map[string]Value); m["error"] != "need method and URL" {
		t.Fatalf("expected apiTest arg error, got %v", m)
	}
	if m := i.builtinAPITest([]Value{"GET", "://bad"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected apiTest request build error, got %v", m)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %s", r.Method)
		}
		if r.Header.Get("X-Test") != "1" {
			t.Fatalf("expected X-Test header")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	api := i.builtinAPITest([]Value{"POST", ts.URL, "payload", map[string]Value{"X-Test": "1"}}).(map[string]Value)
	if api["success"] != true || api["statusCode"] != 201 || api["body"] != "ok" {
		t.Fatalf("expected apiTest success payload, got %v", api)
	}
}
