package xxscript

import (
	"strings"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch29_OAuthAndSession(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinOAuth2URL([]Value{}).(map[string]Value); m["error"] != "need provider, clientID, redirectURL, and scopes" {
		t.Fatalf("expected oauth2url arg error, got %v", m)
	}
	if m := i.builtinOAuth2URL([]Value{"unknown", "cid", "https://cb", []Value{"read"}}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected oauth2url unknown provider error, got %v", m)
	}
	oauth := i.builtinOAuth2URL([]Value{"github", "cid123", "https://example.com/cb", []Value{"repo", "user"}}).(map[string]Value)
	oauthURL, _ := oauth["url"].(string)
	if oauth["provider"] != "github" || oauth["configID"] == nil || !strings.Contains(oauthURL, "client_id=cid123") || !strings.Contains(oauthURL, "scope=repo+user") {
		t.Fatalf("expected oauth2url success payload, got %v", oauth)
	}

	if m := i.builtinOAuth2Token([]Value{}).(map[string]Value); m["error"] != "need provider, clientID, clientSecret, code" {
		t.Fatalf("expected oauth2token arg error, got %v", m)
	}
	if m := i.builtinOAuth2Token([]Value{"unknown", "id", "sec", "code"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected oauth2token unknown provider error, got %v", m)
	}

	if m := i.builtinOAuth2Refresh([]Value{}).(map[string]Value); m["error"] != "need provider, clientID, clientSecret, refreshToken" {
		t.Fatalf("expected oauth2refresh arg error, got %v", m)
	}
	if m := i.builtinOAuth2Refresh([]Value{"facebook", "id", "sec", "rt"}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected oauth2refresh unsupported provider error, got %v", m)
	}

	if m := i.builtinSessionCreate([]Value{}).(map[string]Value); m["error"] != "need data map" {
		t.Fatalf("expected sessionCreate arg error, got %v", m)
	}
	if m := i.builtinSessionCreate([]Value{"bad"}).(map[string]Value); m["error"] != "data must be a map" {
		t.Fatalf("expected sessionCreate type error, got %v", m)
	}

	created := i.builtinSessionCreate([]Value{map[string]Value{"uid": "u1", "role": "admin"}, 1.0}).(map[string]Value)
	sid, _ := created["sessionId"].(string)
	if created["created"] != true || sid == "" || created["expiresAt"] == nil {
		t.Fatalf("expected sessionCreate success payload, got %v", created)
	}

	if m := i.builtinSessionValidate([]Value{}).(map[string]Value); m["error"] != "need session ID" {
		t.Fatalf("expected sessionValidate arg error, got %v", m)
	}
	if m := i.builtinSessionValidate([]Value{"sess_missing"}).(map[string]Value); m["valid"] != false || m["error"] != "session not found" {
		t.Fatalf("expected sessionValidate not found, got %v", m)
	}
	validated := i.builtinSessionValidate([]Value{sid}).(map[string]Value)
	if validated["valid"] != true || validated["sessionId"] != sid {
		t.Fatalf("expected sessionValidate success, got %v", validated)
	}

	// Force expiration branch via direct map update for deterministic coverage.
	sessionsMutex.Lock()
	if s, ok := sessions[sid]; ok {
		s["_expiresAt"] = time.Now().Add(-time.Minute).Format(time.RFC3339)
	}
	sessionsMutex.Unlock()
	expired := i.builtinSessionValidate([]Value{sid}).(map[string]Value)
	if expired["valid"] != false || expired["error"] != "session expired" {
		t.Fatalf("expected sessionValidate expired, got %v", expired)
	}

	if m := i.builtinSessionDestroy([]Value{}).(map[string]Value); m["error"] != "need session ID" {
		t.Fatalf("expected sessionDestroy arg error, got %v", m)
	}
	destroyed := i.builtinSessionDestroy([]Value{"sess_missing"}).(map[string]Value)
	if destroyed["destroyed"] != true || destroyed["sessionId"] != "sess_missing" {
		t.Fatalf("expected sessionDestroy success payload, got %v", destroyed)
	}
}
