package xxscript

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuiltin_ZeroCoverage_Batch22_ConfigAndNetwork(t *testing.T) {
	i := NewInterpreter(NewContext())

	tmpDir := t.TempDir()

	if m := i.builtinLoadConfig([]Value{}).(map[string]Value); m["error"] != "need filepath" {
		t.Fatalf("expected loadConfig arg error, got %v", m)
	}
	if m := i.builtinLoadConfig([]Value{123}).(map[string]Value); m["error"] != "filepath must be string" {
		t.Fatalf("expected loadConfig filepath type error, got %v", m)
	}
	if m := i.builtinLoadConfig([]Value{filepath.Join(tmpDir, "missing.json")}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected loadConfig read error, got %v", m)
	}

	jsonPath := filepath.Join(tmpDir, "cfg.json")
	if err := os.WriteFile(jsonPath, []byte(`{"name":"x","n":1}`), 0644); err != nil {
		t.Fatalf("write json fixture: %v", err)
	}
	jsonCfg := i.builtinLoadConfig([]Value{jsonPath}).(map[string]Value)
	if jsonCfg["name"] != "x" {
		t.Fatalf("expected json config name=x, got %v", jsonCfg)
	}

	yamlPath := filepath.Join(tmpDir, "cfg.yaml")
	if err := os.WriteFile(yamlPath, []byte("name: y\nn: 2\n"), 0644); err != nil {
		t.Fatalf("write yaml fixture: %v", err)
	}
	yamlCfg := i.builtinLoadConfig([]Value{yamlPath}).(map[string]Value)
	if yamlCfg["name"] != "y" {
		t.Fatalf("expected yaml config name=y, got %v", yamlCfg)
	}

	tomlPath := filepath.Join(tmpDir, "cfg.toml")
	if err := os.WriteFile(tomlPath, []byte("name = \"t\"\nn = 3\n"), 0644); err != nil {
		t.Fatalf("write toml fixture: %v", err)
	}
	tomlCfg := i.builtinLoadConfig([]Value{tomlPath}).(map[string]Value)
	if tomlCfg["name"] != "t" {
		t.Fatalf("expected toml config name=t, got %v", tomlCfg)
	}

	unknownJSONPath := filepath.Join(tmpDir, "cfg.unknown")
	if err := os.WriteFile(unknownJSONPath, []byte(`{"k":"v"}`), 0644); err != nil {
		t.Fatalf("write unknown+json fixture: %v", err)
	}
	unknownOK := i.builtinLoadConfig([]Value{unknownJSONPath}).(map[string]Value)
	if unknownOK["k"] != "v" {
		t.Fatalf("expected unknown extension to fallback to json, got %v", unknownOK)
	}

	unknownBadPath := filepath.Join(tmpDir, "cfg.bad")
	if err := os.WriteFile(unknownBadPath, []byte("not-json"), 0644); err != nil {
		t.Fatalf("write bad unknown fixture: %v", err)
	}
	if m := i.builtinLoadConfig([]Value{unknownBadPath}).(map[string]Value); m["error"] != "unknown format" {
		t.Fatalf("expected loadConfig unknown format error, got %v", m)
	}

	if m := i.builtinSaveConfig([]Value{}).(map[string]Value); m["success"] != false {
		t.Fatalf("expected saveConfig arg error, got %v", m)
	}
	if m := i.builtinSaveConfig([]Value{"bad", filepath.Join(tmpDir, "x.json")}).(map[string]Value); m["error"] != "config must be object" {
		t.Fatalf("expected saveConfig config type error, got %v", m)
	}
	if m := i.builtinSaveConfig([]Value{map[string]Value{"a": 1}, 123}).(map[string]Value); m["error"] != "filepath must be string" {
		t.Fatalf("expected saveConfig path type error, got %v", m)
	}

	cfg := map[string]Value{"name": "save", "n": 9}
	paths := []string{
		filepath.Join(tmpDir, "out.json"),
		filepath.Join(tmpDir, "out.yaml"),
		filepath.Join(tmpDir, "out.toml"),
		filepath.Join(tmpDir, "out.unknown"),
	}
	for _, p := range paths {
		saved := i.builtinSaveConfig([]Value{cfg, p}).(map[string]Value)
		if saved["success"] != true {
			t.Fatalf("expected saveConfig success for %s, got %v", p, saved)
		}
		loaded := i.builtinLoadConfig([]Value{p}).(map[string]Value)
		if loaded["name"] != "save" {
			t.Fatalf("expected saved+loaded config name=save for %s, got %v", p, loaded)
		}
	}

	if m := i.builtinIPLookup([]Value{}).(map[string]Value); m["error"] != "need IP or hostname" {
		t.Fatalf("expected ipLookup arg error, got %v", m)
	}
	if m := i.builtinIPLookup([]Value{123}).(map[string]Value); m["error"] != "input must be string" {
		t.Fatalf("expected ipLookup type error, got %v", m)
	}

	ipRes := i.builtinIPLookup([]Value{"127.0.0.1"}).(map[string]Value)
	if ipRes["valid"] != true || ipRes["ip"] != "127.0.0.1" {
		t.Fatalf("expected ipLookup ip branch valid response, got %v", ipRes)
	}

	hostRes := i.builtinIPLookup([]Value{"localhost"}).(map[string]Value)
	if hostRes["ips"] == nil {
		t.Fatalf("expected ipLookup host branch with ips, got %v", hostRes)
	}

	hostErr := i.builtinIPLookup([]Value{"definitely-not-a-real-hostname.xxsql"}).(map[string]Value)
	if hostErr["error"] == nil {
		t.Fatalf("expected ipLookup host error branch, got %v", hostErr)
	}

	if m := i.builtinWhois([]Value{}).(map[string]Value); m["error"] != "need domain" {
		t.Fatalf("expected whois arg error, got %v", m)
	}
	if m := i.builtinWhois([]Value{123}).(map[string]Value); m["error"] != "domain must be string" {
		t.Fatalf("expected whois type error, got %v", m)
	}
	w := i.builtinWhois([]Value{"example.com"}).(map[string]Value)
	if w["domain"] != "example.com" || w["message"] == nil {
		t.Fatalf("expected whois placeholder response, got %v", w)
	}
}

func TestBuiltin_ZeroCoverage_Batch22_CronFieldHelpers(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	next, err := parseCronAndFindNext("*/5 * * * *", start)
	if err != nil || !next.After(start) {
		t.Fatalf("expected valid cron next time, got next=%v err=%v", next, err)
	}
	if _, err := parseCronAndFindNext("bad expr", start); err == nil {
		t.Fatalf("expected parseCronAndFindNext invalid expression error")
	}

	if !matchField(10, "*", 0, 59) {
		t.Fatalf("expected wildcard match")
	}
	if !matchField(2, "1,2,3", 0, 59) || matchField(4, "1,2,3", 0, 59) {
		t.Fatalf("expected comma list match behavior")
	}
	if !matchField(5, "3-7", 0, 59) || matchField(8, "3-7", 0, 59) {
		t.Fatalf("expected range match behavior")
	}
	if !matchField(15, "*/5", 0, 59) || matchField(16, "*/5", 0, 59) {
		t.Fatalf("expected step match behavior")
	}
	if matchField(5, "*/0", 0, 59) {
		t.Fatalf("expected invalid step not to match")
	}
	if !matchField(7, "7", 0, 59) || matchField(8, "7", 0, 59) {
		t.Fatalf("expected exact match behavior")
	}
}
