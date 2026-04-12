package xxscript

import "testing"

func TestBuiltin_ZeroCoverage_Batch19_GeoHelpers(t *testing.T) {
	i := NewInterpreter(NewContext())

	if m := i.builtinGeoIP([]Value{}).(map[string]Value); m["error"] != "need IP address" {
		t.Fatalf("expected geoIP arg error, got %v", m)
	}
	if m := i.builtinGeoIP([]Value{123}).(map[string]Value); m["error"] != "IP must be string" {
		t.Fatalf("expected geoIP type error, got %v", m)
	}
	if m := i.builtinGeoIP([]Value{"not-an-ip"}).(map[string]Value); m["error"] != "invalid IP address" {
		t.Fatalf("expected geoIP invalid ip error, got %v", m)
	}
	v4 := i.builtinGeoIP([]Value{"127.0.0.1"}).(map[string]Value)
	v4ver := v4["version"].(map[string]Value)
	if v4["valid"] != true || v4ver["v4"] != true || v4ver["v6"] != false {
		t.Fatalf("expected valid IPv4 metadata, got %v", v4)
	}
	v6 := i.builtinGeoIP([]Value{"::1"}).(map[string]Value)
	v6ver := v6["version"].(map[string]Value)
	if v6["valid"] != true || v6ver["v4"] != false || v6ver["v6"] != true {
		t.Fatalf("expected valid IPv6 metadata, got %v", v6)
	}

	if m := i.builtinGeoWithin([]Value{}).(map[string]Value); m["error"] == nil {
		t.Fatalf("expected geoWithin arg error, got %v", m)
	}
	if m := i.builtinGeoWithin([]Value{1, 2, "bad"}).(map[string]Value); m["error"] != "third argument must be bounding box map" {
		t.Fatalf("expected geoWithin third-arg type error, got %v", m)
	}
	bbox := map[string]Value{"minLat": 10, "maxLat": 20, "minLon": 30, "maxLon": 40}
	in := i.builtinGeoWithin([]Value{15, 35, bbox}).(map[string]Value)
	if in["valid"] != true || in["within"] != true {
		t.Fatalf("expected point inside bbox, got %v", in)
	}
	out := i.builtinGeoWithin([]Value{25, 35, bbox}).(map[string]Value)
	if out["valid"] != true || out["within"] != false {
		t.Fatalf("expected point outside bbox, got %v", out)
	}

	if m := i.builtinGeoDecode([]Value{}).(map[string]Value); m["error"] != "need geohash" {
		t.Fatalf("expected geoDecode arg error, got %v", m)
	}
	if m := i.builtinGeoDecode([]Value{123}).(map[string]Value); m["error"] != "geohash must be string" {
		t.Fatalf("expected geoDecode type error, got %v", m)
	}

	enc := i.builtinGeoEncode([]Value{37.7749, -122.4194}).(map[string]Value)
	if enc["valid"] != true || enc["geohash"] == "" {
		t.Fatalf("expected geoEncode to produce geohash, got %v", enc)
	}
	gh := enc["geohash"].(string)
	dec := i.builtinGeoDecode([]Value{gh}).(map[string]Value)
	if dec["valid"] != true || dec["geohash"] != gh {
		t.Fatalf("expected geoDecode valid roundtrip metadata, got %v", dec)
	}

	badDec := i.builtinGeoDecode([]Value{"$$$$"}).(map[string]Value)
	if badDec["valid"] != true {
		t.Fatalf("expected geoDecode to still return valid map for unknown chars, got %v", badDec)
	}

	b := i.builtinGeoBoundingBox([]Value{37.0, -122.0, 10}).(map[string]Value)
	if b["valid"] != true || b["center"] == nil {
		t.Fatalf("expected bounding box valid response, got %v", b)
	}
	if m := i.builtinGeoBoundingBox([]Value{37.0, -122.0}).(map[string]Value); m["error"] != "need lat, lon, radius_km" {
		t.Fatalf("expected geoBoundingBox arg error, got %v", m)
	}
}
