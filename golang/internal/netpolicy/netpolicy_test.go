package netpolicy

import (
	"net/netip"
	"testing"
)

func TestBlockedAddress(t *testing.T) {
	for address, blocked := range map[string]bool{
		"8.8.8.8":              false,
		"2001:4860:4860::8888": false,
		"64:ff9b::808:808":     false, // DNS64 synthesis of public 8.8.8.8.
		"0.0.0.0":              true,
		"0.1.2.3":              true,
		"127.0.0.1":            true,
		"10.0.0.1":             true,
		"172.16.0.1":           true,
		"192.168.1.1":          true,
		"169.254.169.254":      true,
		"100.64.0.1":           true,
		"100.100.100.200":      true,
		"168.63.129.16":        true,
		"192.0.0.8":            true,
		"198.18.0.1":           true,
		"224.0.0.1":            true,
		"255.255.255.255":      true,
		"::":                   true,
		"::1":                  true,
		"::7f00:1":             true, // IPv4-compatible loopback.
		"fe80::1":              true,
		"fec0::1":              true,
		"fc00::1":              true,
		"fd00:ec2::254":        true,
		"ff02::1":              true,
		"64:ff9b::a9fe:a9fe":   true, // NAT64 of 169.254.169.254.
		"64:ff9b:1::a00:1":     true,
		"2002:a9fe:a9fe::1":    true, // 6to4 of 169.254.169.254.
		"2001:0:a9fe:a9fe::1":  true, // Teredo.
	} {
		if got := BlockedAddress(netip.MustParseAddr(address)); got != blocked {
			t.Errorf("BlockedAddress(%s) = %t, want %t", address, got, blocked)
		}
	}
	if !BlockedAddress(netip.Addr{}) {
		t.Error("the zero address is not blocked")
	}
}
