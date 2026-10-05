// Package netpolicy holds the destination-address policy shared by the
// provider egress transport and the validation of caller-supplied media URLs,
// so the two cannot drift apart.
package netpolicy

import "net/netip"

var metadataAddresses = map[netip.Addr]struct{}{
	netip.MustParseAddr("100.100.100.200"): {}, // Alibaba metadata service.
	netip.MustParseAddr("168.63.129.16"):   {}, // Azure platform metadata service.
	netip.MustParseAddr("169.254.169.254"): {}, // AWS, GCP, and EC2-compatible metadata.
	netip.MustParseAddr("fd00:ec2::254"):   {}, // AWS IMDS IPv6 endpoint.
}

var blockedIPv4Prefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // RFC 6598 carrier-grade NAT.
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments (RFC 6890).
	netip.MustParsePrefix("198.18.0.0/15"), // Benchmarking network.
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

var blockedIPv6Prefixes = []netip.Prefix{
	// Deprecated IPv6 site-local addresses are neither global-unicast nor
	// private according to netip, but must never be reachable.
	netip.MustParsePrefix("fec0::/10"),
	// IPv6 forms that embed an IPv4 destination. A NAT64, 6to4 or Teredo
	// gateway translates them to that IPv4 address, which would bypass the
	// IPv4 private, loopback and metadata checks above.
	// The NAT64 well-known prefix is decoded and checked separately below so
	// DNS64 deployments can still reach public IPv4-only providers.
	netip.MustParsePrefix("::/96"),          // IPv4-compatible (deprecated).
	netip.MustParsePrefix("64:ff9b:1::/48"), // NAT64 local-use prefix (RFC 8215).
	netip.MustParsePrefix("2002::/16"),      // 6to4 (RFC 3056).
	netip.MustParsePrefix("2001::/32"),      // Teredo (RFC 4380).
}

var nat64WellKnownPrefix = netip.MustParsePrefix("64:ff9b::/96")

// BlockedAddress reports whether address is not a public unicast destination:
// unspecified, loopback, private, link-local, multicast, reserved, a cloud
// metadata endpoint, or an IPv6 form that a gateway translates to such an
// IPv4 address. Callers pass an unmapped, zone-free address.
func BlockedAddress(address netip.Addr) bool {
	if !address.IsValid() || address.IsUnspecified() || address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsMulticast() {
		return true
	}
	if _, metadata := metadataAddresses[address]; metadata {
		return true
	}
	if address.Is4() {
		for _, prefix := range blockedIPv4Prefixes {
			if prefix.Contains(address) {
				return true
			}
		}
	}
	if address.Is6() && nat64WellKnownPrefix.Contains(address) {
		// RFC 6052 /96: the low 32 bits are the IPv4 destination the gateway
		// translates to, so apply the IPv4 policy to it.
		raw := address.As16()
		return BlockedAddress(netip.AddrFrom4([4]byte(raw[12:])))
	}
	if address.Is6() {
		for _, prefix := range blockedIPv6Prefixes {
			if prefix.Contains(address) {
				return true
			}
		}
	}
	return false
}
