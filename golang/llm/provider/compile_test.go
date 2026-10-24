package provider

import (
	"errors"
	"testing"
)

func TestRequireFeaturesRunsExtraAfterEachResolve(t *testing.T) {
	set := CapabilitySet{Features: map[Feature]Capability{
		FeatureText:             {State: CapabilityNative},
		FeatureStructuredOutput: {State: CapabilityEmulated},
	}}
	var seen []Feature
	rejected := errors.New("rejected")
	err := RequireFeatures(set, []Feature{FeatureText, FeatureStructuredOutput, FeatureImage}, true, func(feature Feature, capability Capability) error {
		seen = append(seen, feature)
		if capability.State == CapabilityEmulated {
			return rejected
		}
		return nil
	})
	if !errors.Is(err, rejected) {
		t.Fatalf("RequireFeatures = %v, want the extra check's error before the unsupported image", err)
	}
	if len(seen) != 2 {
		t.Fatalf("extra ran for %v", seen)
	}
	err = RequireFeatures(set, []Feature{FeatureImage}, false, nil)
	var mapped *Error
	if !errors.As(err, &mapped) || mapped.Code != CodeUnsupportedCapability || mapped.SafeMessage != `image: capability "image" is unknown` {
		t.Fatalf("unknown feature error = %#v", err)
	}
}

func TestCompileCapabilitiesLoadsOnlyWhenEmpty(t *testing.T) {
	loads := 0
	load := func() (CapabilitySet, error) {
		loads++
		return CapabilitySet{Features: map[Feature]Capability{FeatureText: {State: CapabilityNative}}}, nil
	}
	set, err := CompileCapabilities(CapabilitySet{}, load, "v1")
	if err != nil || loads != 1 || set.Version != "v1" {
		t.Fatalf("empty input: set=%+v loads=%d err=%v", set, loads, err)
	}
	set, err = CompileCapabilities(CapabilitySet{Version: "caller"}, load, "v1")
	if err != nil || loads != 1 || set.Version != "caller" {
		t.Fatalf("caller set: set=%+v loads=%d err=%v", set, loads, err)
	}
	_, err = CompileCapabilities(CapabilitySet{}, func() (CapabilitySet, error) { return CapabilitySet{}, errors.New("boom") }, "v1")
	var mapped *Error
	if !errors.As(err, &mapped) || mapped.Code != CodeInvalidArgument || mapped.Phase != PhaseCompile {
		t.Fatalf("load error = %#v", err)
	}
}

func TestCapabilitySetCloneIsIndependent(t *testing.T) {
	original := CapabilitySet{Version: "v", Features: map[Feature]Capability{FeatureText: {State: CapabilityNative}}}
	clone := original.Clone()
	clone.Features[FeatureImage] = Capability{State: CapabilityNative}
	if _, ok := original.Features[FeatureImage]; ok || clone.Version != "v" {
		t.Fatalf("clone shares features or lost version: %+v", original)
	}
	if empty := (CapabilitySet{}).Clone(); empty.Features == nil {
		t.Fatal("clone of an empty set has a nil feature map")
	}
}
