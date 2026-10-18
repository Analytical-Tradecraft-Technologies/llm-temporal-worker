package modelsync

import (
	"fmt"
	"sort"
	"strings"
)

// Country lists name the countries an OpenRouter upstream provider may be in.
// A provider qualifies for a list only when its headquarters and every
// datacenter country it lists are in the list, and it lists at least one
// datacenter.
const (
	CountryListUS    = "US"
	CountryListEU    = "EU"
	CountryListCore1 = "Core1"
	CountryListCore2 = "Core2"
	CountryListCore3 = "Core3"
	CountryListCore4 = "Core4"
	CountryListCore5 = "Core5"
)

// DefaultCountryList applies when no list is configured.
const DefaultCountryList = CountryListCore5

var (
	countriesEU = []string{"AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU", "IE", "IT",
		"LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"}
	countriesCore1 = []string{"AU", "CA", "GB", "NZ", "US"}
	countriesCore2 = append(append([]string(nil), countriesCore1...), "DK", "FR", "NL", "NO")
	countriesCore3 = append(append([]string(nil), countriesCore2...), "BE", "DE", "ES", "IT", "SE")
	countriesCore4 = append(append([]string(nil), countriesCore3...), "AL", "BG", "CZ", "EE", "FI", "GR", "HR", "HU", "IS",
		"LT", "LU", "LV", "ME", "MK", "PL", "PT", "RO", "SI", "SK", "TR")
	countriesCore5 = append(append([]string(nil), countriesCore4...), "JP", "KR")

	countryLists = map[string][]string{
		CountryListUS:    {"US"},
		CountryListEU:    countriesEU,
		CountryListCore1: countriesCore1,
		CountryListCore2: countriesCore2,
		CountryListCore3: countriesCore3,
		CountryListCore4: countriesCore4,
		CountryListCore5: countriesCore5,
	}
)

// CountryListNames returns the supported list names in sorted order.
func CountryListNames() []string {
	names := make([]string, 0, len(countryLists))
	for name := range countryLists {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// CountryList returns the ISO 3166-1 alpha-2 codes of a named list.
func CountryList(name string) (map[string]struct{}, error) {
	codes, ok := countryLists[name]
	if !ok {
		return nil, fmt.Errorf("country list %q is not one of %s", name, strings.Join(CountryListNames(), ", "))
	}
	set := make(map[string]struct{}, len(codes))
	for _, code := range codes {
		set[code] = struct{}{}
	}
	return set, nil
}

// normalizeCountry maps a published country code to ISO 3166-1 alpha-2,
// accepting the UK and EL spellings of GB and GR. It returns "" for a value
// that is not a two-letter code.
func normalizeCountry(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	switch code {
	case "UK":
		return "GB"
	case "EL":
		return "GR"
	}
	if len(code) != 2 || code[0] < 'A' || code[0] > 'Z' || code[1] < 'A' || code[1] > 'Z' {
		return ""
	}
	return code
}
