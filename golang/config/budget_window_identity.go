package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// maxBudgetWindowIdentityBytes is the longest "<policy id>/<window id>"
// identity the budget stores accept in a reservation.
const maxBudgetWindowIdentityBytes = 128

// budgetWindowIDPattern keeps an explicit window id free of "/" so the
// "<policy id>/<window id>" identity stays unique across policies.
var budgetWindowIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Key returns the window's identity within its policy: the explicit id, or
// "<duration>-<bucket>" derived from the window geometry. It never depends on
// the window's position in the list, so editing other windows cannot move a
// window onto another window's accounting.
func (window BudgetWindow) Key() string {
	if window.ID != "" {
		return window.ID
	}
	return budgetGeometryDuration(window.Duration) + "-" + budgetGeometryDuration(window.Bucket)
}

// WindowIdentity returns the budget accounting identity of one of the
// policy's windows. Redis accounting keys are selected by this value.
func (policy BudgetPolicy) WindowIdentity(window BudgetWindow) string {
	return policy.ID + "/" + window.Key()
}

// budgetGeometryDuration renders a duration as its Go duration string without
// trailing zero units ("24h", "1h30m", "5m", "1.5s"), using ASCII only.
func budgetGeometryDuration(duration Duration) string {
	text := time.Duration(duration).String()
	if strings.HasSuffix(text, "m0s") {
		text = strings.TrimSuffix(text, "0s")
	}
	if strings.HasSuffix(text, "h0m") {
		text = strings.TrimSuffix(text, "0m")
	}
	return strings.ReplaceAll(text, "µ", "u")
}

// validateBudgetWindowIdentities requires every window of a policy to resolve
// to a distinct, storable identity. Two windows with the same duration and
// bucket need explicit ids; they would otherwise share one accounting hash.
func validateBudgetWindowIdentities(policy BudgetPolicy, path string) error {
	seen := make(map[string]int, len(policy.Windows))
	for index, window := range policy.Windows {
		windowPath := fmt.Sprintf("%s.windows[%d]", path, index)
		if window.ID != "" && !budgetWindowIDPattern.MatchString(window.ID) {
			return fmt.Errorf("%s.id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}", windowPath)
		}
		key := window.Key()
		if previous, exists := seen[key]; exists {
			if window.ID == "" && policy.Windows[previous].ID == "" {
				return fmt.Errorf("%s has the same duration and bucket as windows[%d]; give each an explicit id", windowPath, previous)
			}
			return fmt.Errorf("%s duplicate window identity %q (also windows[%d])", windowPath, key, previous)
		}
		seen[key] = index
		if len(policy.ID)+1+len(key) > maxBudgetWindowIdentityBytes {
			return fmt.Errorf("%s identity \"<policy id>/%s\" must be at most %d bytes; shorten the policy id or set a shorter window id", windowPath, key, maxBudgetWindowIdentityBytes)
		}
	}
	return nil
}
