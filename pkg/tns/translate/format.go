// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package translate

import "strings"

// translateOracleDateFormat converts an Oracle date/time format model string
// to the equivalent PostgreSQL/CockroachDB format model.
//
// Oracle and PostgreSQL share most format elements, but a few differ:
//
//	Oracle   → PostgreSQL    Notes
//	RRRR       YYYY           Oracle 4-digit year with RR-pivot semantics
//	RR         YY             Oracle 2-digit year with pivot
//	MON        Mon            PG is case-sensitive
//	MONTH      Month
//	DY         Dy
//	DAY        Day
//	FF[1-9]    US             Fractional seconds → microseconds
//	AM/PM      AM/PM          Same (but Oracle also accepts A.M./P.M.)
//
// The translation is done via ordered string replacement. Elements are
// replaced longest-first to avoid partial matches (e.g. RRRR before RR).
func translateOracleDateFormat(oracleFmt string) string {
	// Work on the original case; Oracle format models are case-insensitive
	// but we need to produce PG-appropriate casing.
	result := oracleFmt

	// Ordered replacements: longest tokens first to prevent partial matches.
	replacements := []struct {
		old, new string
	}{
		// Year.
		{"RRRR", "YYYY"},
		{"RR", "YY"},
		// Month name.
		{"MONTH", "Month"},
		{"MON", "Mon"},
		// Day name.
		{"DAY", "Day"},
		{"DY", "Dy"},
		// Fractional seconds: FF1-FF9 → US (microseconds is the best PG
		// approximation; sub-microsecond precision is not supported).
		{"FF9", "US"},
		{"FF8", "US"},
		{"FF7", "US"},
		{"FF6", "US"},
		{"FF5", "US"},
		{"FF4", "US"},
		{"FF3", "MS"},
		{"FF2", "MS"},
		{"FF1", "MS"},
		{"FF", "US"},
		// AM/PM with dots.
		{"A.M.", "AM"},
		{"P.M.", "PM"},
	}

	for _, r := range replacements {
		result = caseInsensitiveReplace(result, r.old, r.new)
	}
	return result
}

// caseInsensitiveReplace replaces all occurrences of old in s with new,
// matching case-insensitively. Oracle format models are case-insensitive,
// so "rrrr", "RRRR", and "Rrrr" should all be translated.
func caseInsensitiveReplace(s, old, new string) string {
	upper := strings.ToUpper(s)
	oldUpper := strings.ToUpper(old)
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		idx := strings.Index(upper[i:], oldUpper)
		if idx == -1 {
			b.WriteString(s[i:])
			break
		}
		b.WriteString(s[i : i+idx])
		b.WriteString(new)
		i += idx + len(old)
	}
	return b.String()
}
