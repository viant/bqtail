package prefix

import (
	"fmt"
	"strings"
	"time"
)

type replacement struct {
	from string
	to   string
	once bool
}

func expandPattern(pattern string) string {
	for _, rule := range patternReplacements() {
		count := 1
		if !rule.once {
			count = strings.Count(pattern, rule.from)
		}
		pattern = strings.Replace(pattern, rule.from, rule.to, count)
	}
	return pattern
}

func patternReplacements() []*replacement {
	now := time.Now()
	return []*replacement{

		{
			from: "(\\d{4})",
			to:   fmt.Sprintf("%04d", now.Year()),
		},
		{
			from: "(\\d{2})",
			to:   fmt.Sprintf("%02d", now.Month()),
			once: true,
		},
		{
			from: "(\\d{2})",
			to:   fmt.Sprintf("%02d", now.Day()),
			once: true,
		},
		{
			from: "(\\d{2})",
			to:   fmt.Sprintf("%02d", now.Hour()),
			once: true,
		},
		{
			from: "(\\d{2})",
			to:   fmt.Sprintf("%02d", now.Minute()),
			once: true,
		},
		{
			from: "(\\d{2})",
			to:   fmt.Sprintf("%02d", now.Second()),
			once: true,
		},
		{
			from: "\\d+",
			to:   fmt.Sprintf("%10d", now.Unix()),
		},
		{
			from: ".+",
			to:   fmt.Sprintf("%10d", now.Unix()),
		},
		{
			from: "$",
			to:   "",
		},
		{
			from: "^",
			to:   "",
		},
	}
}
