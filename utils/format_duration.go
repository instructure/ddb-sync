package utils

import (
	"regexp"
	"strings"
	"time"
)

var DurationOutputFormat = regexp.MustCompile(`\d{1,2}\D`)

func FormatDuration(duration time.Duration) string {
	var str string
	switch {
	case duration > time.Hour:
		str = duration.Round(time.Minute).String()
	case duration > time.Minute:
		str = duration.Round(time.Second).String()
	case duration > time.Second:
		str = duration.Round(time.Second).String()
	}
	return strings.Join(DurationOutputFormat.FindAllString(str, 2), "")
}
