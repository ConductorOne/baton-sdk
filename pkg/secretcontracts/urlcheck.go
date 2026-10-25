package secretcontracts

import (
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"unicode"
)

func hasSpaceOrControl(value string) bool {
	return strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || r == '\\' }) >= 0
}

// httpURL accepts an http or https URL with a host. It is stricter than the
// receiver's URL parser in places (no backslashes, no leading or trailing
// space, ASCII-and-letters hosts), never looser.
func httpURL(value string) bool {
	if hasSpaceOrControl(value) {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	if (scheme != "http" && scheme != "https") || parsed.Opaque != "" || parsed.User != nil {
		return false
	}
	host := parsed.Hostname()
	if host == "" {
		return false
	}
	if port := parsed.Port(); port != "" {
		number, err := strconv.ParseUint(port, 10, 16)
		if err != nil || number > 65535 {
			return false
		}
	}
	if strings.HasPrefix(parsed.Host, "[") {
		_, err := netip.ParseAddr(host)
		return err == nil
	}
	for _, r := range host {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.' && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// absoluteURI accepts any URI with a scheme, such as a native app redirect.
func absoluteURI(value string) bool {
	if hasSpaceOrControl(value) {
		return false
	}
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme != ""
}

// rfc3339FullDate accepts exactly YYYY-MM-DD naming a real calendar day.
func rfc3339FullDate(value string) bool {
	if len(value) != 10 || value[4] != '-' || value[7] != '-' {
		return false
	}
	for i := 0; i < 10; i++ {
		if i == 4 || i == 7 {
			continue
		}
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	year, _ := strconv.Atoi(value[0:4])
	month, _ := strconv.Atoi(value[5:7])
	day, _ := strconv.Atoi(value[8:10])
	if month < 1 || month > 12 || day < 1 {
		return false
	}
	days := [...]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	limit := days[month-1]
	if month == 2 && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		limit = 29
	}
	return day <= limit
}

// sshUsername rejects whitespace, control characters and a leading dash.
func sshUsername(value string) bool {
	return !strings.HasPrefix(value, "-") &&
		strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}
