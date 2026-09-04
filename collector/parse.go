package collector

import (
	"regexp"
	"strconv"
	"strings"
)

// info represents a parsed Redis INFO response separated into
// normal, keyspace and errorstat fields.
type info struct {
	normal    map[string]string
	keyspace  []keyspace
	errorstat []errorstat
}

// keyspace is a parsed Redis INFO Keyspace row:
// db0:keys=value1,expires=value2,avg_ttl=value3,subexpiry=value4.
type keyspace struct {
	id        string
	keys      float64
	expires   float64
	avgTTL    float64
	subexpiry float64
}

// errorstat is a parsed Redis INFO Errorstat row:
// errorstat_<CODE>
type errorstat struct {
	code  string
	value float64
}

// errorstatPrefix is a prefix for Errorstats metrics.
const errorstatPrefix string = "errorstat_"

// keyspaceRegexp is a regular expression that matches all Keyspace metrics.
var keyspaceRegexp = regexp.MustCompile(`^db(\d+)$`)

// parseInfo parses redis INFO output into a struct. It's a best-effort parsing.
// Redis returns INFO as a series of key:value pairs separated by \r\n.
// Comment/Section lines start with a # character.
func parseInfo(raw string) *info {
	result := &info{
		normal:    make(map[string]string),
		keyspace:  make([]keyspace, 0),
		errorstat: make([]errorstat, 0),
	}

	for line := range strings.SplitSeq(raw, "\r\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}

		if code, found := strings.CutPrefix(key, errorstatPrefix); found { // parse errorstat_<CODE>
			_, f, ok := parseCSVField(value)
			if !ok {
				continue
			}
			result.errorstat = append(result.errorstat, errorstat{code, f})
		} else if matches := keyspaceRegexp.FindStringSubmatch(key); matches != nil { // parse db<N> keyspace
			result.keyspace = append(result.keyspace, parseKeyspace(value, matches[1]))
		} else { // parse normal field
			result.normal[key] = value
		}
	}

	return result
}

// parseCSVField resolves a key and a float value from field=value. It's a best effort method.
// Returns false during an error.
func parseCSVField(raw string) (string, float64, bool) {
	key, value, found := strings.Cut(raw, "=")
	if !found {
		return "", 0.0, false
	}

	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", 0.0, false
	}

	return key, f, true
}

// parseKeyspace resolves float values from db0:keys=value1,expires=value2,avg_ttl=value3,subexpiry=value4 into a struct.
func parseKeyspace(raw string, n string) keyspace {
	result := keyspace{}
	for group := range strings.SplitSeq(raw, ",") {
		key, f, ok := parseCSVField(group)
		if !ok {
			continue
		}

		switch key {
		case "keys":
			result.keys = f

		case "expires":
			result.expires = f

		case "avg_ttl":
			result.avgTTL = f

		case "subexpiry":
			result.subexpiry = f
		}
	}

	result.id = n

	return result
}
