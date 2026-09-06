package collector

import (
	"regexp"
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
	keys      *string
	expires   *string
	avgTTL    *string
	subexpiry *string
}

// errorstat is a parsed Redis INFO Errorstat row:
// errorstat_<CODE>:count=value.
type errorstat struct {
	code  string
	value string
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
			count, ok := strings.CutPrefix(value, "count=")
			if !ok {
				continue
			}

			result.errorstat = append(result.errorstat, errorstat{
				code:  code,
				value: count,
			})
		} else if matches := keyspaceRegexp.FindStringSubmatch(key); matches != nil { // parse db<N> keyspace
			result.keyspace = append(result.keyspace, parseKeyspace(value, matches[1]))
		} else { // parse normal field
			result.normal[key] = value
		}
	}

	return result
}

// parseKeyspace resolves values from db0:keys=value1,expires=value2,avg_ttl=value3,subexpiry=value4 into a struct.
// Any field absent from raw is left as nil.
func parseKeyspace(raw string, n string) keyspace {
	result := keyspace{
		id: n,
	}
	for group := range strings.SplitSeq(raw, ",") {
		key, value, ok := strings.Cut(group, "=")
		if !ok {
			continue
		}

		v := value
		switch key {
		case "keys":
			result.keys = &v

		case "expires":
			result.expires = &v

		case "avg_ttl":
			result.avgTTL = &v

		case "subexpiry":
			result.subexpiry = &v
		}
	}

	return result
}
