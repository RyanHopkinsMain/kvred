package db

// globMatch implements Redis-style glob patterns for KEYS:
//
//   - any run of characters (including none)
//     ?        exactly one character
//     [abc]    one of a, b, c      [a-z]  a range      [^a]  anything but a
//     \x       a literal x
//
// It's the classic linear-ish wildcard algorithm: walk both strings, and when
// a mismatch happens after a '*', backtrack so the star swallows one more byte.
func globMatch(pattern, s string) bool {
	p, i := 0, 0
	starP, starI := -1, -1
	for i < len(s) || p < len(pattern) {
		if p < len(pattern) {
			switch c := pattern[p]; c {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				if i < len(s) {
					p, i = p+1, i+1
					continue
				}
			case '[':
				if i < len(s) {
					if ok, next := matchClass(pattern, p, s[i]); ok {
						p, i = next, i+1
						continue
					}
				}
			case '\\':
				if p+1 < len(pattern) && i < len(s) && pattern[p+1] == s[i] {
					p, i = p+2, i+1
					continue
				}
			default:
				if i < len(s) && s[i] == c {
					p, i = p+1, i+1
					continue
				}
			}
		}
		if starP >= 0 && starI < len(s) {
			starI++
			p, i = starP+1, starI
			continue
		}
		return false
	}
	return true
}

// matchClass tests ch against the bracket expression starting at pattern[open].
// It returns whether it matched and the index just past the closing ']'.
// An unterminated '[' is treated as a literal.
func matchClass(pattern string, open int, ch byte) (bool, int) {
	i := open + 1
	negate := i < len(pattern) && (pattern[i] == '^' || pattern[i] == '!')
	if negate {
		i++
	}
	matched := false
	for i < len(pattern) && pattern[i] != ']' {
		lo := pattern[i]
		if lo == '\\' && i+1 < len(pattern) {
			i++
			lo = pattern[i]
		}
		hi := lo
		if i+2 < len(pattern) && pattern[i+1] == '-' && pattern[i+2] != ']' {
			hi = pattern[i+2]
			i += 2
			if lo > hi {
				lo, hi = hi, lo
			}
		}
		if lo <= ch && ch <= hi {
			matched = true
		}
		i++
	}
	if i >= len(pattern) {
		return ch == '[', open + 1
	}
	return matched != negate, i + 1
}
