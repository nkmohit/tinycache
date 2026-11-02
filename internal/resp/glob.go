package resp

// maxGlobPattern caps KEYS patterns so a long chain of stars cannot blow the stack.
const maxGlobPattern = 1024

const maxGlobSteps = 1 << 20

type memoVal uint8

const (
	memoNone memoVal = iota
	memoVisiting
	memoYes
	memoNo
)

type globState struct {
	memo  map[[2]int]memoVal
	steps int
}

// matchGlob reports whether name matches a Redis KEYS glob.
// Supported forms: *, ?, \, character classes, ranges, and [^...] negation.
// Matching is byte-wise and case-sensitive.
func matchGlob(pattern, name string) bool {
	if len(pattern) > maxGlobPattern {
		return false
	}
	g := &globState{memo: make(map[[2]int]memoVal)}
	return g.match(pattern, name, 0, 0)
}

func (g *globState) match(pat, s string, pi, si int) bool {
	key := [2]int{pi, si}
	switch g.memo[key] {
	case memoYes:
		return true
	case memoNo, memoVisiting:
		return false
	}
	g.steps++
	if g.steps > maxGlobSteps {
		g.memo[key] = memoNo
		return false
	}
	g.memo[key] = memoVisiting
	res := g.eval(pat, s, pi, si)
	if res {
		g.memo[key] = memoYes
	} else {
		g.memo[key] = memoNo
	}
	return res
}

func (g *globState) eval(pat, s string, pi, si int) bool {
	for {
		if pi == len(pat) {
			return si == len(s)
		}
		switch pat[pi] {
		case '*':
			pi++
			for pi < len(pat) && pat[pi] == '*' {
				pi++
			}
			if pi == len(pat) {
				return true
			}
			for ; si <= len(s); si++ {
				if g.match(pat, s, pi, si) {
					return true
				}
			}
			return false
		case '?':
			if si >= len(s) {
				return false
			}
			pi++
			si++
		case '[':
			if si >= len(s) {
				return false
			}
			next, ok := matchClass(pat, pi, s[si])
			if !ok {
				return false
			}
			pi = next
			si++
		case '\\':
			if pi+1 < len(pat) {
				pi++
			}
			if si >= len(s) || pat[pi] != s[si] {
				return false
			}
			pi++
			si++
		default:
			if si >= len(s) || pat[pi] != s[si] {
				return false
			}
			pi++
			si++
		}
	}
}

// matchClass consumes a [...] class starting at pi and reports whether ch is in it.
// The returned index is the first byte after the closing ']'.
func matchClass(pat string, pi int, ch byte) (int, bool) {
	i := pi + 1
	if i >= len(pat) {
		return len(pat), false
	}
	negate := false
	if pat[i] == '^' {
		negate = true
		i++
	}
	if i >= len(pat) {
		return len(pat), false
	}
	matched := false
	closed := false
	for i < len(pat) {
		if pat[i] == ']' {
			closed = true
			i++
			break
		}
		if pat[i] == '\\' && i+1 < len(pat) {
			i++
			if pat[i] == ch {
				matched = true
			}
			i++
			continue
		}
		if i+2 < len(pat) && pat[i+1] == '-' && pat[i+2] != ']' {
			start := pat[i]
			end := pat[i+2]
			if start > end {
				start, end = end, start
			}
			if ch >= start && ch <= end {
				matched = true
			}
			i += 3
			continue
		}
		if pat[i] == ch {
			matched = true
		}
		i++
	}
	if !closed {
		return len(pat), false
	}
	if negate {
		matched = !matched
	}
	return i, matched
}
