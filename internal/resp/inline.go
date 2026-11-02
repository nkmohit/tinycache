package resp

import "strconv"

func isInlineSpace(c byte) bool {
	return c == ' ' || c == '\t'
}

// splitInline splits a telnet-style command. Double quotes and backslash
// escapes are honored; single quotes are literal until the closing quote.
func splitInline(line string) ([]string, error) {
	var args []string
	var b []byte
	i := 0
	n := len(line)
	for i < n {
		for i < n && isInlineSpace(line[i]) {
			i++
		}
		if i >= n {
			break
		}
		b = b[:0]
		var quote byte
		for i < n {
			c := line[i]
			if quote == 0 && isInlineSpace(c) {
				break
			}
			if quote == 0 && (c == '"' || c == '\'') {
				quote = c
				i++
				continue
			}
			if quote != 0 && c == quote {
				quote = 0
				i++
				continue
			}
			if c == '\\' && quote != '\'' {
				if i+1 >= n {
					return nil, proto("ERR Protocol error: unbalanced quotes in request")
				}
				i++
				esc := line[i]
				switch esc {
				case 'n':
					b = append(b, '\n')
				case 'r':
					b = append(b, '\r')
				case 't':
					b = append(b, '\t')
				case 'a':
					b = append(b, '\a')
				case 'b':
					b = append(b, '\b')
				case 'x':
					if i+2 >= n {
						return nil, proto("ERR Protocol error: unbalanced quotes in request")
					}
					v, err := strconv.ParseUint(line[i+1:i+3], 16, 8)
					if err != nil {
						return nil, proto("ERR Protocol error: invalid hex escape")
					}
					b = append(b, byte(v))
					i += 3
					continue
				default:
					b = append(b, esc)
				}
				i++
				continue
			}
			b = append(b, c)
			i++
		}
		if quote != 0 {
			return nil, proto("ERR Protocol error: unbalanced quotes in request")
		}
		args = append(args, string(b))
	}
	if len(args) == 0 {
		return nil, ErrEmptyCommand
	}
	return args, nil
}
