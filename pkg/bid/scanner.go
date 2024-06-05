package bid

import (
	"fmt"
	"strings"
)

type tokenType int

const (
	colon tokenType = iota
	slash
	literal
	eof
)

type bidScanner struct {
	index int
	str   string
}

func (rs *bidScanner) nextToken() (tokenType, string, int, error) {
	// Do not modify rs.index in here. It will break Peek()
	if rs.index >= len(rs.str) {
		return eof, "", len(rs.str), nil
	}

	if rs.str[rs.index] == ':' {
		return colon, ":", rs.index + 1, nil
	}
	if rs.str[rs.index] == '/' {
		return slash, "/", rs.index + 1, nil
	}

	var token strings.Builder
	for i := rs.index; i < len(rs.str); i++ {
		switch rs.str[i] {
		case ':', '/':
			return literal, token.String(), i, nil
		case '\\':
			if i+1 >= len(rs.str) {
				return -1, "", -1, fmt.Errorf("invalid escape sequence")
			}
			nextChar := rs.str[i+1]
			switch nextChar {
			case ':', '/', '\\':
				i++
				err := token.WriteByte(nextChar)
				if err != nil {
					return -1, "", -1, err
				}
			default:
				return -1, "", -1, fmt.Errorf("invalid escape sequence")
			}
		default:
			err := token.WriteByte(rs.str[i])
			if err != nil {
				return -1, "", -1, err
			}
		}
	}

	return literal, token.String(), len(rs.str), nil
}

func (rs *bidScanner) PeekToken() (tokenType, string, error) {
	tType, val, _, err := rs.nextToken()
	if err != nil {
		return -1, "", err
	}
	return tType, val, nil
}

func (rs *bidScanner) NextToken() (tokenType, string, error) {
	tType, val, idx, err := rs.nextToken()
	if err != nil {
		return -1, "", err
	}
	rs.index = idx
	return tType, val, nil
}

func (rs *bidScanner) SkipToken() error {
	_, _, idx, err := rs.nextToken()
	if err != nil {
		return err
	}
	rs.index = idx
	return nil
}
