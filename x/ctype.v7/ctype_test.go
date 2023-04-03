package ctype

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testCase struct {
	c    rune
	mask uint32
	is   bool
}

type stringTestCase struct {
	str       string
	maskFirst uint32
	maskNext  uint32
	is        bool
}

func TestIs(t *testing.T) {
	var isCases = []testCase{
		{'-', DOMAIN_CHAR, true},
		{'.', DOMAIN_CHAR, true},
		{'_', DOMAIN_CHAR, false},
		{'+', DOMAIN_CHAR, true},
		{'a', DOMAIN_CHAR, true},
		{'A', DOMAIN_CHAR, true},
		{'0', DOMAIN_CHAR, true},
		{':', DOMAIN_CHAR, false},
		{'1', ALPHA, false},
		{'a', ALPHA, true},
		{'A', ALPHA, true},
	}

	for _, a := range isCases {
		f := Is(a.mask, a.c)
		assert.Equal(t, a.is, f)
	}
}

func TestIsTypeEx(t *testing.T) {
	var strCases = []stringTestCase{
		{"", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, false},
		{"123", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, false},
		{"_", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, true},
		{"_123", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, true},
		{"x_123", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, true},
		{"x_", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, true},
		{"_x", CSYMBOL_FIRST_CHAR, CSYMBOL_NEXT_CHAR, true},
		{"", CSYMBOL_FIRST_CHAR, CSYMBOL_FIRST_CHAR, false},
		{"x_123", CSYMBOL_FIRST_CHAR, CSYMBOL_FIRST_CHAR, false},
		{"x_", CSYMBOL_FIRST_CHAR, CSYMBOL_FIRST_CHAR, true},
		{"_x", CSYMBOL_FIRST_CHAR, CSYMBOL_FIRST_CHAR, true},
		{"_", CSYMBOL_FIRST_CHAR, CSYMBOL_FIRST_CHAR, true},
	}
	for _, strCase := range strCases {
		f := IsTypeEx(strCase.maskFirst, strCase.maskNext, strCase.str)
		assert.Equal(t, strCase.is, f)

		if strCase.maskFirst == strCase.maskNext {
			f = IsType(strCase.maskFirst, strCase.str)
			assert.Equal(t, strCase.is, f)
		}
	}
}
