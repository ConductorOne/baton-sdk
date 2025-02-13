package field

import (
	"fmt"
	"regexp"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
)

type IntRuler struct {
	rules *v1_conf.Int64Rules
}

func NewIntBuilder(rules *v1_conf.Int64Rules) *IntRuler {
	return &IntRuler{rules: rules}
}

func (b *IntRuler) Eq(value int64) *IntRuler {
	b.rules.Eq = &value
	return b
}

func (b *IntRuler) Gt(value int64) *IntRuler {
	b.rules.Gt = &value
	return b
}

func (b *IntRuler) Lt(value int64) *IntRuler {
	b.rules.Lt = &value
	return b
}

func (b *IntRuler) Lte(value int64) *IntRuler {
	b.rules.Lte = &value
	return b
}

func (b *IntRuler) Gte(value int64) *IntRuler {
	b.rules.Gte = &value
	return b
}

func (b *IntRuler) In(values []int64) *IntRuler {
	b.rules.In = values
	return b
}

func (b *IntRuler) NotIn(values []int64) *IntRuler {
	b.rules.NotIn = values
	return b
}

func (b *IntRuler) ValidateEmpty(value bool) *IntRuler {
	b.rules.ValidateEmpty = value
	return b
}

type BoolRuler struct {
	rules *v1_conf.BoolRules
}

func NewBoolBuilder(rules *v1_conf.BoolRules) *BoolRuler {
	return &BoolRuler{rules: rules}
}

func (b *BoolRuler) Eq(v bool) *BoolRuler {
	b.rules.Eq = &v
	return b
}

type StringRuler struct {
	rules *v1_conf.StringRules
}

func NewStringBuilder(rules *v1_conf.StringRules) *StringRuler {
	return &StringRuler{rules: rules}
}

func (b *StringRuler) Eq(value string) *StringRuler {
	b.rules.Eq = &value
	return b
}

func (b *StringRuler) Len(value uint64) *StringRuler {
	b.rules.Len = &value
	return b
}

func (b *StringRuler) MinLen(value uint64) *StringRuler {
	b.rules.MinLen = &value
	return b
}

func (b *StringRuler) MaxLen(value uint64) *StringRuler {
	b.rules.MaxLen = &value
	return b
}

func (b *StringRuler) Pattern(value string) *StringRuler {
	_, err := regexp.CompilePOSIX(value)
	if err != nil {
		panic(fmt.Errorf("invalid regex: %w", err))
	}
	b.rules.Pattern = &value
	return b
}

func (b *StringRuler) Prefix(value string) *StringRuler {
	b.rules.Prefix = &value
	return b
}

func (b *StringRuler) Suffix(value string) *StringRuler {
	b.rules.Suffix = &value
	return b
}

func (b *StringRuler) Contains(value string) *StringRuler {
	b.rules.Contains = &value
	return b
}

func (b *StringRuler) NotContains(value string) *StringRuler {
	b.rules.NotContains = &value
	return b
}

func (b *StringRuler) In(values []string) *StringRuler {
	b.rules.In = values
	return b
}

func (b *StringRuler) NotIn(values []string) *StringRuler {
	b.rules.NotIn = values
	return b
}

func (b *StringRuler) IsEmail() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_EMAIL
	return b
}

func (b *StringRuler) IsHostname() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_HOSTNAME
	return b
}

func (b *StringRuler) IsIP() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_IP
	return b
}

func (b *StringRuler) IsIpv4() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_IPV4
	return b
}

func (b *StringRuler) IsIpv6() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_IPV6
	return b
}

func (b *StringRuler) IsURI() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_URI
	return b
}

func (b *StringRuler) IsAddress() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_ADDRESS
	return b
}

func (b *StringRuler) IsUUID() *StringRuler {
	if b.rules.WellKnown != 0 {
		panic("well known rules are already set")
	}
	b.rules.WellKnown = v1_conf.WellKnownString_WELL_KNOWN_STRING_UUID
	return b
}

type StringSliceRuler struct {
	rules    *v1_conf.RepeatedStringRules
	stringer *StringRuler
}

func NewRepeatedStringBuilder(rules *v1_conf.RepeatedStringRules) *StringSliceRuler {
	itemRules := rules.ItemRules
	if itemRules == nil {
		itemRules = &v1_conf.StringRules{}
		rules.ItemRules = itemRules
	}
	stringer := NewStringBuilder(itemRules)
	return &StringSliceRuler{rules: rules, stringer: stringer}
}

func (b *StringSliceRuler) MinItems(value uint64) *StringSliceRuler {
	b.rules.MinItems = &value
	return b
}

func (b *StringSliceRuler) MaxItems(value uint64) *StringSliceRuler {
	b.rules.MaxItems = &value
	return b
}

func (b *StringSliceRuler) Unique(unique bool) *StringSliceRuler {
	b.rules.Unique = unique
	return b
}

func (b *StringSliceRuler) ValidateEmpty(value bool) *StringSliceRuler {
	b.rules.ValidateEmpty = value
	return b
}

func (b *StringSliceRuler) ItemRules(f func(stringer *StringRuler)) *StringSliceRuler {
	f(b.stringer)
	return b
}
