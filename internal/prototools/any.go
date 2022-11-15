package prototools

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// UnmarshalFromAnys searches a set of Anys for the first message of our type and attempts to unmarshal to it.
func UnmarshalFromAnys(needle proto.Message, haystack []*anypb.Any) (bool, error) {
	for _, v := range haystack {
		if v.MessageIs(needle) {
			if err := v.UnmarshalTo(needle); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}
