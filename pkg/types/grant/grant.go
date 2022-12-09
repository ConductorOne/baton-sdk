package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
)

type GrantOption func(*v2.Grant)

func WithAnnotation(msgs ...proto.Message) GrantOption {
	return func(g *v2.Grant) {
		annos := annotations.Annotations(g.Annotations)
		for _, msg := range msgs {
			annos.Append(msg)
		}
		g.Annotations = annos
	}
}
