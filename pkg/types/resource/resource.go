package resource

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
)

type ResourceOption func(*v2.Resource)

func WithAnnotation(msgs ...proto.Message) ResourceOption {
	return func(r *v2.Resource) {
		annos := annotations.Annotations(r.Annotations)
		for _, msg := range msgs {
			annos.Append(msg)
		}
		r.Annotations = annos
	}
}
