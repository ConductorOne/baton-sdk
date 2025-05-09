package naive

import (
	"context"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type listRequest interface {
	proto.Message
	GetPageSize() uint32
	GetPageToken() string
	GetAnnotations() []*anypb.Any
}

type listResponse[T proto.Message] interface {
	GetNextPageToken() string
	GetAnnotations() []*anypb.Any
	GetList() []T
}

// createRequest creates a new request object of type REQ using reflection.
func createRequest[REQ listRequest]() REQ {
	var r REQ
	baseType := reflect.TypeOf(r).Elem()
	pointerToInitializedVal := reflect.New(baseType)
	return pointerToInitializedVal.Interface().(REQ)
}

// setFieldIfValid sets a field in a struct if it exists and can be set.
func setFieldIfValid(obj interface{}, fieldName string, setValue func(reflect.Value)) {
	val := reflect.ValueOf(obj)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return
	}

	field := val.Elem().FieldByName(fieldName)
	if field.IsValid() && field.CanSet() {
		setValue(field)
	}
}

// setPageSize sets the PageSize field in a request to the specified value.
func setPageSize(req listRequest, size uint64) {
	setFieldIfValid(req, "PageSize", func(field reflect.Value) {
		field.SetUint(size)
	})
}

// setPageToken sets the PageToken field in a request to the specified token.
func setPageToken(req listRequest, token string) {
	setFieldIfValid(req, "PageToken", func(field reflect.Value) {
		field.SetString(token)
	})
}

type listFunc[T proto.Message, REQ listRequest, RESP listResponse[T]] func(context.Context, REQ) (RESP, error)

func listAllObjects[T proto.Message, REQ listRequest, RESP listResponse[T]](ctx context.Context, list listFunc[T, REQ, RESP], cb func(items []T) (bool, error)) error {
	// Create a new request using reflection
	req := createRequest[REQ]()

	// Set initial page size
	setPageSize(req, 100) // Set a reasonable default page size

	var nextPageToken string
	for {
		// Set the page token for the current request if needed
		if nextPageToken != "" {
			setPageToken(req, nextPageToken)
		}

		// Call the list function with the current request
		resp, err := list(ctx, req)
		if err != nil {
			return err
		}

		// Collect the results
		shouldContinue, err := cb(resp.GetList())
		if err != nil {
			return err
		}
		if !shouldContinue {
			return nil
		}

		// Check if there are more pages
		nextPageToken = resp.GetNextPageToken()
		if nextPageToken == "" || len(resp.GetList()) == 0 {
			break // No more pages
		}
	}

	return nil
}
