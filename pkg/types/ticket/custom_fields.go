package ticket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

var ErrFieldNil = errors.New("error: field is nil")
var ErrTicketValidationError = errors.New("create ticket request is not valid")

// CustomFieldForSchemaField returns a typed custom field for a given schema field.
func CustomFieldForSchemaField(id string, schema *v2.TicketSchema, value interface{}) (*v2.TicketCustomField, error) {
	field, ok := schema.GetCustomFields()[id]
	if !ok {
		return nil, fmt.Errorf("error: id(%s) not found in schema", id)
	}

	switch field.GetValue().(type) {
	case *v2.TicketCustomField_StringValue:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return StringField(id, v), nil

	case *v2.TicketCustomField_StringValues:
		v, ok := value.([]string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return StringsField(id, v), nil

	case *v2.TicketCustomField_BoolValue:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return BoolField(id, v), nil

	case *v2.TicketCustomField_TimestampValue:
		v, ok := value.(*timestamppb.Timestamp)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return TimestampField(id, v.AsTime()), nil

	case *v2.TicketCustomField_PickStringValue:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return PickStringField(id, v), nil

	case *v2.TicketCustomField_PickMultipleStringValues:
		v, ok := value.([]string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return PickMultipleStringsField(id, v), nil

	case *v2.TicketCustomField_PickObjectValue:
		rawBytes, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}

		ov := &v2.TicketCustomFieldObjectValue{}
		err = protojson.Unmarshal(rawBytes, ov)
		if err != nil {
			return nil, err
		}

		return PickObjectValueField(id, ov), nil

	case *v2.TicketCustomField_PickMultipleObjectValues:
		rawValue, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}

		var vals []interface{}
		err = json.Unmarshal(rawValue, &vals)
		if err != nil {
			return nil, err
		}
		var ret []*v2.TicketCustomFieldObjectValue

		for _, v := range vals {
			rawBytes, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}

			ov := &v2.TicketCustomFieldObjectValue{}
			err = protojson.Unmarshal(rawBytes, ov)
			if err != nil {
				return nil, err
			}

			ret = append(ret, ov)
		}

		return PickMultipleObjectValuesField(id, ret), nil

	default:
		return nil, errors.New("error: unknown custom field type")
	}
}

func GetStringValue(field *v2.TicketCustomField) (string, error) {
	if field == nil {
		return "", ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_StringValue)
	if !ok {
		return "", errors.New("error: expected string value")
	}
	return v.StringValue.Value, nil
}

func GetStringsValue(field *v2.TicketCustomField) ([]string, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_StringValues)
	if !ok {
		return nil, errors.New("error: expected string values")
	}
	return v.StringValues.Values, nil
}

func GetBoolValue(field *v2.TicketCustomField) (bool, error) {
	if field == nil {
		return false, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_BoolValue)
	if !ok {
		return false, errors.New("error: expected bool value")
	}
	return v.BoolValue.Value, nil
}

func GetTimestampValue(field *v2.TicketCustomField) (time.Time, error) {
	if field == nil {
		return time.Time{}, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_TimestampValue)
	if !ok {
		return time.Time{}, errors.New("error: expected timestamp value")
	}
	return v.TimestampValue.Value.AsTime(), nil
}

func GetPickStringValue(field *v2.TicketCustomField) (string, error) {
	if field == nil {
		return "", ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickStringValue)
	if !ok {
		return "", errors.New("error: expected pick string value")
	}
	return v.PickStringValue.Value, nil
}

func GetPickMultipleStringValues(field *v2.TicketCustomField) ([]string, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickMultipleStringValues)
	if !ok {
		return nil, errors.New("error: expected pick multiple string values")
	}
	return v.PickMultipleStringValues.Values, nil
}

func GetPickObjectValue(field *v2.TicketCustomField) (*v2.TicketCustomFieldObjectValue, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickObjectValue)
	if !ok {
		return nil, errors.New("error: expected pick object value")
	}
	return v.PickObjectValue.Value, nil
}

func GetPickMultipleObjectValues(field *v2.TicketCustomField) ([]*v2.TicketCustomFieldObjectValue, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickMultipleObjectValues)
	if !ok {
		return nil, errors.New("error: expected pick multiple object values")
	}
	return v.PickMultipleObjectValues.Values, nil
}

// GetCustomFieldValue returns the interface{} of the value set on a given custom field.
func GetCustomFieldValue(field *v2.TicketCustomField) (interface{}, error) {
	if field == nil {
		return nil, nil
	}
	switch v := field.GetValue().(type) {
	case *v2.TicketCustomField_StringValue:
		return v.StringValue.GetValue(), nil

	case *v2.TicketCustomField_StringValues:
		return v.StringValues.GetValues(), nil

	case *v2.TicketCustomField_BoolValue:
		return v.BoolValue.GetValue(), nil

	case *v2.TicketCustomField_TimestampValue:
		return v.TimestampValue.GetValue(), nil

	case *v2.TicketCustomField_PickStringValue:
		return v.PickStringValue.GetValue(), nil

	case *v2.TicketCustomField_PickMultipleStringValues:
		return v.PickMultipleStringValues.GetValues(), nil

	case *v2.TicketCustomField_PickObjectValue:
		return v.PickObjectValue.GetValue(), nil

	case *v2.TicketCustomField_PickMultipleObjectValues:
		return v.PickMultipleObjectValues.GetValues(), nil

	default:
		return false, errors.New("error: unknown custom field type")
	}
}

// TODO(lauren) doesn't validate fields on ticket that are not in the schema
// ValidateTicket takes a ticket schema and ensures that the supplied ticket conforms.
func ValidateTicket(ctx context.Context, schema *v2.TicketSchema, ticket *v2.Ticket) (bool, error) {
	l := ctxzap.Extract(ctx)

	// Validate the ticket status is one defined in the schema
	// Ticket status is not required so if a ticket doesn't have a status
	// we don't need to validate, skip the loop in this case
	validTicketStatus := ticket.Status == nil
	if !validTicketStatus {
		for _, status := range schema.GetStatuses() {
			if ticket.Status.GetId() == status.GetId() {
				validTicketStatus = true
				break
			}
		}
	}
	if !validTicketStatus {
		l.Debug("error: invalid ticket: could not find status", zap.String("status_id", ticket.Status.GetId()))
		return false, nil
	}

	// Validate the ticket type is one defined in the schema
	// Ticket type is not required so if a ticket doesn't have a type
	// we don't need to validate, skip the loop in this case
	validTicketType := ticket.Type == nil
	if !validTicketType {
		for _, tType := range schema.GetTypes() {
			if ticket.Type.GetId() == tType.GetId() {
				validTicketType = true
				break
			}
		}
	}
	if !validTicketType {
		l.Debug("error: invalid ticket: could not find ticket type", zap.String("ticket_type_id", ticket.Type.GetId()))
		return false, nil
	}

	schemaCustomFields := schema.GetCustomFields()
	ticketCustomFields := ticket.GetCustomFields()

	for id, cf := range schemaCustomFields {
		ticketCf, ok := ticketCustomFields[id]
		if !ok {
			if cf.Required {
				l.Debug("error: invalid ticket: missing custom field", zap.String("custom_field_id", cf.Id))
				return false, nil
			} else {
				// field not present but not required, so skip it
				continue
			}
		}

		switch v := cf.GetValue().(type) {
		case *v2.TicketCustomField_StringValue:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_StringValue)
			if !tok {
				l.Debug("error: invalid ticket: expected string value for field", zap.String("custom_field_id", cf.Id), zap.Any("value", tv))
				return false, nil
			}

			if cf.Required && tv.StringValue.GetValue() == "" {
				l.Debug("error: invalid ticket: string value is required but was empty", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

		case *v2.TicketCustomField_StringValues:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_StringValues)
			if !tok {
				l.Debug("error: invalid ticket: expected string values for field", zap.String("custom_field_id", cf.Id), zap.Any("values", tv))
				return false, nil
			}

			if cf.Required && len(tv.StringValues.GetValues()) == 0 {
				l.Debug("error: invalid ticket: string values is required but was empty", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

		case *v2.TicketCustomField_BoolValue:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_BoolValue)
			if !tok {
				l.Debug("error: invalid ticket: expected bool value for field", zap.String("custom_field_id", cf.Id), zap.Any("value", tv))
				return false, nil
			}

		case *v2.TicketCustomField_TimestampValue:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_TimestampValue)
			if !tok {
				l.Debug("error: invalid ticket: expected timestamp value for field", zap.String("custom_field_id", cf.Id), zap.Any("value", tv))
				return false, nil
			}

			if cf.Required && tv.TimestampValue.GetValue() == nil {
				l.Debug("error: invalid ticket: expected timestamp value for field but was empty", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

		case *v2.TicketCustomField_PickStringValue:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickStringValue)
			if !tok {
				l.Debug("error: invalid ticket: expected string value for field", zap.String("custom_field_id", cf.Id), zap.Any("value", tv))
				return false, nil
			}

			ticketValue := tv.PickStringValue.GetValue()
			allowedValues := v.PickStringValue.GetAllowedValues()

			// String value is empty but custom field is not required, skip further validation
			if !cf.Required && ticketValue == "" {
				continue
			}

			// Custom field is required, check if string is empty
			if ticketValue == "" {
				l.Debug("error: invalid ticket: expected string value for field but was empty", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			foundMatch := false
			for _, m := range allowedValues {
				if m == ticketValue {
					foundMatch = true
					break
				}
			}
			if !foundMatch {
				l.Debug(
					"error: invalid ticket: expected value from schema",
					zap.String("custom_field_id", cf.Id),
					zap.String("value", ticketValue),
					zap.Strings("allowed_values", allowedValues),
				)
				return false, nil
			}

		case *v2.TicketCustomField_PickMultipleStringValues:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickMultipleStringValues)
			if !tok {
				l.Debug("error: invalid ticket: expected string values for field", zap.String("custom_field_id", cf.Id), zap.Any("values", tv))
				return false, nil
			}

			ticketValues := tv.PickMultipleStringValues.GetValues()
			allowedValues := v.PickMultipleStringValues.GetAllowedValues()

			// String values are empty but custom field is not required, skip further validation
			if !cf.Required && len(ticketValues) == 0 {
				continue
			}

			// Custom field is required so check if string values are empty
			if len(ticketValues) == 0 {
				l.Debug("error: invalid ticket: string values is required but was empty", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			foundMatches := 0
			for _, tm := range ticketValues {
				for _, m := range allowedValues {
					if m == tm {
						foundMatches++
					}
				}
			}
			if len(ticketValues) != foundMatches {
				l.Debug(
					"error: invalid ticket: expected value from schema",
					zap.String("custom_field_id", cf.Id),
					zap.Strings("values", ticketValues),
					zap.Strings("allowed_values", allowedValues),
				)
				return false, nil
			}

		case *v2.TicketCustomField_PickObjectValue:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickObjectValue)
			if !tok {
				l.Debug("error: invalid ticket: expected object value for field", zap.String("custom_field_id", cf.Id), zap.Any("value", tv))
				return false, nil
			}

			ticketValue := tv.PickObjectValue.GetValue()
			allowedValues := v.PickObjectValue.GetAllowedValues()

			// Object value for field is nil, but custom field is not required, skip further validation
			if !cf.Required && (ticketValue == nil || ticketValue.GetId() == "") {
				continue
			}

			// Custom field is required so check if object value for field is nil
			if ticketValue == nil || ticketValue.GetId() == "" {
				l.Debug("error: invalid ticket: expected object value for field but was nil", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			foundMatch := false
			for _, m := range allowedValues {
				if m.GetId() == ticketValue.GetId() {
					foundMatch = true
					break
				}
			}
			if !foundMatch {
				l.Debug(
					"error: invalid ticket: expected value from schema",
					zap.String("custom_field_id", cf.Id),
					zap.String("value_id", ticketValue.GetId()),
					zap.Any("allowed_values", allowedValues),
				)
				return false, nil
			}

		case *v2.TicketCustomField_PickMultipleObjectValues:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickMultipleObjectValues)
			if !tok {
				l.Debug("error: invalid ticket: expected object values for field", zap.String("custom_field_id", cf.Id), zap.Any("values", tv))
				return false, nil
			}

			ticketValues := tv.PickMultipleObjectValues.GetValues()
			allowedValues := v.PickMultipleObjectValues.GetAllowedValues()

			// Object values are empty but custom field is not required, skip further validation
			if !cf.Required && len(ticketValues) == 0 {
				continue
			}

			// Custom field is required so check if object values are empty
			if len(ticketValues) == 0 {
				l.Debug("error: invalid ticket: object values is required but was empty", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.Id))
				return false, nil
			}

			foundMatches := 0
			for _, tm := range ticketValues {
				for _, m := range allowedValues {
					if m.GetId() == tm.GetId() {
						foundMatches++
					}
				}
			}
			if len(ticketValues) != foundMatches {
				l.Debug(
					"error: invalid ticket: expected value from schema",
					zap.String("custom_field_id", cf.Id),
					zap.Any("values", ticketValues),
					zap.Any("allowed_values", allowedValues),
				)
				return false, nil
			}

		default:
			l.Debug("error: invalid schema: unknown custom field type", zap.Any("custom_field_type", v))
			return false, errors.New("error: invalid schema: unknown custom field type")
		}
	}

	return true, nil
}

func StringFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_StringValue{
			StringValue: &v2.TicketCustomFieldStringValue{},
		},
	}
}

func StringField(id, value string) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_StringValue{
			StringValue: &v2.TicketCustomFieldStringValue{
				Value: value,
			},
		},
	}
}

func StringsFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_StringValues{
			StringValues: &v2.TicketCustomFieldStringValues{},
		},
	}
}

func StringsField(id string, values []string) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_StringValues{
			StringValues: &v2.TicketCustomFieldStringValues{
				Values: values,
			},
		},
	}
}

func BoolFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_BoolValue{
			BoolValue: &v2.TicketCustomFieldBoolValue{},
		},
	}
}

func BoolField(id string, value bool) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_BoolValue{
			BoolValue: &v2.TicketCustomFieldBoolValue{
				Value: value,
			},
		},
	}
}

func TimestampFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_TimestampValue{
			TimestampValue: &v2.TicketCustomFieldTimestampValue{},
		},
	}
}

func TimestampField(id string, value time.Time) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_TimestampValue{
			TimestampValue: &v2.TicketCustomFieldTimestampValue{
				Value: timestamppb.New(value),
			},
		},
	}
}

func PickStringFieldSchema(id, displayName string, required bool, allowedValues []string) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_PickStringValue{
			PickStringValue: &v2.TicketCustomFieldPickStringValue{
				AllowedValues: allowedValues,
			},
		},
	}
}

func PickStringField(id string, value string) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_PickStringValue{
			PickStringValue: &v2.TicketCustomFieldPickStringValue{
				Value: value,
			},
		},
	}
}

func PickMultipleStringsFieldSchema(id, displayName string, required bool, allowedValues []string) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_PickMultipleStringValues{
			PickMultipleStringValues: &v2.TicketCustomFieldPickMultipleStringValues{
				AllowedValues: allowedValues,
			},
		},
	}
}

func PickMultipleStringsField(id string, values []string) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_PickMultipleStringValues{
			PickMultipleStringValues: &v2.TicketCustomFieldPickMultipleStringValues{
				Values: values,
			},
		},
	}
}

func PickObjectValueFieldSchema(id, displayName string, required bool, allowedValues []*v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_PickObjectValue{
			PickObjectValue: &v2.TicketCustomFieldPickObjectValue{
				AllowedValues: allowedValues,
			},
		},
	}
}

func PickObjectValueField(id string, value *v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_PickObjectValue{
			PickObjectValue: &v2.TicketCustomFieldPickObjectValue{
				Value: value,
			},
		},
	}
}

func PickMultipleObjectValuesFieldSchema(id, displayName string, required bool, allowedValues []*v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		Value: &v2.TicketCustomField_PickMultipleObjectValues{
			PickMultipleObjectValues: &v2.TicketCustomFieldPickMultipleObjectValues{
				AllowedValues: allowedValues,
			},
		},
	}
}

func PickMultipleObjectValuesField(id string, values []*v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return &v2.TicketCustomField{
		Id: id,
		Value: &v2.TicketCustomField_PickMultipleObjectValues{
			PickMultipleObjectValues: &v2.TicketCustomFieldPickMultipleObjectValues{
				Values: values,
			},
		},
	}
}
