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
	"google.golang.org/protobuf/types/known/wrapperspb"

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

	switch field.WhichValue() {
	case v2.TicketCustomField_StringValue_case:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return StringField(id, v), nil

	case v2.TicketCustomField_StringValues_case:
		v, ok := value.([]string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return StringsField(id, v), nil

	case v2.TicketCustomField_BoolValue_case:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return BoolField(id, v), nil

	case v2.TicketCustomField_NumberValue_case:
		v, ok := value.(float32)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return NumberField(id, v), nil

	case v2.TicketCustomField_TimestampValue_case:
		v, ok := value.(*timestamppb.Timestamp)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return TimestampField(id, v.AsTime()), nil

	case v2.TicketCustomField_PickStringValue_case:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return PickStringField(id, v), nil

	case v2.TicketCustomField_PickMultipleStringValues_case:
		v, ok := value.([]string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for custom field: %s %T", id, v)
		}
		return PickMultipleStringsField(id, v), nil

	case v2.TicketCustomField_PickObjectValue_case:
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

	case v2.TicketCustomField_PickMultipleObjectValues_case:
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
	return v.StringValue.GetValue(), nil
}

func GetStringsValue(field *v2.TicketCustomField) ([]string, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_StringValues)
	if !ok {
		return nil, errors.New("error: expected string values")
	}
	return v.StringValues.GetValues(), nil
}

func GetBoolValue(field *v2.TicketCustomField) (bool, error) {
	if field == nil {
		return false, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_BoolValue)
	if !ok {
		return false, errors.New("error: expected bool value")
	}
	return v.BoolValue.GetValue(), nil
}

func GetNumberValue(field *v2.TicketCustomField) (float32, error) {
	if field == nil {
		return 0, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_NumberValue)
	if !ok {
		return 0, errors.New("error: expected number value")
	}
	return v.NumberValue.GetValue().GetValue(), nil
}

func GetTimestampValue(field *v2.TicketCustomField) (time.Time, error) {
	if field == nil {
		return time.Time{}, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_TimestampValue)
	if !ok {
		return time.Time{}, errors.New("error: expected timestamp value")
	}
	return v.TimestampValue.GetValue().AsTime(), nil
}

func GetPickStringValue(field *v2.TicketCustomField) (string, error) {
	if field == nil {
		return "", ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickStringValue)
	if !ok {
		return "", errors.New("error: expected pick string value")
	}
	return v.PickStringValue.GetValue(), nil
}

func GetPickMultipleStringValues(field *v2.TicketCustomField) ([]string, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickMultipleStringValues)
	if !ok {
		return nil, errors.New("error: expected pick multiple string values")
	}
	return v.PickMultipleStringValues.GetValues(), nil
}

func GetPickObjectValue(field *v2.TicketCustomField) (*v2.TicketCustomFieldObjectValue, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickObjectValue)
	if !ok {
		return nil, errors.New("error: expected pick object value")
	}
	return v.PickObjectValue.GetValue(), nil
}

func GetPickMultipleObjectValues(field *v2.TicketCustomField) ([]*v2.TicketCustomFieldObjectValue, error) {
	if field == nil {
		return nil, ErrFieldNil
	}
	v, ok := field.GetValue().(*v2.TicketCustomField_PickMultipleObjectValues)
	if !ok {
		return nil, errors.New("error: expected pick multiple object values")
	}
	return v.PickMultipleObjectValues.GetValues(), nil
}

// GetCustomFieldValue returns the interface{} of the value set on a given custom field.
func GetCustomFieldValue(field *v2.TicketCustomField) (interface{}, error) {
	if field == nil {
		return nil, nil
	}
	switch field.WhichValue() {
	case v2.TicketCustomField_StringValue_case:
		strVal := field.GetStringValue().GetValue()
		if strVal == "" {
			return nil, nil
		}
		return field.GetStringValue().GetValue(), nil

	case v2.TicketCustomField_StringValues_case:
		strVals := field.GetStringValues().GetValues()
		if len(strVals) == 0 {
			return nil, nil
		}
		return strVals, nil

	case v2.TicketCustomField_BoolValue_case:
		return field.GetBoolValue().GetValue(), nil

	case v2.TicketCustomField_NumberValue_case:
		wrapperVal := field.GetNumberValue().GetValue()
		if wrapperVal == nil {
			return nil, nil
		}
		return wrapperVal.GetValue(), nil
	case v2.TicketCustomField_TimestampValue_case:
		return field.GetTimestampValue().GetValue(), nil

	case v2.TicketCustomField_PickStringValue_case:
		strVal := field.GetPickStringValue().GetValue()
		if strVal == "" {
			return nil, nil
		}
		return strVal, nil

	case v2.TicketCustomField_PickMultipleStringValues_case:
		strVals := field.GetPickMultipleStringValues().GetValues()
		if len(strVals) == 0 {
			return nil, nil
		}
		return strVals, nil

	case v2.TicketCustomField_PickObjectValue_case:
		return field.GetPickObjectValue().GetValue(), nil

	case v2.TicketCustomField_PickMultipleObjectValues_case:
		objVals := field.GetPickMultipleObjectValues().GetValues()
		if len(objVals) == 0 {
			return nil, nil
		}
		return objVals, nil

	default:
		return false, errors.New("error: unknown custom field type")
	}
}

func GetDefaultCustomFieldValue(field *v2.TicketCustomField) (interface{}, error) {
	if field == nil {
		return nil, nil
	}
	switch field.WhichValue() {
	case v2.TicketCustomField_StringValue_case:
		strVal := field.GetStringValue().GetDefaultValue()
		if strVal == "" {
			return nil, nil
		}
		return strVal, nil

	case v2.TicketCustomField_StringValues_case:
		strVals := field.GetStringValues().GetDefaultValues()
		if len(strVals) == 0 {
			return nil, nil
		}
		return strVals, nil

	case v2.TicketCustomField_BoolValue_case:
		return field.GetBoolValue().GetValue(), nil

	case v2.TicketCustomField_NumberValue_case:
		defaultWrapper := field.GetNumberValue().GetDefaultValue()
		if defaultWrapper == nil {
			return nil, nil
		}
		return defaultWrapper.GetValue(), nil

	case v2.TicketCustomField_TimestampValue_case:
		return field.GetTimestampValue().GetDefaultValue(), nil

	case v2.TicketCustomField_PickStringValue_case:
		strVal := field.GetPickStringValue().GetDefaultValue()
		if strVal == "" {
			return nil, nil
		}
		return strVal, nil

	case v2.TicketCustomField_PickMultipleStringValues_case:
		strVals := field.GetPickMultipleStringValues().GetDefaultValues()
		if len(strVals) == 0 {
			return nil, nil
		}
		return strVals, nil

	case v2.TicketCustomField_PickObjectValue_case:
		return field.GetPickObjectValue().GetDefaultValue(), nil

	case v2.TicketCustomField_PickMultipleObjectValues_case:
		objVals := field.GetPickMultipleObjectValues().GetDefaultValues()
		if len(objVals) == 0 {
			return nil, nil
		}
		return objVals, nil

	default:
		return false, errors.New("error: unknown custom field type")
	}
}

func GetCustomFieldValueOrDefault(field *v2.TicketCustomField) (interface{}, error) {
	v, err := GetCustomFieldValue(field)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return GetDefaultCustomFieldValue(field)
	}
	return v, nil
}

// TODO(lauren) doesn't validate fields on ticket that are not in the schema
// ValidateTicket takes a ticket schema and ensures that the supplied ticket conforms.
func ValidateTicket(ctx context.Context, schema *v2.TicketSchema, ticket *v2.Ticket) (bool, error) {
	l := ctxzap.Extract(ctx)

	// Validate the ticket status is one defined in the schema
	// Ticket status is not required so if a ticket doesn't have a status
	// we don't need to validate, skip the loop in this case
	validTicketStatus := !ticket.HasStatus()
	if !validTicketStatus {
		for _, status := range schema.GetStatuses() {
			if ticket.GetStatus().GetId() == status.GetId() {
				validTicketStatus = true
				break
			}
		}
	}
	if !validTicketStatus {
		l.Debug("error: invalid ticket: could not find status", zap.String("status_id", ticket.GetStatus().GetId()))
		return false, nil
	}

	schemaCustomFields := schema.GetCustomFields()
	ticketCustomFields := ticket.GetCustomFields()

	for id, cf := range schemaCustomFields {
		ticketCf, ok := ticketCustomFields[id]
		if !ok {
			if cf.GetRequired() {
				l.Debug("error: invalid ticket: missing custom field", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			} else {
				// field not present but not required, so skip it
				continue
			}
		}

		switch cf.WhichValue() {
		case v2.TicketCustomField_StringValue_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_StringValue)
			if !tok {
				l.Debug("error: invalid ticket: expected string value for field", zap.String("custom_field_id", cf.GetId()), zap.Any("value", tv))
				return false, nil
			}

			stringValue := tv.StringValue.GetValue()
			if stringValue == "" {
				stringValue = tv.StringValue.GetDefaultValue()
			}

			if cf.GetRequired() && stringValue == "" {
				l.Debug("error: invalid ticket: string value is required but was empty", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

		case v2.TicketCustomField_StringValues_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_StringValues)
			if !tok {
				l.Debug("error: invalid ticket: expected string values for field", zap.String("custom_field_id", cf.GetId()), zap.Any("values", tv))
				return false, nil
			}

			stringValues := tv.StringValues.GetValues()
			if len(stringValues) == 0 {
				stringValues = tv.StringValues.GetDefaultValues()
			}

			if cf.GetRequired() && len(stringValues) == 0 {
				l.Debug("error: invalid ticket: string values is required but was empty", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

		case v2.TicketCustomField_BoolValue_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_BoolValue)
			if !tok {
				l.Debug("error: invalid ticket: expected bool value for field", zap.String("custom_field_id", cf.GetId()), zap.Any("value", tv))
				return false, nil
			}
		case v2.TicketCustomField_NumberValue_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_NumberValue)
			if !tok {
				l.Debug("error: invalid ticket: expected number value for field", zap.String("custom_field_id", cf.GetId()), zap.Any("value", tv))
				return false, nil
			}

		case v2.TicketCustomField_TimestampValue_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_TimestampValue)
			if !tok {
				l.Debug("error: invalid ticket: expected timestamp value for field", zap.String("custom_field_id", cf.GetId()), zap.Any("value", tv))
				return false, nil
			}

			timestampValue := tv.TimestampValue.GetValue()
			if timestampValue == nil {
				timestampValue = tv.TimestampValue.GetDefaultValue()
			}

			if cf.GetRequired() && timestampValue == nil {
				l.Debug("error: invalid ticket: expected timestamp value for field but was empty", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

		case v2.TicketCustomField_PickStringValue_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickStringValue)
			if !tok {
				l.Debug("error: invalid ticket: expected string value for field", zap.String("custom_field_id", cf.GetId()), zap.Any("value", tv))
				return false, nil
			}

			ticketValue := tv.PickStringValue.GetValue()
			allowedValues := cf.GetPickStringValue().GetAllowedValues()
			defaultTicketValue := tv.PickStringValue.GetDefaultValue()

			if ticketValue == "" {
				ticketValue = defaultTicketValue
			}

			// String value is empty but custom field is not required, skip further validation
			if !cf.GetRequired() && ticketValue == "" {
				continue
			}

			// Custom field is required, check if string is empty
			if ticketValue == "" {
				l.Debug("error: invalid ticket: expected string value for field but was empty", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.GetId()))
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
					zap.String("custom_field_id", cf.GetId()),
					zap.String("value", ticketValue),
					zap.Strings("allowed_values", allowedValues),
				)
				return false, nil
			}

		case v2.TicketCustomField_PickMultipleStringValues_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickMultipleStringValues)
			if !tok {
				l.Debug("error: invalid ticket: expected string values for field", zap.String("custom_field_id", cf.GetId()), zap.Any("values", tv))
				return false, nil
			}

			ticketValues := tv.PickMultipleStringValues.GetValues()
			allowedValues := cf.GetPickMultipleStringValues().GetAllowedValues()

			if len(ticketValues) == 0 {
				ticketValues = tv.PickMultipleStringValues.GetDefaultValues()
			}

			// String values are empty but custom field is not required, skip further validation
			if !cf.GetRequired() && len(ticketValues) == 0 {
				continue
			}

			// Custom field is required so check if string values are empty
			if len(ticketValues) == 0 {
				l.Debug("error: invalid ticket: string values is required but was empty", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.GetId()))
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
					zap.String("custom_field_id", cf.GetId()),
					zap.Strings("values", ticketValues),
					zap.Strings("allowed_values", allowedValues),
				)
				return false, nil
			}

		case v2.TicketCustomField_PickObjectValue_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickObjectValue)
			if !tok {
				l.Debug("error: invalid ticket: expected object value for field", zap.String("custom_field_id", cf.GetId()), zap.Any("value", tv))
				return false, nil
			}

			ticketValue := tv.PickObjectValue.GetValue()
			allowedValues := cf.GetPickObjectValue().GetAllowedValues()

			if ticketValue == nil || ticketValue.GetId() == "" {
				ticketValue = tv.PickObjectValue.GetDefaultValue()
			}

			// Object value for field is nil, but custom field is not required, skip further validation
			if !cf.GetRequired() && (ticketValue == nil || ticketValue.GetId() == "") {
				continue
			}

			// Custom field is required so check if object value for field is nil
			if ticketValue == nil || ticketValue.GetId() == "" {
				l.Debug("error: invalid ticket: expected object value for field but was nil", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.GetId()))
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
					zap.String("custom_field_id", cf.GetId()),
					zap.String("value_id", ticketValue.GetId()),
					zap.Any("allowed_values", allowedValues),
				)
				return false, nil
			}

		case v2.TicketCustomField_PickMultipleObjectValues_case:
			tv, tok := ticketCf.GetValue().(*v2.TicketCustomField_PickMultipleObjectValues)
			if !tok {
				l.Debug("error: invalid ticket: expected object values for field", zap.String("custom_field_id", cf.GetId()), zap.Any("values", tv))
				return false, nil
			}

			ticketValues := tv.PickMultipleObjectValues.GetValues()
			if len(ticketValues) == 0 {
				ticketValues = cf.GetPickMultipleObjectValues().GetDefaultValues()
			}

			allowedValues := cf.GetPickMultipleObjectValues().GetAllowedValues()

			// Object values are empty but custom field is not required, skip further validation
			if !cf.GetRequired() && len(ticketValues) == 0 {
				continue
			}

			// Custom field is required so check if object values are empty
			if len(ticketValues) == 0 {
				l.Debug("error: invalid ticket: object values is required but was empty", zap.String("custom_field_id", cf.GetId()))
				return false, nil
			}

			if len(allowedValues) == 0 {
				l.Debug("error: invalid schema: expected schema to specify at least one allowed value", zap.String("custom_field_id", cf.GetId()))
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
					zap.String("custom_field_id", cf.GetId()),
					zap.Any("values", ticketValues),
					zap.Any("allowed_values", allowedValues),
				)
				return false, nil
			}

		default:
			l.Debug("error: invalid schema: unknown custom field type", zap.Any("custom_field_type", cf.WhichValue()))
			return false, errors.New("error: invalid schema: unknown custom field type")
		}
	}

	return true, nil
}

func StringFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		StringValue: &v2.TicketCustomFieldStringValue{},
	}.Build()
}

func StringField(id, value string) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		StringValue: v2.TicketCustomFieldStringValue_builder{
			Value: value,
		}.Build(),
	}.Build()
}

func StringsFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:           id,
		DisplayName:  displayName,
		Required:     required,
		StringValues: &v2.TicketCustomFieldStringValues{},
	}.Build()
}

func StringsField(id string, values []string) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		StringValues: v2.TicketCustomFieldStringValues_builder{
			Values: values,
		}.Build(),
	}.Build()
}

func BoolFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		BoolValue:   &v2.TicketCustomFieldBoolValue{},
	}.Build()
}

func NumberFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		NumberValue: &v2.TicketCustomFieldNumberValue{},
	}.Build()
}

func BoolField(id string, value bool) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		BoolValue: v2.TicketCustomFieldBoolValue_builder{
			Value: value,
		}.Build(),
	}.Build()
}

func NumberField(id string, value float32) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		NumberValue: v2.TicketCustomFieldNumberValue_builder{
			Value: wrapperspb.Float(value),
		}.Build(),
	}.Build()
}

func TimestampFieldSchema(id, displayName string, required bool) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:             id,
		DisplayName:    displayName,
		Required:       required,
		TimestampValue: &v2.TicketCustomFieldTimestampValue{},
	}.Build()
}

func TimestampField(id string, value time.Time) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		TimestampValue: v2.TicketCustomFieldTimestampValue_builder{
			Value: timestamppb.New(value),
		}.Build(),
	}.Build()
}

func PickStringFieldSchema(id, displayName string, required bool, allowedValues []string) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		PickStringValue: v2.TicketCustomFieldPickStringValue_builder{
			AllowedValues: allowedValues,
		}.Build(),
	}.Build()
}

func PickStringField(id string, value string) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		PickStringValue: v2.TicketCustomFieldPickStringValue_builder{
			Value: value,
		}.Build(),
	}.Build()
}

func PickMultipleStringsFieldSchema(id, displayName string, required bool, allowedValues []string) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		PickMultipleStringValues: v2.TicketCustomFieldPickMultipleStringValues_builder{
			AllowedValues: allowedValues,
		}.Build(),
	}.Build()
}

func PickMultipleStringsField(id string, values []string) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		PickMultipleStringValues: v2.TicketCustomFieldPickMultipleStringValues_builder{
			Values: values,
		}.Build(),
	}.Build()
}

func PickObjectValueFieldSchema(id, displayName string, required bool, allowedValues []*v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		PickObjectValue: v2.TicketCustomFieldPickObjectValue_builder{
			AllowedValues: allowedValues,
		}.Build(),
	}.Build()
}

func PickObjectValueField(id string, value *v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		PickObjectValue: v2.TicketCustomFieldPickObjectValue_builder{
			Value: value,
		}.Build(),
	}.Build()
}

func PickMultipleObjectValuesFieldSchema(id, displayName string, required bool, allowedValues []*v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id:          id,
		DisplayName: displayName,
		Required:    required,
		PickMultipleObjectValues: v2.TicketCustomFieldPickMultipleObjectValues_builder{
			AllowedValues: allowedValues,
		}.Build(),
	}.Build()
}

func PickMultipleObjectValuesField(id string, values []*v2.TicketCustomFieldObjectValue) *v2.TicketCustomField {
	return v2.TicketCustomField_builder{
		Id: id,
		PickMultipleObjectValues: v2.TicketCustomFieldPickMultipleObjectValues_builder{
			Values: values,
		}.Build(),
	}.Build()
}
