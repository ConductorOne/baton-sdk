package main

import (
	"encoding/json"
	"fmt"
	"reflect"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/types/known/structpb"
)

func TypeToAccountCreationSchema(param any) *v2.ConnectorAccountCreationSchema {
	var schema v2.ConnectorAccountCreationSchema
	typeModel := reflect.TypeOf(param).Elem()
	elem := reflect.ValueOf(param).Elem()
	if typeModel == reflect.TypeOf(reflect.Value{}) {
		typeModel = param.(*reflect.Value).Type()
		elem = *param.(*reflect.Value)
	}
	if typeModel.Kind() == reflect.Struct {
		schema = v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{},
		}
		for i := range typeModel.NumField() {
			field := typeModel.Field(i)
			schema.FieldMap[field.Name] = &v2.ConnectorAccountCreationSchema_Field{
				DisplayName: field.Tag.Get("display_name"),
				Required:    field.Tag.Get("required") == "true",
				Description: field.Tag.Get("description"),
				Placeholder: field.Tag.Get("placeholder"),
				Order:       int32(i),
			}
			switch field.Type.Kind() {
			case reflect.String:
				defaultValue := elem.Field(i).String()
				schema.FieldMap[field.Name].Field = &v2.ConnectorAccountCreationSchema_Field_StringField{
					StringField: &v2.ConnectorAccountCreationSchema_StringField{
						DefaultValue: &defaultValue,
					},
				}
			case reflect.Int:
				defaultValue := int32(elem.Field(i).Int())
				schema.FieldMap[field.Name].Field = &v2.ConnectorAccountCreationSchema_Field_IntField{
					IntField: &v2.ConnectorAccountCreationSchema_IntField{
						DefaultValue: &defaultValue,
					},
				}
			case reflect.Bool:
				defaultValue := elem.Field(i).Bool()
				schema.FieldMap[field.Name].Field = &v2.ConnectorAccountCreationSchema_Field_BoolField{
					BoolField: &v2.ConnectorAccountCreationSchema_BoolField{
						DefaultValue: &defaultValue,
					},
				}
			case reflect.Slice:
				defaultValue := elem.Field(i).Interface().([]string)
				schema.FieldMap[field.Name].Field = &v2.ConnectorAccountCreationSchema_Field_StringListField{
					StringListField: &v2.ConnectorAccountCreationSchema_StringListField{
						DefaultValue: defaultValue,
					},
				}
			case reflect.Struct:
				defaultValue := elem.Field(i)
				miniSchema := TypeToAccountCreationSchema(&defaultValue)
				schema.FieldMap[field.Name].Field = &v2.ConnectorAccountCreationSchema_Field_MapField{
					MapField: &v2.ConnectorAccountCreationSchema_MapField{
						DefaultValue: miniSchema.FieldMap,
					},
				}
			}
		}
	}
	return &schema
}

func setField(field *reflect.Value, value *structpb.Value) error {
	if !field.CanSet() {
		return fmt.Errorf("field %s is not settable", field.String())
	}
	switch value.GetKind().(type) {
	case *structpb.Value_StringValue:
		field.SetString(value.GetStringValue())
	case *structpb.Value_NumberValue:
		field.SetInt(int64(value.GetNumberValue()))
	case *structpb.Value_BoolValue:
		field.SetBool(value.GetBoolValue())
	case *structpb.Value_ListValue:
		for _, v := range value.GetListValue().GetValues() {
			newField := reflect.New(reflect.TypeOf(v.AsInterface())).Elem()
			if err := setField(&newField, v); err != nil {
				return err
			}
			field.Set(reflect.Append(*field, newField))
		}
	case *structpb.Value_StructValue:
		values := value.GetStructValue().GetFields()
		for i := range field.NumField() {
			fieldValue := field.Field(i)
			err := setField(&fieldValue, values[field.Type().Field(i).Name])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func AccountCreationSchemaToType[T any](schema map[string]*structpb.Value, typeModel *T) error {
	typeModelType := reflect.TypeOf(typeModel).Elem()
	if typeModelType.Kind() == reflect.Struct {
		for i := range typeModelType.NumField() {
			field := typeModelType.Field(i)
			fieldValue := reflect.ValueOf(typeModel).Elem().Field(i)
			err := setField(&fieldValue, schema[field.Name])
			if err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("error converting schema to type %T, typeModel is not a struct", typeModel)
	}
	return nil
}

//
// -----------------------
// -----------------------
// -----------------------
// -----------------------
//

func testDataSchemaMap() map[string]*structpb.Value {
	// Create accountInfo.Profile mock data
	schemaMap := make(map[string]*structpb.Value)
	schemaMap["Email"] = structpb.NewStringValue("test@test.com")
	schemaMap["FavoriteNumber"] = structpb.NewNumberValue(42)
	listValues, _ := structpb.NewList([]any{"1", "2", "3"})
	schemaMap["ArbitraryValues"] = structpb.NewListValue(listValues)
	mapValues := structpb.NewStructValue(&structpb.Struct{
		Fields: map[string]*structpb.Value{
			"Key1": structpb.NewStringValue("value1"),
			"Key2": structpb.NewStringValue("value2"),
			"Nested": structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"Key1": structpb.NewStringValue("value1"),
					"Key2": structpb.NewStringValue("value2"),
				},
			}),
		},
	})
	schemaMap["MapValues"] = mapValues
	return schemaMap
}

func testBatonToType() {
	schemaMap := testDataSchemaMap()
	// Test building struct from field map
	typedResponse := &AccountCreationSchema{}
	if err := AccountCreationSchemaToType(schemaMap, typedResponse); err != nil {
		fmt.Println(err)
	}
	jsonResponse, _ := json.MarshalIndent(typedResponse, "", "  ")
	fmt.Println("Baton Action Parameters: ", string(jsonResponse))
}

func testTypeToBaton() {
	defaultValues := &AccountCreationSchema{
		Email:           "test@test.com",
		FavoriteNumber:  42,
		ArbitraryValues: []string{"1", "2", "3"},
		MapValues: ArbStruct{
			Key1: "value1",
			Key2: "value2",
			Nested: Nested{
				Key1: "value1",
				Key2: "value2",
			},
		},
	}
	// Test creating schema object from struct
	schema := TypeToAccountCreationSchema(defaultValues)
	jsonSchema, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Schema: %s\n", string(jsonSchema))
}

type AccountCreationSchema struct {
	Email           string    `display_name:"Email" required:"true" description:"This email will be used as the login for the user." placeholder:"user@example.com"`
	FavoriteNumber  int       `display_name:"Favorite Number" required:"true" description:"This is a favorite number." placeholder:"42"`
	ArbitraryValues []string  `display_name:"Arbitrary Values" description:"This is a list of random values." placeholder:"[1, 2, 3]"`
	MapValues       ArbStruct `display_name:"Arbitrary Struct" description:"This is a struct of random values." placeholder:"{'key1': 'value1', 'key2': 'value2'}"`
}

type ArbStruct struct {
	Key1   string `display_name:"Key 1" description:"This is a key." placeholder:"value1"`
	Key2   string `display_name:"Key 2" description:"This is a key." placeholder:"value2"`
	Nested Nested `display_name:"Nested" description:"This is a nested struct." placeholder:"{'key1': 'value1', 'key2': 'value2'}"`
}

type Nested struct {
	Key1 string `display_name:"Key 1" description:"This is a key." placeholder:"value1"`
	Key2 string `display_name:"Key 2" description:"This is a key." placeholder:"value2"`
}

func main() {
	testBatonToType()
	testTypeToBaton()
}
