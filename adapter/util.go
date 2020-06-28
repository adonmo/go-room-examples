package adapter

import (
	"fmt"
	"reflect"

	"github.com/adonmo/goroom/orm"
)

func isValidModel(modelDef orm.ModelDefinition) bool {
	return modelDef.TableName != ""
}

func getReflectTypeForEntity(entity interface{}) (reflectType reflect.Type, err error) {
	if entity == nil {
		err = fmt.Errorf("Bad argument: Nil entity")
		return
	}

	reflectType = reflect.ValueOf(entity).Type()
	for reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}

	if reflectType.Kind() != reflect.Struct {
		err = fmt.Errorf("Entity must be a struct")
	}

	return
}
