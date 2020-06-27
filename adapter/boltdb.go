package adapter

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go/ast"
	"reflect"

	"github.com/adonmo/goroom/orm"
	"github.com/adonmo/goroom/room"
	bolt "go.etcd.io/bbolt"
)

//BoltDBField Representation
type BoltDBField struct {
	Name string
	Tag  reflect.StructTag
}

//BoltDBEntityModel Entity Model for GORM for Room
type BoltDBEntityModel struct {
	Fields []*BoltDBField
}

//ORMAdapter Adapter for BoltDB to work with Room
type ORMAdapter struct {
	db *bolt.DB
}

//NewORMAdapter Constructor
func NewORMAdapter(db *bolt.DB) orm.ORM {
	return &ORMAdapter{
		db: db,
	}
}

// HasTable(entity interface{}) bool
// CreateTable(models ...interface{}) Result
// TruncateTable(entity interface{}) Result
// Create(entity interface{}) Result
// DropTable(entities ...interface{}) Result
// GetModelDefinition(entity interface{}) ModelDefinition
// GetUnderlyingORM() interface{}
// GetLatestSchemaIdentityHashAndVersion() (identityHash string, version int, err error)
// DoInTransaction(fc func(tx ORM) error) (err error)

func isValidModel(modelDef orm.ModelDefinition) bool {
	return modelDef.TableName != ""
}

func (oAdap *ORMAdapter) getBucketForEntity(entity interface{}) (bucket *bolt.Bucket) {

	modelDef := oAdap.GetModelDefinition(entity)
	if isValidModel(modelDef) {
		oAdap.db.View(func(tx *bolt.Tx) error {
			bucket = tx.Bucket([]byte(modelDef.TableName))
			return nil
		})
	}

	return
}

//HasTable Check if a table for the given entity exists
func (oAdap *ORMAdapter) HasTable(entity interface{}) bool {
	bucket := oAdap.getBucketForEntity(entity)
	return bucket != nil
}

//CreateTable Create tables for given entities
func (oAdap *ORMAdapter) CreateTable(entities ...interface{}) orm.Result {

	err := oAdap.db.Update(func(tx *bolt.Tx) error {
		for _, entity := range entities {
			modelDef := oAdap.GetModelDefinition(entity)
			if isValidModel(modelDef) {
				bucket, err := tx.CreateBucket([]byte(modelDef.TableName))
				if err != nil || bucket == nil {
					return fmt.Errorf("Unable to create bucket for %+v", modelDef)
				}
			} else {
				return fmt.Errorf("Invalid Model Def: %+v", modelDef)
			}
		}
		return nil
	})

	return orm.Result{
		Error: err,
	}
}

//TruncateTable Clear out a table
func (oAdap *ORMAdapter) TruncateTable(entity interface{}) orm.Result {

	err := oAdap.db.Update(func(tx *bolt.Tx) error {
		bucket := oAdap.getBucketForEntity(entity)
		if bucket == nil {
			return fmt.Errorf("No table for entity")
		}

		keyList := [][]byte{}
		bucket.ForEach(func(k, v []byte) error {
			keyList = append(keyList, k)
			return nil
		})

		for _, k := range keyList {
			bucket.Delete(k)
		}

		return nil
	})

	return orm.Result{
		Error: err,
	}
}

//Create Add the entity to the table
func (oAdap *ORMAdapter) Create(entity interface{}) orm.Result {

	err := oAdap.db.Update(func(tx *bolt.Tx) error {
		bucket := oAdap.getBucketForEntity(entity)
		if bucket == nil {
			return fmt.Errorf("No table for entity")
		}

		nextSeq, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		nextID := int(nextSeq)

		val := reflect.ValueOf(entity).FieldByName("ID")
		if val.IsValid() && val.CanSet() && val.Kind() == reflect.Int {
			val.SetInt(int64(nextID))
		}

		buf, err := json.Marshal(entity)
		if err != nil {
			return err
		}

		bucket.Put(itob(nextID), buf)
		return nil
	})

	return orm.Result{
		Error: err,
	}
}

func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

//DropTable Drop a table
func (oAdap *ORMAdapter) DropTable(entities ...interface{}) orm.Result {

	err := oAdap.db.Update(func(tx *bolt.Tx) error {
		for _, entity := range entities {
			modelDef := oAdap.GetModelDefinition(entity)
			err := tx.DeleteBucket([]byte(modelDef.TableName))
			if err != nil {
				return err
			}
		}

		return nil
	})

	return orm.Result{
		Error: err,
	}
}

//GetModelDefinition Gives model definition of a given database entity
func (oAdap *ORMAdapter) GetModelDefinition(entity interface{}) (modelDefinition orm.ModelDefinition) {
	reflectType, err := getReflectTypeForEntity(entity)
	if err != nil {
		return
	}

	fields := make([]*BoltDBField, 0, reflectType.NumField())
	for i := 0; i < reflectType.NumField(); i++ {
		if fieldStruct := reflectType.Field(i); ast.IsExported(fieldStruct.Name) {
			fields = append(fields, &BoltDBField{
				Name: fieldStruct.Name + ":" + fieldStruct.Type.Name(),
				Tag:  fieldStruct.Tag,
			})
		}
	}

	return orm.ModelDefinition{
		EntityModel: &BoltDBEntityModel{
			Fields: fields,
		},
		TableName: reflectType.Name(),
	}
}

//GetUnderlyingORM Returns the underlying DB object
func (oAdap *ORMAdapter) GetUnderlyingORM() interface{} {
	return oAdap.db
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

//GetLatestSchemaIdentityHashAndVersion Get latest metadata for Room
func (oAdap *ORMAdapter) GetLatestSchemaIdentityHashAndVersion() (identityHash string, version int, err error) {

	var latestMetadata *room.GoRoomSchemaMaster
	err = oAdap.db.View(func(tx *bolt.Tx) error {
		bucket := oAdap.getBucketForEntity(room.GoRoomSchemaMaster{})
		if bucket == nil {
			return fmt.Errorf("No such table")
		}

		err := bucket.ForEach(func(k, v []byte) error {
			metadata := room.GoRoomSchemaMaster{}
			err := json.Unmarshal(v, &metadata)
			if err != nil {
				return err
			}

			latestMetadata = &metadata
			return nil
		})

		if err != nil {
			return err
		}

		return nil
	})

	if latestMetadata != nil {
		identityHash = latestMetadata.IdentityHash
		version = int(latestMetadata.Version)
	}

	return
}

//DoInTransaction Runs the DB operations in a single transaction
func (oAdap *ORMAdapter) DoInTransaction(fc func(wrappedORM orm.ORM) error) (err error) {

	boltTransactionFunc := func(tx *bolt.Tx) error {
		return fc(NewORMAdapter(tx.DB()))
	}

	return oAdap.db.Update(boltTransactionFunc)
}
