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

//ObjectDataField Representation
type ObjectDataField struct {
	Name string
	Tag  reflect.StructTag
}

//ObjectStoreEntityModel Entity Model for GORM for Room
type ObjectStoreEntityModel struct {
	Fields []*ObjectDataField
}

//ObjectStoreORMAdapter Adapter for BoltDB to work with Room
type ObjectStoreORMAdapter struct {
	db *bolt.DB
}

//NewORMAdapter Constructor
func NewORMAdapter(db *bolt.DB) *ObjectStoreORMAdapter {
	return &ObjectStoreORMAdapter{
		db: db,
	}
}

//GetBucketForEntity Namesake
func (oAdap *ObjectStoreORMAdapter) GetBucketForEntity(entity interface{}) (bucket *bolt.Bucket) {
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
func (oAdap *ObjectStoreORMAdapter) HasTable(entity interface{}) bool {
	bucket := oAdap.GetBucketForEntity(entity)
	return bucket != nil
}

//CreateTable Create tables for given entities
func (oAdap *ObjectStoreORMAdapter) CreateTable(entities ...interface{}) orm.Result {
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
func (oAdap *ObjectStoreORMAdapter) TruncateTable(entity interface{}) orm.Result {
	err := oAdap.db.Update(func(tx *bolt.Tx) error {
		bucket := oAdap.GetBucketForEntity(entity)
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
func (oAdap *ObjectStoreORMAdapter) Create(entity interface{}) orm.Result {
	err := oAdap.db.Update(func(tx *bolt.Tx) error {
		bucket := oAdap.GetBucketForEntity(entity)
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

		bucket.Put(Itob(nextID), buf)
		return nil
	})

	return orm.Result{
		Error: err,
	}
}

func Itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

//DropTable Drop a table
func (oAdap *ObjectStoreORMAdapter) DropTable(entities ...interface{}) orm.Result {
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
func (oAdap *ObjectStoreORMAdapter) GetModelDefinition(entity interface{}) (modelDefinition orm.ModelDefinition) {
	reflectType, err := getReflectTypeForEntity(entity)
	if err != nil {
		return
	}

	fields := make([]*ObjectDataField, 0, reflectType.NumField())
	for i := 0; i < reflectType.NumField(); i++ {
		if fieldStruct := reflectType.Field(i); ast.IsExported(fieldStruct.Name) {
			fields = append(fields, &ObjectDataField{
				Name: fieldStruct.Name + ":" + fieldStruct.Type.Name(),
				Tag:  fieldStruct.Tag,
			})
		}
	}

	return orm.ModelDefinition{
		EntityModel: &ObjectStoreEntityModel{
			Fields: fields,
		},
		TableName: reflectType.Name(),
	}
}

//GetUnderlyingORM Returns the underlying DB object
func (oAdap *ObjectStoreORMAdapter) GetUnderlyingORM() interface{} {
	return oAdap.db
}

//GetLatestSchemaIdentityHashAndVersion Get latest metadata for Room
func (oAdap *ObjectStoreORMAdapter) GetLatestSchemaIdentityHashAndVersion() (identityHash string, version int, err error) {
	var latestMetadata *room.GoRoomSchemaMaster
	err = oAdap.db.View(func(tx *bolt.Tx) error {
		bucket := oAdap.GetBucketForEntity(room.GoRoomSchemaMaster{})
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
func (oAdap *ObjectStoreORMAdapter) DoInTransaction(fc func(wrappedORM orm.ORM) error) (err error) {
	boltTransactionFunc := func(tx *bolt.Tx) error {
		return fc(NewORMAdapter(tx.DB()))
	}

	return oAdap.db.Update(boltTransactionFunc)
}
