package v2

import (
	"encoding/json"
	"fmt"

	"github.com/adonmo/goroom/logger"
	"github.com/adonmo/goroom/orm"
	"github.com/adonmo/goroomex/adapter"
	"go.etcd.io/bbolt"
)

//UserDBMigration Represents migration objects used for the example DB
type UserDBMigration struct {
	BaseVersion   orm.VersionNumber
	TargetVersion orm.VersionNumber
	MigrationFunc func(db interface{}) error
}

//GetBaseVersion ...
func (m *UserDBMigration) GetBaseVersion() orm.VersionNumber {
	return m.BaseVersion
}

//GetTargetVersion ...
func (m *UserDBMigration) GetTargetVersion() orm.VersionNumber {
	return m.TargetVersion
}

//Apply ....
func (m *UserDBMigration) Apply(db interface{}) error {
	logger.Infof("Applying Migrations for %v to %v", m.BaseVersion, m.TargetVersion)
	return m.MigrationFunc(db)
}

//GetMigrations namesake
func GetMigrations() []orm.Migration {
	return []orm.Migration{
		&UserDBMigration{
			BaseVersion:   orm.VersionNumber(1),
			TargetVersion: orm.VersionNumber(2),
			MigrationFunc: func(db interface{}) error {
				return nil
			},
		},
	}
}

func migrateFrom1To2(tx *bbolt.Tx) error {
	objectStoreAdapter := adapter.NewORMAdapter(tx.DB())

	bucket := objectStoreAdapter.GetBucketForEntity(User{}, tx)
	if bucket == nil {
		return fmt.Errorf("No such table")
	}

	err := bucket.ForEach(func(k, v []byte) error {
		existing := struct {
			ID   int
			Name string
		}{}

		err := json.Unmarshal(v, &existing)
		if err != nil {
			return err
		}

		newEntry := User{
			ID:   existing.ID,
			Name: existing.Name,
		}

		//The name field will be rewritten in serialized data with key as username instead of name
		newData, err := json.Marshal(newEntry)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(adapter.Itob(existing.ID)), newData)
	})

	return err
}
