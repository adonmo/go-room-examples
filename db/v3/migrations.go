package v3

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
			BaseVersion:   orm.VersionNumber(2),
			TargetVersion: orm.VersionNumber(3),
			MigrationFunc: func(db interface{}) error {
				return nil
			},
		},
	}
}

func migrateFrom2To3(db *bbolt.DB) error {
	objectStoreAdapter := adapter.NewORMAdapter(db)

	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := objectStoreAdapter.GetBucketForEntity(User{})
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
				ID:    existing.ID,
				Name:  existing.Name,
				Score: 10, //All old users start with a base score of 10
			}

			//A default value for score will be added to all existing data
			newData, err := json.Marshal(newEntry)
			if err != nil {
				return err
			}

			return bucket.Put([]byte(adapter.Itob(existing.ID)), newData)
		})

		if err != nil {
			return err
		}

		return nil
	})

	return err
}
