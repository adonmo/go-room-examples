package main

import (
	"encoding/json"
	"flag"
	"fmt"

	"github.com/adonmo/goroom"
	"github.com/adonmo/goroom/orm"
	"github.com/adonmo/goroom/room"

	"github.com/adonmo/goroom/util/adapter"
	objectstoreadapter "github.com/adonmo/goroomex/adapter"
	v1 "github.com/adonmo/goroomex/db/v1"
	v2 "github.com/adonmo/goroomex/db/v2"
	v3 "github.com/adonmo/goroomex/db/v3"
	"go.etcd.io/bbolt"
)

func main() {
	version := flag.Int("version", 0, "Version number of the database schema for which init must be run")
	insertSamples := flag.Bool("insertSamples", true, "Should insert samples after init?")
	dumpData := flag.Bool("dumpData", true, "Dump the data from database after Init")
	flag.Parse()

	currentVersionNumber := orm.VersionNumber(*version)
	if currentVersionNumber > 3 || currentVersionNumber < 1 {
		panic(fmt.Errorf("Bad input to version number argument"))
	}

	db, err := bbolt.Open("goroomex.db", 0600, nil)
	defer db.Close()

	if err != nil {
		panic(err)
	}

	var entityList []interface{}
	var availableMigrations []orm.Migration
	var sampleData []interface{}

	switch currentVersionNumber {
	case 1:
		entityList = []interface{}{v1.User{}}
		availableMigrations = v1.GetMigrations()
		sampleData = v1.GetSampleData()
	case 2:
		entityList = []interface{}{v2.User{}}
		availableMigrations = v2.GetMigrations()
		sampleData = v2.GetSampleData()
	case 3:
		entityList = []interface{}{v3.User{}}
		availableMigrations = append(v3.GetMigrations(), v2.GetMigrations()...)
		sampleData = v3.GetSampleData()
	}

	err = initObjectStore(db, entityList, availableMigrations, currentVersionNumber, false)
	if err != nil {
		panic(err)
	}

	if *insertSamples {
		err := db.Update(func(tx *bbolt.Tx) error {
			objectStoreORMAdapter := objectstoreadapter.NewORMAdapterForTransaction(db, tx)
			for _, entity := range sampleData {

				result := objectStoreORMAdapter.Create(entity)
				if result.Error != nil {
					return result.Error
				}
			}
			return nil
		})

		if err != nil {
			panic(err)
		}

	}

	if *dumpData {
		for _, entity := range entityList {
			dumpUserData(db, entity)
		}
	}

	fmt.Printf("Object Store initialized succesfully for version: %v\n", currentVersionNumber)
}

func initObjectStore(db *bbolt.DB, entityList []interface{}, migrations []orm.Migration, versionNumber orm.VersionNumber, fallbackToDestructiveMigration bool) (err error) {
	objectStoreORMAdapter := objectstoreadapter.NewORMAdapter(db)
	identityHashCalculator := &adapter.EntityHashConstructor{}

	roomDB, errList := room.New(entityList, objectStoreORMAdapter, versionNumber, migrations, identityHashCalculator)
	if errList != nil {
		err = fmt.Errorf("Error while setting up DB props. Err: %v", errList)
		return
	}

	err = goroom.InitializeRoom(roomDB, fallbackToDestructiveMigration)
	if err != nil {
		err = fmt.Errorf("Error while initializing App Database. Err: %v", err)
		return
	}

	return
}

func dumpUserData(db *bbolt.DB, entity interface{}) {
	objectStoreAdapter := objectstoreadapter.NewORMAdapter(db)

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := objectStoreAdapter.GetBucketForEntity(entity, tx)
		if bucket == nil {
			return fmt.Errorf("No such table")
		}

		err := bucket.ForEach(func(k, v []byte) error {
			err := json.Unmarshal(v, &entity)
			if err != nil {
				return err
			}

			fmt.Printf("%+v\n", entity)
			return nil
		})

		return err
	})

	if err != nil {
		panic(err)
	}
}

func saveUserData(db *bbolt.DB, entity interface{}, rawDataDictionary []map[string]interface{}) {

	db.Update(func(tx *bbolt.Tx) error {

		for _, rawData := range rawDataDictionary {
			jsonData, err := json.Marshal(rawData)
			if err != nil {
				panic(err)
			}

			err = json.Unmarshal(jsonData, &entity)
			if err != nil {
				panic(err)
			}

			user, ok := entity.(v1.User)
			if !ok {
				panic(fmt.Errorf("Incorrect data type when unmarshalling"))
			}
			fmt.Printf("Entity %+v\n", user)
		}
		return nil
	})

}
