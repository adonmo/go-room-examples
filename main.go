package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

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
	dataFile := flag.String("insertFrom", "", "Data file to insert data from into the database after init")
	flag.Parse()

	currentVersionNumber := orm.VersionNumber(*version)
	if currentVersionNumber > 3 || currentVersionNumber < 1 {
		panic(fmt.Errorf("Bad input to version number argument"))
	}

	db, err := bbolt.Open("goroomex.db", 0600, nil)
	if err != nil {
		panic(err)
	}

	var entityList []interface{}
	var availableMigrations []orm.Migration
	var dataInputArr interface{}

	switch currentVersionNumber {
	case 1:
		entityList = []interface{}{v1.User{}}
		availableMigrations = v1.GetMigrations()
		dataInputArr = []*v1.User{}
	case 2:
		entityList = []interface{}{v2.User{}}
		availableMigrations = v2.GetMigrations()
		dataInputArr = []*v2.User{}
	case 3:
		entityList = []interface{}{v3.User{}}
		availableMigrations = append(v3.GetMigrations(), v2.GetMigrations()...)
		dataInputArr = []*v3.User{}
	}

	err = initObjectStore(db, entityList, availableMigrations, currentVersionNumber, false)
	if err != nil {
		panic(err)
	}

	if *dataFile != "" {
		data, _ := ioutil.ReadFile(*dataFile)
		err := json.Unmarshal(data, &dataInputArr)
		if err != nil {
			panic(err)
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
