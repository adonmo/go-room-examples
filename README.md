# go-room-examples
Example Usage of go-room

#### The main function exposes a command line utility interface with following options:  
  - dumpData (true by default) => Dump the data from database after Init (default true)  
  - insertSamples (true by default) => Should insert samples after init? (default true)  
  - version int (Needs non negative value [1,3]) => Version number of the database schema for which init must be run  

#### Example Usage
```bash
go build *.go
./main -version=1
```
This will initialize the DB for version 1 schema. You can run the command with version=2 or version=3 to check if upgrade works
