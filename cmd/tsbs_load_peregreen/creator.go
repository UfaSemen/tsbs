package main

type dbCreator struct {
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) Init() {
	loader.GetBufferedReader()
}

func (d *dbCreator) Close() {}
