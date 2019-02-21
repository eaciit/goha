package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/eaciit/toolkit"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/eaciit/goha"
)

type Connection struct {
	ConnectionString string
}

type Config struct {
	Connections    map[string]*Connection
	ConnectionName string
	Namespace      string
	FamilyName     string
	TableName      string
	Debug          bool
}

var (
	cfg              *Config
	conn             dbflex.IConnection
	err              error
	connectionString string

	configParm = flag.String("config", "app.json", "path of config file")
)

type Record struct {
	KeyIndex string
	Name     string
	Age      int
}

func main() {
	flag.Parse()

	if cfg, err = loadConfig(*configParm); err != nil {
		panic("panic. " + err.Error())
	}

	if connectionString, conn, err = prepareConnection(); err != nil {
		panic("connectione error. " + err.Error())
	}

	fmt.Println("Connected to:", connectionString)
	defer conn.Close()

	goha.SetActiveNameSpace(cfg.Namespace)
	goha.SetDefaultFamilyName(cfg.FamilyName)
	goha.SetDefaultIDFieldName("KeyIndex")
	tableName := cfg.TableName
	if tableName == "" {
		tableName = "TestTable"
	}

	cmdDeleteTable := dbflex.From(tableName).Command("delete-table")
	conn.Execute(cmdDeleteTable, nil)

	if cfg.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
	cmdCreateTable := dbflex.From(tableName).Command("create-table")
	if _, err = conn.Execute(cmdCreateTable, nil); err != nil {
		fmt.Println("error create table.", err.Error())
	}
	fmt.Println("Create table table success")

	cmd := dbflex.From(tableName).Save()

	saveError := false
	for i := 0; i < 5; i++ {
		id := toolkit.RandomString(5)
		logrus.Info("Saving record " + id)
		record := NewRecord(id)
		_, err := conn.Execute(cmd, toolkit.M{}.Set("data", record))
		if err != nil {
			logrus.Errorf("Saving %s error: %s", id, err.Error())
			saveError = true
			break
		}
	}
	if !saveError {
		logrus.Info("Saving is executed successfully")
	}

	logrus.Info("Populate data")
	cmd = dbflex.From(tableName).Select()
	records := []Record{}
	cursor := conn.Cursor(cmd, nil)
	if cursor.Error() != nil {
		logrus.Errorf("error create cursor: %s", cursor.Error())
		os.Exit(1)
	}
	defer cursor.Close()

	if err = cursor.Fetchs(&records, 0); err != nil {
		logrus.Errorf("unable to retrieve cursor result. %s", err.Error())
	}
	logrus.Infof("Found %d records", len(records))
}

func NewRecord(id string) *Record {
	r := new(Record)
	r.KeyIndex = id
	r.Name = "Name for " + id
	r.Age = toolkit.RandInt(20) + 21
	return r
}

func loadConfig(p string) (*Config, error) {
	if p == "" {
		return nil, fmt.Errorf("config is blank")
	}

	if bs, err := ioutil.ReadFile(p); err != nil {
		return nil, fmt.Errorf("unable to open file %s. %s", p, err.Error())
	} else {
		cfg := new(Config)
		if err = json.Unmarshal(bs, cfg); err != nil {
			return nil, fmt.Errorf("unable to unmarshall config. %s", err.Error())
		}
		return cfg, nil
	}
}

func prepareConnection() (string, dbflex.IConnection, error) {
	if selected, exist := cfg.Connections[cfg.ConnectionName]; exist {
		connString := selected.ConnectionString
		logrus.Infof("Prepare connection: %s", connString)
		if conn, err := dbflex.NewConnectionFromURI(connString, nil); err != nil {
			return connString, conn, fmt.Errorf("unable to build connection. %s", err.Error())
		} else {
			if err = conn.Connect(); err != nil {
				return connString, conn, fmt.Errorf("unable to connect. %s", err.Error())
			}
			return connString, conn, err
		}
	} else {
		return "", nil, fmt.Errorf("connection %s is not exist", cfg.ConnectionName)
	}
}
