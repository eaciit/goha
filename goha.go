package goha

import (
	"strings"
	"time"

	"git.eaciitapp.com/sebar/dbflex"
)

var defaultIDFieldName = "ID"

func init() {
	dbflex.RegisterDriver("hbase", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.ServerInfo = *si
		c.SetThis(c)
		return c
	})
}

func SetDefaultIDFieldName(n string) {
	defaultIDFieldName = n
}

func DefaultIDFieldName() string {
	return defaultIDFieldName
}

var timeOut time.Duration

func SetContextTimeOut(d time.Duration) {
	timeOut = d
}

func ContextTimeOut() time.Duration {
	return timeOut
}

var activeNameSpace string

func SetActiveNameSpace(ns string) {
	activeNameSpace = ns
}

func ActiveNameSpace() string {
	if activeNameSpace == "" {
		activeNameSpace = "default"
	}
	return activeNameSpace
}

func tableNameNs(ns, name string) string {
	if ns == "" {
		ns = "default"
	}
	names := strings.Split(name, ":")
	if len(names) > 1 {
		name = names[1]
		if ns == "default" {
			ns = names[0]
		}
	}

	if ns == "default" {
		return name
	} else {
		return ns + ":" + name
	}
}

func parseTableName(name string) (string, string) {
	ns := ActiveNameSpace()
	names := strings.Split(name, ":")
	if len(names) > 1 {
		name = names[1]
		if ns == "default" {
			ns = names[0]
		}
	} else {
		name = names[0]
	}
	//fmt.Println("ns", ns, "name", name, "names", names)
	return ns, name
}
