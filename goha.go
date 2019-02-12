package goha

import (
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
