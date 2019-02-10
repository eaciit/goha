package goha

import (
	"fmt"

	"git.eaciitapp.com/sebar/dbflex"
	"github.com/tsuna/gohbase"
)

type Connection struct {
	dbflex.ConnectionBase
	state  string
	client gohbase.Client
	admin  gohbase.AdminClient
}

func (c *Connection) Connect() error {
	var e error
	func() {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("connect error. %v", r)
			}
		}()
		c.client = gohbase.NewClient(c.ServerInfo.Host)
		c.admin = gohbase.NewAdminClient(c.ServerInfo.Host)
		c.state = dbflex.StateConnected
	}()
	return e
}

func (c *Connection) State() string {
	return c.state
}

func (c *Connection) Close() {
	if c.client != nil {
		c.client.Close()
	}
	c.client = nil

	if c.admin != nil {
		c.admin = nil
	}

	c.state = dbflex.StateUnknown
}

func (c *Connection) Prepare(cmd dbflex.ICommand) (dbflex.IQuery, error) {
	q := c.NewQuery()
	return q, nil
}

func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.SetConnection(c)
	return q
}
