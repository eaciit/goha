package goha

import (
	"context"
	"fmt"
	"strings"

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

		options := []gohbase.Option{}
		if c.ServerInfo.Database != "" {
			zkRoot := c.ServerInfo.Database
			if !strings.HasPrefix(zkRoot, "/") {
				zkRoot = "/" + zkRoot
			}
			options = append(options, gohbase.ZookeeperRoot(zkRoot))
		}

		c.client = gohbase.NewClient(c.ServerInfo.Host, options...)
		c.admin = gohbase.NewAdminClient(c.ServerInfo.Host, options...)
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

	if int(ContextTimeOut()) == int(0) {
		q.ctx = context.Background()
	} else {
		q.ctx, q.cancelCtx = context.WithTimeout(context.Background(), ContextTimeOut())
	}

	return q
}
